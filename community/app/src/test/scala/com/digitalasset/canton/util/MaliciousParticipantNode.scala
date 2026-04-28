// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.{
  CryptoPureApi,
  Salt,
  SigningKeyUsage,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ViewType.{AssignmentViewType, UnassignmentViewType}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.util.TestSubmissionService
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.ViewHashAndRecipients
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
  TransactionConfirmationRequestFactory,
  TransactionTreeFactory,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.client.{SendResult, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SessionKeyStoreWithInMemoryCache
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  FutureHelpers,
  LedgerCommandId,
  LfPartyId,
  WorkflowId,
  checked,
}
import com.digitalasset.daml.lf.data.Ref.UserId
import com.digitalasset.daml.lf.transaction.test.TestIdFactory
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction}
import org.scalatest.EitherValues.*
import org.scalatest.OptionValues.*

import java.time.Duration
import java.util.UUID
import scala.concurrent.ExecutionContext

/** Wrapper around a participant that allows for submitting invalid confirmation requests. */
class MaliciousParticipantNode(
    participantId: ParticipantId,
    testSubmissionService: TestSubmissionService,
    seedGenerator: SeedGenerator,
    contractOfId: TransactionTreeFactory.ContractInstanceOfId,
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    sequencerClient: SequencerClient,
    defaultPsid: PhysicalSynchronizerId,
    defaultMediatorGroup: MediatorGroupRecipient,
    pureCrypto: CryptoPureApi,
    defaultCryptoSnapshot: () => SynchronizerSnapshotSyncCryptoApi,
    defaultProtocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging
    with TestIdFactory {

  val futureSupervisor: FutureSupervisor = FutureSupervisor.Noop

  private val transactionTreeFactory: TransactionTreeFactory =
    confirmationRequestFactory.transactionTreeFactory

  private def sendRequestBatchToSequencer(
      batch: Batch[DefaultOpenEnvelope],
      approximateTimestampForSigning: CantonTimestamp,
      maxSequencingTime: CantonTimestamp,
      topologyTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    val promise = PromiseUnlessShutdown.unsupervised[Either[String, SendResult.Success]]()
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    for {
      _ <- sequencerClient
        .send(
          batch,
          timestamps = SendRequestTimestamps(
            topologyTimestamp = topologyTimestamp,
            approximateTimestampForSigning = approximateTimestampForSigning,
            maxSequencingTime = maxSequencingTime,
          ),
          callback = {
            case UnlessShutdown.Outcome(success: SendResult.Success) =>
              promise.outcome_(Right(success))
            case UnlessShutdown.Outcome(result: SendResult) =>
              promise.outcome_(Left(s"Sending request failed asynchronously: $result"))
            case UnlessShutdown.AbortedDueToShutdown =>
              promise.failure(new RuntimeException("Shutdown happened while test was running"))
          },
        )
        .leftMap(_.show)
      sendResult <- EitherT(promise.futureUS)
    } yield sendResult
  }

  def submitUnassignmentRequest(
      fullTree: FullUnassignmentTree,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      sourceProtocolVersion: Source[ProtocolVersion] = Source(defaultProtocolVersion),
      overrideRecipients: Option[Recipients] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    val cids = fullTree.tree.view.tryUnwrap.contracts.contractIds
    logger.info(s"Malicious participant $participantId submitting unassignment request for $cids")

    val rootHash = fullTree.rootHash
    val stakeholders = fullTree.stakeholders

    val now = sequencerClient.clock.now
    val maxSequencingTime = sequencerClient.generateMaxSequencingTime(now)
    val signingTimestampOverrides = Some(
      SigningTimestampOverrides(
        approximateTimestamp = now,
        validityPeriodEnd = Some(maxSequencingTime),
      )
    )

    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        SessionEncryptionKeyCacheConfig(),
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        submittingParticipantSignature <- cryptoSnapshot
          .sign(
            rootHash.unwrap,
            SigningKeyUsage.ProtocolOnly,
            signingTimestampOverrides,
          )
          .leftMap(_.toString)
        mediatorMessage = fullTree.mediatorMessage(
          submittingParticipantSignature,
          sourceProtocolVersion,
        )
        recipientsSet <- EitherT
          .right[String](
            cryptoSnapshot.ipsSnapshot
              .activeParticipantsOfParties(stakeholders.all.toSeq)
              .map(_.values.flatten.toSet)
          )
        recipients <- EitherT.fromEither[FutureUnlessShutdown](
          overrideRecipients.orElse(Recipients.ofSet(recipientsSet)).toRight("no recipients")
        )
        viewsToKeyMap <- EncryptedViewMessageFactory
          .generateKeysFromRecipients(
            Seq(
              (
                ViewHashAndRecipients(fullTree.viewHash, recipients),
                None,
                fullTree.informees.toList,
              )
            ),
            parallel = true,
            pureCrypto,
            cryptoSnapshot,
            sessionKeyStore,
          )
          .leftMap(_.show)
        viewMessage <- EncryptedViewMessageFactory
          .encryptView(UnassignmentViewType)(
            fullTree,
            viewsToKeyMap.keyAndEncryptedRandomnessByRecipients(recipients),
            cryptoSnapshot,
            signingTimestampOverrides,
            sourceProtocolVersion.unwrap,
          )
          .leftMap(_.toString)
        rootHashMessage =
          RootHashMessage(
            rootHash,
            fullTree.psid,
            ViewType.UnassignmentViewType,
            cryptoSnapshot.ipsSnapshot.timestamp,
            EmptyRootHashMessagePayload,
          )
        rootHashRecipients =
          Recipients.recipientGroups(
            checked(
              NonEmptyUtil.fromUnsafe(
                recipientsSet.toSeq.map(participant =>
                  NonEmpty(Set, mediator, MemberRecipient(participant))
                )
              )
            )
          )
        messages = Seq[(ProtocolMessage, Recipients)](
          mediatorMessage -> Recipients.cc(mediator),
          viewMessage -> recipients,
          rootHashMessage -> rootHashRecipients,
        )
        batch = Batch.of(sourceProtocolVersion.unwrap, messages*)
        _ <- sendRequestBatchToSequencer(batch, now, maxSequencingTime)
      } yield ()
    }
  }

  def submitAssignmentRequest(
      submitter: LfPartyId,
      reassignmentData: UnassignmentData,
      submittingParticipant: ParticipantId = participantId,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      targetProtocolVersion: Target[ProtocolVersion] = Target(defaultProtocolVersion),
      overrideRecipients: Option[Recipients] = None,
      overrideReassignmentId: Option[ReassignmentId] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val reassignmentId = overrideReassignmentId.getOrElse(reassignmentData.reassignmentId)
    logger.info(
      s"Malicious participant $participantId submitting assignment request for $reassignmentId"
    )
    val sourceSynchronizer = reassignmentData.sourcePsid
    val targetSynchronizer = reassignmentData.targetPsid

    val stakeholders = reassignmentData.contractsBatch.stakeholders

    val assignmentUuid = seedGenerator.generateUuid()
    val seed = seedGenerator.generateSaltSeed()

    val commonDataSalt = Salt.tryDeriveSalt(seed, 0, pureCrypto)
    val viewSalt = Salt.tryDeriveSalt(seed, 1, pureCrypto)

    val submitterMetadata = ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString(UUID.randomUUID().toString),
      None,
      // Hard-coding our default user id to make sure that submissions and subscriptions use the same value
      UserId.assertFromString(LedgerApiCommands.defaultUserId),
      None,
    )

    val commonData = AssignmentCommonData
      .create(pureCrypto)(
        commonDataSalt,
        sourceSynchronizer,
        targetSynchronizer,
        mediator,
        stakeholders,
        assignmentUuid,
        submitterMetadata,
        reassigningParticipants = reassignmentData.reassigningParticipants,
        unassignmentTs = reassignmentData.unassignmentTs,
      )

    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        SessionEncryptionKeyCacheConfig(),
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        view <- EitherT.fromEither[FutureUnlessShutdown](
          AssignmentView
            .create(pureCrypto)(
              viewSalt,
              reassignmentId,
              reassignmentData.contractsBatch,
              targetProtocolVersion,
            )
        )
        fullTree = FullAssignmentTree(
          AssignmentViewTree(commonData, view, targetProtocolVersion, pureCrypto)
        )

        rootHash = fullTree.rootHash
        now = sequencerClient.clock.now
        maxSequencingTime = sequencerClient.generateMaxSequencingTime(now)
        signingTimestampOverrides = Some(
          SigningTimestampOverrides(
            approximateTimestamp = now,
            validityPeriodEnd = Some(maxSequencingTime),
          )
        )

        submittingParticipantSignature <- cryptoSnapshot
          .sign(
            rootHash.unwrap,
            SigningKeyUsage.ProtocolOnly,
            signingTimestampOverrides,
          )
          .leftMap(_.toString)
        mediatorMessage = fullTree.mediatorMessage(
          submittingParticipantSignature,
          targetProtocolVersion,
        )
        recipientsSet <- EitherT
          .right[String](
            cryptoSnapshot.ipsSnapshot
              .activeParticipantsOfParties(stakeholders.all.toSeq)
              .map(_.values.flatten.toSet)
          )
        recipients <- EitherT.fromEither[FutureUnlessShutdown](
          overrideRecipients.orElse(Recipients.ofSet(recipientsSet)).toRight("no recipients")
        )
        viewsToKeyMap <- EncryptedViewMessageFactory
          .generateKeysFromRecipients(
            Seq(
              (
                ViewHashAndRecipients(fullTree.viewHash, recipients),
                None,
                fullTree.informees.toList,
              )
            ),
            parallel = true,
            pureCrypto,
            cryptoSnapshot,
            sessionKeyStore,
          )
          .leftMap(_.show)
        viewMessage <- EncryptedViewMessageFactory
          .encryptView(AssignmentViewType)(
            fullTree,
            viewsToKeyMap.keyAndEncryptedRandomnessByRecipients(recipients),
            cryptoSnapshot,
            signingTimestampOverrides,
            targetProtocolVersion.unwrap,
          )
          .leftMap(_.toString)
        rootHashMessage =
          RootHashMessage(
            rootHash,
            targetSynchronizer.unwrap,
            ViewType.AssignmentViewType,
            cryptoSnapshot.ipsSnapshot.timestamp,
            EmptyRootHashMessagePayload,
          )
        rootHashRecipients =
          Recipients.recipientGroups(
            checked(
              NonEmptyUtil.fromUnsafe(
                recipientsSet.toSeq.map(participant =>
                  NonEmpty(Set, mediator, MemberRecipient(participant))
                )
              )
            )
          )
        messages = Seq[(ProtocolMessage, Recipients)](
          mediatorMessage -> Recipients.cc(mediator),
          viewMessage -> recipients,
          rootHashMessage -> rootHashRecipients,
        )
        batch = Batch.of(targetProtocolVersion.unwrap, messages*)
        _ <- sendRequestBatchToSequencer(batch, now, maxSequencingTime)
      } yield ()
    }
  }

  def submitTopologyTransactionRequest[Op <: TopologyChangeOp, M <: TopologyMapping](
      signedTopologyTransaction: SignedTopologyTransaction[Op, M],
      psid: PhysicalSynchronizerId = defaultPsid,
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipients: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    logger.info(
      s"Malicious participant $participantId submitting topology transaction $signedTopologyTransaction to $psid"
    )

    val broadcast = TopologyTransactionsBroadcast(
      psid,
      List(signedTopologyTransaction),
    )

    // It's safe to use these timestamps for signing the request because batch signatures are either empty
    // or generated with the long-term key, and no specific max sequencing time is defined, so the default can be used.
    val now = sequencerClient.clock.now
    val maxSequencingTime = sequencerClient.generateMaxSequencingTime(now)

    sendRequestBatchToSequencer(
      Batch.of(protocolVersion, broadcast -> recipients),
      approximateTimestampForSigning = now,
      maxSequencingTime = maxSequencingTime,
      topologyTimestamp = topologyTimestamp,
    )
  }

  def submitTopologyTransactionBroadcasts(
      topologyBroadcasts: Seq[TopologyTransactionsBroadcast],
      psid: PhysicalSynchronizerId = defaultPsid,
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
      topologyTimestamp: Option[CantonTimestamp] = None,
      recipients: Recipients = Recipients.cc(AllMembersOfSynchronizer),
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] = {
    logger.info(
      s"Malicious participant $participantId submitting topology transaction broadcasts ${topologyBroadcasts
          .map(_.transactions)} to $psid"
    )
    // It's safe to use these timestamps for signing the batch of `TopologyTransactionsBroadcast`
    // because they match the timestamps and max sequencing typically applied in production.
    val now = sequencerClient.clock.now
    val maxSequencingTime = sequencerClient.generateMaxSequencingTime(now)
    sendRequestBatchToSequencer(
      Batch.of(protocolVersion, topologyBroadcasts.map(_ -> recipients)*),
      approximateTimestampForSigning = now,
      maxSequencingTime = maxSequencingTime,
      topologyTimestamp = topologyTimestamp,
    )
  }

  def submitCommand(
      command: CommandsWithMetadata,
      transactionInterceptor: SubmittedTransaction => SubmittedTransaction = identity,
      transactionTreeInterceptor: GenTransactionTree => GenTransactionTree = identity,
      confirmationRequestInterceptor: TransactionConfirmationRequest => TransactionConfirmationRequest =
        identity,
      envelopeInterceptor: DefaultOpenEnvelope => DefaultOpenEnvelope = identity,
      mediator: MediatorGroupRecipient = defaultMediatorGroup,
      cryptoSnapshot: SynchronizerSnapshotSyncCryptoApi = defaultCryptoSnapshot(),
      maxDeduplicationDuration: Duration = Duration.ofDays(7),
      protocolVersion: ProtocolVersion = defaultProtocolVersion,
      submitterInfoInterceptor: SubmitterInfo => SubmitterInfo = identity,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SendResult.Success] =
    ResourceUtil.withResourceM(
      new SessionKeyStoreWithInMemoryCache(
        SessionEncryptionKeyCacheConfig(),
        timeouts,
        loggerFactory,
      )
    ) { sessionKeyStore =>
      for {
        transactionAndMetadata <- testSubmissionService
          .interpret(command)
          .leftMap(err => s"Unable to create transaction: $err")
          .mapK(FutureUnlessShutdown.outcomeK)
        (_submittedTransaction, metadata) = transactionAndMetadata
        submittedTransaction = transactionInterceptor(_submittedTransaction)
        transactionMeta = command.transactionMeta(submittedTransaction, metadata)
        transactionMetadata = TransactionMetadata
          .fromTransactionMeta(
            metaLedgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
            metaPreparationTime = transactionMeta.preparationTime,
            metaOptNodeSeeds = transactionMeta.optNodeSeeds,
          )
          .value

        wfTransaction = WellFormedTransaction.checkOrThrow(
          submittedTransaction,
          transactionMetadata,
          WellFormedTransaction.WithoutSuffixes,
        )

        now = sequencerClient.clock.now
        maxSequencingTime = sequencerClient.generateMaxSequencingTime(now)
        signingTimestampOverrides = Some(
          SigningTimestampOverrides(
            approximateTimestamp = now,
            validityPeriodEnd = Some(maxSequencingTime),
          )
        )

        transactionTree <- transactionTreeFactory
          .createTransactionTree(
            wfTransaction,
            submitterInfoInterceptor(command.submitterInfo(maxDeduplicationDuration)),
            command.workflowIdO.map(WorkflowId(_)),
            mediator,
            command.transactionSeed,
            command.transactionUuid,
            cryptoSnapshot.ipsSnapshot,
            contractOfId,
            metadata.globalKeyMapping,
            maxSequencingTime,
            validatePackageVettings = false,
          )
          .leftMap(err => s"Unable to create transaction tree: $err")

        modifiedTransactionTree = transactionTreeInterceptor(transactionTree)

        confirmationRequest <- confirmationRequestFactory
          .createConfirmationRequest(
            modifiedTransactionTree,
            cryptoSnapshot,
            signingTimestampOverrides,
            sessionKeyStore,
            protocolVersion,
          )
          .leftMap(err => s"Unable to create confirmation request: $err")

        modifiedConfirmationRequest = confirmationRequestInterceptor(confirmationRequest)
        batch <- EitherT
          .right(modifiedConfirmationRequest.asBatch(cryptoSnapshot.ipsSnapshot))
        modifiedBatch = batch.map(envelopeInterceptor)
        sendResult <- sendRequestBatchToSequencer(modifiedBatch, now, maxSequencingTime)
      } yield sendResult
    }
}

object MaliciousParticipantNode extends FutureHelpers {
  def apply(
      participant: LocalParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      defaultProtocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      defaultMediatorGroup: MediatorGroupRecipient = MediatorGroupRecipient(
        MediatorGroupIndex.zero
      ),
      testSubmissionServiceOverrideO: Option[TestSubmissionService] = None,
      resolveContractOverride: LfContractId => OptionT[
        FutureUnlessShutdown,
        GenContractInstance,
      ] = _ => OptionT(FutureUnlessShutdown.pure[Option[GenContractInstance]](None)),
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): MaliciousParticipantNode = {
    import env.*

    val participantNode = participant.underlying.value
    val sync = participantNode.sync
    val connectedSynchronizer = sync.readyConnectedSynchronizerById(synchronizerId.logical).value

    val contractStore = sync.participantNodePersistentState.value.contractStore

    val testSubmissionService = testSubmissionServiceOverrideO.getOrElse(
      TestSubmissionService(
        participant = participant,
        // Switch off authorization, so we can also test unauthorized commands.
        checkAuthorization = false,
        enableLfDev = true,
        resolveContractOverride = resolveContractOverride(_)
          .mapK(
            FutureUnlessShutdown.failOnShutdownToAbortExceptionK("MaliciousParticipantNode")
          )
          .map[FatContractInstance](_.inst),
      )
    )
    val seedGenerator = new SeedGenerator(participantNode.cryptoPureApi)

    def contractOfId(cid: LfContractId): EitherT[
      FutureUnlessShutdown,
      TransactionTreeFactory.ContractLookupError,
      GenContractInstance,
    ] = {
      val lookupFromStore = TransactionTreeFactory.contractInstanceLookup(contractStore)
      resolveContractOverride(cid)
        .toRight(())
        .orElse[TransactionTreeFactory.ContractLookupError, GenContractInstance](
          lookupFromStore(cid)
        )
    }

    val confirmationRequestFactory = connectedSynchronizer.requestGenerator
    val sequencerClient = connectedSynchronizer.sequencerClient

    def currentCryptoSnapshot(): SynchronizerSnapshotSyncCryptoApi = sync.syncCrypto
      .tryForSynchronizer(synchronizerId, BaseTest.defaultStaticSynchronizerParameters)
      .currentSnapshotApproximation
      .futureValueUS

    new MaliciousParticipantNode(
      participant.id,
      testSubmissionService,
      seedGenerator,
      contractOfId,
      confirmationRequestFactory,
      sequencerClient,
      synchronizerId,
      defaultMediatorGroup,
      participantNode.cryptoPureApi,
      () => currentCryptoSnapshot(),
      defaultProtocolVersion,
      timeouts,
      loggerFactory,
    )
  }
}
