// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data._
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import cats.{Applicative, MonoidK}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps}
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree, ViewType}
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.PendingRequestDataOrReplayData
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.{
  EncryptedViewMessageFactory,
  SeedGenerator,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferInProcessingSteps.NoTransferData
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps._
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps._
import com.digitalasset.canton.participant.protocol.{
  ProtocolProcessor,
  SingleDomainCausalTracker,
  TransferOutUpdate,
}
import com.digitalasset.canton.participant.store.TransferStore.TransferCompleted
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.EncryptedViewMessageDecryptionError
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Disabled,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter, checked}
import org.slf4j.event.Level

import scala.collection.{concurrent, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Either

class TransferOutProcessingSteps(
    domainId: DomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    version: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferOutViewType,
      TransferOutResult,
    ]
    with NamedLogging {

  import TransferOutProcessingSteps.stringOfNec

  override type PendingRequestData = PendingTransferOut

  override type SubmissionResultArgs = PendingTransferSubmission

  override type PendingDataAndResponseArgs = TransferOutProcessingSteps.PendingDataAndResponseArgs

  override type RejectionArgs = Unit

  override def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions = {
    state.pendingTransferOutSubmissions
  }

  override def requestKind: String = "TransferOut"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitter ${param.submittingParty}, contract ${param.contractId}, target ${param.targetDomain}"

  override def submissionIdOfPendingRequest(pendingData: PendingTransferOut): RootHash =
    pendingData.rootHash

  override def prepareSubmission(
      param: SubmissionParam,
      mediatorId: MediatorId,
      ephemeralState: SyncDomainEphemeralStateLookup,
      originRecentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Submission] = {
    val SubmissionParam(submittingParty, contractId, targetDomain) = param
    val pureCrypto = originRecentSnapshot.pureCrypto

    def withDetails(message: String) = s"Transfer-out $contractId to $targetDomain: $message"

    for {
      _ <- condUnitET[FutureUnlessShutdown](
        targetDomain != domainId,
        TargetDomainIsOriginDomain(domainId, contractId),
      )
      stakeholders <- stakeholdersOfContractId(ephemeralState.contractLookup, contractId).mapK(
        FutureUnlessShutdown.outcomeK
      )

      timeProofAndSnapshot <- transferCoordination.getTimeProofAndSnapshot(targetDomain)
      (timeProof, targetCrypto) = timeProofAndSnapshot
      _ = logger.debug(withDetails(s"Picked time proof ${timeProof.timestamp}"))

      transferOutRequestAndRecipients <- TransferOutProcessingSteps
        .createTransferOutRequest(
          participantId,
          timeProof,
          contractId,
          submittingParty,
          stakeholders,
          domainId,
          mediatorId,
          targetDomain,
          originRecentSnapshot.ipsSnapshot,
          targetCrypto.ipsSnapshot,
          logger,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      (transferOutRequest, recipients) = transferOutRequestAndRecipients

      transferOutUuid = seedGenerator.generateUuid()
      seed <- seedGenerator
        .generateSeedForTransferOut(transferOutRequest, transferOutUuid)
        .leftMap(SeedGeneratorError)
        .mapK(FutureUnlessShutdown.outcomeK)
      fullTree = transferOutRequest.toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        transferOutUuid,
      )
      mediatorMessage = fullTree.mediatorMessage
      rootHash = fullTree.rootHash
      viewMessage <- EncryptedViewMessageFactory
        .create(TransferOutViewType)(fullTree, originRecentSnapshot, version)
        .leftMap[TransferProcessorError](EncryptionError)
        .mapK(FutureUnlessShutdown.outcomeK)
      maybeRecipients = Recipients.ofSet(recipients)
      recipientsT <- EitherT
        .fromOption[FutureUnlessShutdown](
          maybeRecipients,
          NoStakeholders.logAndCreate(contractId, logger): TransferProcessorError,
        )
    } yield {
      val rootHashMessage =
        RootHashMessage(
          rootHash,
          domainId,
          ViewType.TransferOutViewType,
          EmptyRootHashMessagePayload,
        )
      val rootHashRecipients =
        Recipients.groups(
          checked(
            NonEmptyList.fromListUnsafe(
              recipients.toList.map(participant => NonEmptySet.of(mediatorId, participant))
            )
          )
        )
      // Each member gets a message sent to itself and to the mediator
      val messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediatorId),
        viewMessage -> recipientsT,
        rootHashMessage -> rootHashRecipients,
      )
      TransferSubmission(Batch.of(messages: _*), rootHash)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, TransferProcessorError, SubmissionResultArgs] = {
    performPendingSubmissionMapUpdate(
      pendingSubmissionMap,
      None,
      submissionParam.submittingParty,
      pendingSubmissionId,
    )
  }

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      pendingSubmission: SubmissionResultArgs,
  ): SubmissionResult = {
    val requestId = RequestId(deliver.timestamp)
    val transferId = TransferId(domainId, requestId.unwrap)
    SubmissionResult(transferId, pendingSubmission.transferCompletion.future)
  }

  private[this] def stakeholdersOfContractId(
      contractLookup: ContractLookup,
      contractId: LfContractId,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Set[LfPartyId]] =
    contractLookup
      .lookup(contractId)
      .toRight[TransferProcessorError](TransferOutProcessingSteps.UnknownContract(contractId))
      .map(storedContract => storedContract.contract.metadata.stakeholders)

  override protected def decryptTree(originSnapshot: DomainSnapshotSyncCryptoApi)(
      envelope: OpenEnvelope[EncryptedViewMessage[TransferOutViewType]]
  ): EitherT[Future, EncryptedViewMessageDecryptionError[TransferOutViewType], WithRecipients[
    FullTransferOutTree
  ]] = {
    EncryptedViewMessage
      .decryptFor(originSnapshot, envelope.protocolMessage, participantId) { bytes =>
        FullTransferOutTree
          .fromByteString(originSnapshot.pureCrypto)(bytes)
          .leftMap(e => DeserializationError(e.toString, bytes))
      }
      .map(WithRecipients(_, envelope.recipients))
  }

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      correctRootHashes: NonEmptyList[WithRecipients[FullTransferOutTree]],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      originSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CheckActivenessAndWritePendingContracts] = {
    // TODO(M40): Send a rejection if malformedPayloads is non-empty
    for {
      txOutRequestAndRecipients <- EitherT.cond[Future](
        correctRootHashes.toList.sizeCompare(1) == 0,
        correctRootHashes.head,
        ReceivedMultipleRequests(correctRootHashes.map(_.unwrap.viewHash)): TransferProcessorError,
      )
      WithRecipients(txOutRequest, recipients) = txOutRequestAndRecipients
      contractId = txOutRequest.contractId
      _ <- condUnitET[Future](
        txOutRequest.originDomain == domainId,
        UnexpectedDomain(
          TransferId(txOutRequest.originDomain, ts),
          domainId,
        ): TransferProcessorError,
      )
      contractIdS = Set(contractId)
      contractsCheck = ActivenessCheck(
        checkFresh = Set.empty,
        checkFree = Set.empty,
        checkActive = contractIdS,
        lock = contractIdS,
      )
      activenessSet = ActivenessSet(
        contracts = contractsCheck,
        transferIds = Set.empty,
        // We check keys on only domains with unique contract key semantics and there cannot be transfers on such domains
        keys = ActivenessCheck.empty,
      )
    } yield CheckActivenessAndWritePendingContracts(
      activenessSet,
      Seq.empty,
      PendingDataAndResponseArgs(txOutRequest, recipients, ts, rc, sc, originSnapshot),
    )
  }

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      tracker: SingleDomainCausalTracker,
      activenessF: Future[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, StorePendingDataAndSendResponseAndCreateTimeout] = {
    val PendingDataAndResponseArgs(fullTree, recipients, ts, rc, sc, originSnapshot) =
      pendingDataAndResponseArgs
    for {
      // Wait for earlier writes to the contract store having completed
      _ <- EitherT.right(pendingCursor)

      // Since the transfer out request should be sent only to participants that host a stakeholder of the contract,
      // we can expect to find the contract in the contract store.
      storedContract <- contractLookup.lookupE(fullTree.contractId).leftMap(ContractStoreFailed)

      originIps = originSnapshot.ipsSnapshot
      transferringParticipant <- validateTransferOutRequest(
        fullTree,
        storedContract.contract.metadata.stakeholders,
        originIps,
        recipients,
      )

      // Since the participant hosts a stakeholder, it should find the creating transaction ID in the contract store
      creatingTransactionId <- EitherT.fromEither[Future](
        storedContract.creatingTransactionIdO
          .toRight(CreatingTransactionIdNotFound(storedContract.contractId))
      )

      activenessResult <- EitherT.right(activenessF)

      hostedStks <- EitherT.liftF(hostedStakeholders(fullTree.stakeholders.toList, originIps))

      requestId = RequestId(ts)
      transferId: TransferId = TransferId(fullTree.originDomain, ts)
      entry = PendingTransferOut(
        requestId,
        rc,
        sc,
        fullTree.tree.rootHash,
        WithContractHash.fromContract(storedContract.contract, fullTree.contractId),
        transferringParticipant,
        transferId,
        fullTree.targetDomain,
        fullTree.stakeholders,
        hostedStks.toSet,
        fullTree.targetTimeProof,
      )

      originDomainParameters <- EitherT.right(originIps.findDynamicDomainParametersOrDefault())

      transferData = TransferData(
        ts,
        rc,
        fullTree,
        originDomainParameters.decisionTimeFor(ts),
        storedContract.contract,
        creatingTransactionId,
        None,
      )
      _ <- ifThenET(transferringParticipant) {
        transferCoordination.addTransferOutRequest(transferData)
      }
      confirmingStakeholders <- EitherT.right(
        storedContract.contract.metadata.stakeholders.toList.traverseFilter(stakeholder =>
          originIps.canConfirm(participantId, stakeholder).map(if (_) Some(stakeholder) else None)
        )
      )
      responseOpt = createTransferOutResponse(
        requestId,
        transferringParticipant,
        activenessResult,
        confirmingStakeholders.toSet,
        fullTree.viewHash,
        fullTree.tree.rootHash,
      )
    } yield StorePendingDataAndSendResponseAndCreateTimeout(
      entry,
      responseOpt.map(_ -> Recipients.cc(mediatorId)).toList,
      List.empty,
      (),
    )
  }

  private[this] def validateTransferOutRequest(
      txOutRequest: FullTransferOutTree,
      actualStakeholders: Set[LfPartyId],
      originIps: TopologySnapshot,
      recipients: Recipients,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Boolean] = {
    val isTransferringParticipant =
      txOutRequest.adminParties.contains(participantId.adminParty.toLf)
    val maybeTargetIpsF =
      if (isTransferringParticipant) {
        val targetTimestamp = txOutRequest.targetTimeProof.timestamp
        for {
          awaitO <- EitherT.fromEither[Future](
            transferCoordination.awaitTimestamp(
              txOutRequest.targetDomain,
              targetTimestamp,
              waitForEffectiveTime = true,
            )
          )
          /* Wait until the participant has received and processed all topology transactions on the target domain
           * up to the target-domain time proof timestamp.
           *
           * As we're not processing messages in parallel, delayed message processing on one domain can
           * block message processing on another domain and thus breaks isolation across domains.
           * Even with parallel processing, the cursors in the request journal would not move forward,
           * so event emission to the event log blocks, too.
           *
           * No deadlocks can arise under normal behaviour though.
           * For a deadlock, we would need cyclic waiting, i.e., a transfer out request on one domain D1 references
           * a time proof on another domain D2 and a earlier transfer-out request on D2 references a time proof on D3
           * and so on to domain Dn and an earlier transfer-out request on Dn references a later time proof on D1.
           * This, however, violates temporal causality of events.
           *
           * This argument breaks down for malicious participants
           * because the participant cannot verify that the time proof is authentic without having processed
           * all topology updates up to the declared timestamp as the sequencer's signing key might change.
           * So a malicious participant could fake a time proof and set a timestamp in the future,
           * which breaks causality.
           * With parallel processing of messages, deadlocks cannot occur as this waiting runs in parallel with
           * the request tracker, so time progresses on the target domain and eventually reaches the timestamp.
           */
          // TODO(M40): Prevent deadlocks. Detect non-sensible timestamps.
          _ <- EitherT.right(awaitO.getOrElse(Future.unit))
          targetCrypto <- transferCoordination.cryptoSnapshot(
            txOutRequest.targetDomain,
            targetTimestamp,
          )
          // TODO(M40): Verify sequencer signature on time proof
        } yield Some(targetCrypto.ipsSnapshot)
      } else EitherT.pure[Future, TransferProcessorError](None)
    maybeTargetIpsF
      .flatMap(targetIps =>
        validateTransferOutRequest(
          txOutRequest,
          actualStakeholders,
          originIps,
          targetIps,
          recipients,
        )
      )
      .map(_ => isTransferringParticipant)
  }

  override def pendingRequestMap
      : SyncDomainEphemeralState => concurrent.Map[RequestId, PendingRequestDataOrReplayData[
        PendingRequestData
      ]] =
    _.pendingTransferOuts

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, TransferOutResult],
      pendingRequestData: PendingTransferOut,
      pendingSubmissionMap: PendingSubmissions,
      tracker: SingleDomainCausalTracker,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val PendingTransferOut(
      requestId,
      requestCounter,
      _requestSequencerCounter,
      rootHash,
      WithContractHash(contractId, contractHash),
      transferringParticipant,
      transferId,
      targetDomain,
      stakeholders,
      hostedStakeholders,
      _targetTimeProof,
    ) = pendingRequestData

    val pendingSubmissionData = pendingSubmissionMap.get(rootHash)

    import scala.util.Either.MergeableEither
    MergeableEither[MediatorResult](result).merge.verdict match {
      case Verdict.Approve =>
        val commitSet = CommitSet(
          archivals = Map.empty,
          creations = Map.empty,
          transferOuts =
            Map(contractId -> WithContractHash(targetDomain -> stakeholders, contractHash)),
          transferIns = Map.empty,
          keyUpdates = Map.empty,
        )
        val commitSetFO = Some(Future.successful(commitSet))
        for {
          _unit <- ifThenET(transferringParticipant) {
            EitherT
              .fromEither[Future](DeliveredTransferOutResult.create(event))
              .leftMap(err => InvalidResult(err))
              .flatMap(deliveredResult =>
                transferCoordination.addTransferOutResult(targetDomain, deliveredResult)
              )
          }

          notInitiator = pendingSubmissionData.isEmpty
          _ <-
            if (notInitiator && transferringParticipant)
              triggerTransferInWhenExclusivityTimeoutExceeded(pendingRequestData)
            else EitherT.pure[Future, TransferProcessorError](())
          // TODO(#4027): Generate a Leave event unless the participant is transferring
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetFO,
          Set(),
          None,
          Some(TransferOutUpdate(hostedStakeholders, requestId.unwrap, transferId, requestCounter)),
        )

      case Verdict.RejectReasons(_) | (_: MediatorReject) | Verdict.Timeout =>
        for {
          _ <- ifThenET(transferringParticipant) {
            deleteTransfer(targetDomain, requestId)
          }
        } yield CommitAndStoreContractsAndPublishEvent(None, Set(), None, None)
    }
  }

  private[this] def triggerTransferInWhenExclusivityTimeoutExceeded(
      pendingRequestData: PendingRequestData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {

    val targetDomain = pendingRequestData.targetDomain
    val t0 = pendingRequestData.targetTimeProof.timestamp

    TransferOutProcessingSteps.autoTransferIn(
      pendingRequestData.transferId,
      targetDomain,
      transferCoordination,
      pendingRequestData.stakeholders,
      participantId,
      t0,
    )
  }

  private[this] def deleteTransfer(targetDomain: DomainId, transferOutRequestId: RequestId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val transferId = TransferId(domainId, transferOutRequestId.unwrap)
    transferCoordination.deleteTransfer(targetDomain, transferId)
  }

  private[this] def validateTransferOutRequest(
      request: FullTransferOutTree,
      expectedStakeholders: Set[LfPartyId],
      originIps: TopologySnapshot,
      maybeTargetIps: Option[TopologySnapshot],
      recipients: Recipients,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
    val stakeholders = request.stakeholders
    val adminParties = request.adminParties

    def checkStakeholderHasTransferringParticipant(
        stakeholder: LfPartyId
    ): Future[ValidatedNec[String, Unit]] =
      originIps
        .activeParticipantsOf(stakeholder)
        .map(
          _.filter(_._2.permission.canConfirm)
            .exists { case (participant, _) =>
              adminParties.contains(participant.adminParty.toLf)
            }
        )
        .map { validated =>
          Validated.condNec(
            validated,
            (),
            s"Stakeholder $stakeholder has no transferring participant.",
          )
        }

    val checkStakeholders: Either[TransferProcessorError, Unit] = condUnitE(
      stakeholders == expectedStakeholders,
      StakeholderMismatch(
        None,
        declaredViewStakeholders = stakeholders,
        declaredContractStakeholders = None,
        expectedStakeholders = Right(expectedStakeholders),
      ),
    )

    maybeTargetIps match {
      case None =>
        /* Checks that can be done by a non-transferring participant
         * - every stakeholder is hosted on a participant with an admin party
         * - the admin parties are hosted only on their participants
         */
        for {
          _ <- EitherT.fromEither[Future](checkStakeholders)
          _ <- EitherT(
            adminParties.toList
              .traverse(checkAdminParticipantCanConfirm(originIps, logger))
              .map(
                _.sequence.toEither.leftMap(errors =>
                  AdminPartyPermissionErrors(stringOfNec(errors)): TransferProcessorError
                )
              )
          )
          _ <- EitherT(
            stakeholders.toList
              .traverse(checkStakeholderHasTransferringParticipant)
              .map(
                _.sequence.toEither
                  .leftMap(errors =>
                    StakeholderHostingErrors(stringOfNec(errors)): TransferProcessorError
                  )
              )
          )
        } yield ()
      case Some(targetIps) =>
        for {
          _ <- EitherT.fromEither[Future](checkStakeholders)
          expectedTransferOutRequest <- TransferOutProcessingSteps
            .adminPartiesWithoutSubmitterCheck(
              request.submitter,
              expectedStakeholders,
              originIps,
              targetIps,
              logger,
            )
          (expectedAdminParties, expectedParticipants) = expectedTransferOutRequest
          _ <- EitherTUtil.condUnitET[Future](
            adminParties == expectedAdminParties,
            AdminPartiesMismatch(
              expected = expectedAdminParties,
              declared = adminParties,
            ): TransferProcessorError,
          )
          expectedRecipientsTree = Recipients.ofSet(expectedParticipants)
          _ <- EitherTUtil.condUnitET[Future](
            expectedRecipientsTree.contains(recipients),
            RecipientsMismatch(
              expected = expectedRecipientsTree,
              declared = recipients,
            ): TransferProcessorError,
          )
        } yield ()
    }
  }

  private[this] def createTransferOutResponse(
      requestId: RequestId,
      transferringParticipant: Boolean,
      activenessResult: ActivenessResult,
      confirmingStakeholders: Set[LfPartyId],
      viewHash: ViewHash,
      rootHash: RootHash,
  ): Option[MediatorResponse] =
    // send a response only if the participant is a transferring participant or the activeness check has failed
    if (transferringParticipant || !activenessResult.isSuccessful) {
      val adminPartySet =
        if (transferringParticipant) Set(participantId.adminParty.toLf) else Set.empty[LfPartyId]
      val confirmingParties = confirmingStakeholders union adminPartySet
      val localVerdict =
        if (activenessResult.isSuccessful) LocalApprove
        else LocalReject.TransferOutRejects.ActivenessCheckFailed.Reject(s"$activenessResult")
      val response = checked(
        MediatorResponse.tryCreate(
          requestId,
          participantId,
          Some(viewHash),
          localVerdict,
          Some(rootHash),
          confirmingParties,
          domainId,
        )
      )
      Some(response)
    } else None

}

object TransferOutProcessingSteps {

  import com.digitalasset.canton.util.ShowUtil._

  case class SubmissionParam(
      submittingParty: LfPartyId,
      contractId: LfContractId,
      targetDomain: DomainId,
  )

  case class SubmissionResult(
      transferId: TransferId,
      transferOutCompletionF: Future[com.google.rpc.status.Status],
  )

  case class PendingTransferOut(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contractIdAndHash: WithContractHash[LfContractId],
      transferringParticipant: Boolean,
      transferId: TransferId,
      targetDomain: DomainId,
      stakeholders: Set[LfPartyId],
      hostedStakeholders: Set[LfPartyId],
      targetTimeProof: TimeProof,
  ) extends PendingTransfer
      with PendingRequestData {
    override def pendingContracts: Set[LfContractId] = Set()
  }

  def createTransferOutRequest(
      participantId: ParticipantId,
      timeProof: TimeProof,
      contractId: LfContractId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      originDomain: DomainId,
      originMediator: MediatorId,
      targetDomain: DomainId,
      originIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, (TransferOutRequest, Set[Member])] = {

    for {
      adminPartiesAndRecipients <- transferOutRequestData(
        participantId,
        submitter,
        stakeholders,
        originIps,
        targetIps,
        logger,
      )
      (transferOutAdminParties, recipients) = adminPartiesAndRecipients
    } yield {
      val transferOutRequest = TransferOutRequest(
        submitter,
        stakeholders,
        transferOutAdminParties,
        contractId,
        originDomain,
        originMediator,
        targetDomain,
        timeProof,
      )

      (transferOutRequest, recipients)
    }
  }

  def transferOutRequestData(
      participantId: ParticipantId,
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      originIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, (Set[LfPartyId], Set[Member])] = {
    for {
      canSubmit <- EitherT.right(
        originIps.hostedOn(submitter, participantId).map(_.exists(_.permission == Submission))
      )
      _ <- EitherTUtil.condUnitET[Future](
        canSubmit,
        NoSubmissionPermission(None, submitter, participantId),
      )
      adminPartiesAndRecipients <- adminPartiesWithoutSubmitterCheck(
        submitter,
        stakeholders,
        originIps,
        targetIps,
        logger,
      )
    } yield {
      adminPartiesAndRecipients
    }
  }

  private def adminPartiesWithoutSubmitterCheck(
      submitter: LfPartyId,
      stakeholders: Set[LfPartyId],
      originIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, (Set[LfPartyId], Set[Member])] = {

    val stakeholdersWithParticipantPermissionsF = stakeholders.toList
      .traverse { stakeholder =>
        val originF = partyParticipants(originIps, stakeholder)
        val targetF = partyParticipants(targetIps, stakeholder)
        for {
          origin <- originF
          target <- targetF
        } yield (stakeholder, (origin, target))
      }
      .map(_.toMap)

    for {
      stakeholdersWithParticipantPermissions <- EitherT.right(
        stakeholdersWithParticipantPermissionsF
      )
      _ <- EitherT.cond[Future](
        stakeholders.contains(submitter),
        (),
        SubmittingPartyMustBeStakeholder(None, submitter, stakeholders),
      )
      transferOutParticipants <- EitherT.fromEither[Future](
        transferOutParticipants(stakeholdersWithParticipantPermissions)
      )
      transferOutAdminParties <- EitherT(
        transferOutParticipants.toList
          .traverse(adminPartyFor(originIps, logger))
          .map(
            _.sequence.toEither
              .bimap(
                errors =>
                  AdminPartyPermissionErrors(
                    s"${errors.toChain.toList.mkString(", ")}"
                  ): TransferProcessorError,
                _.toSet,
              )
          )
      )
    } yield {
      val participants =
        stakeholdersWithParticipantPermissions.values
          .map(_._1.all)
          .foldLeft[Set[Member]](Set.empty[Member])(_ ++ _)
      (transferOutAdminParties, participants)
    }
  }

  private def adminPartyFor(originIps: TopologySnapshot, logger: TracedLogger)(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ValidatedNec[String, LfPartyId]] = {
    val adminParty = participant.adminParty.toLf
    confirmingAdminParticipants(originIps, adminParty, logger).map { adminParticipants =>
      Validated.condNec(
        adminParticipants.get(participant).exists(_.permission.canConfirm),
        adminParty,
        s"Transfer-out participant $participant cannot confirm on behalf of its admin party.",
      )
    }
  }

  private def checkAdminParticipantCanConfirm(originIps: TopologySnapshot, logger: TracedLogger)(
      adminParty: LfPartyId
  )(implicit traceContext: TraceContext, ec: ExecutionContext): Future[ValidatedNec[String, Unit]] =
    confirmingAdminParticipants(originIps, adminParty, logger).map { adminParticipants =>
      Validated.condNec(
        adminParticipants.exists { case (participant, _) =>
          participant.adminParty.toLf == adminParty
        },
        (),
        s"Admin party $adminParty not hosted on its transfer-out participant with confirmation permission.",
      )
    }

  private def confirmingAdminParticipants(
      ips: TopologySnapshot,
      adminParty: LfPartyId,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Map[ParticipantId, ParticipantAttributes]] = {
    ips.activeParticipantsOf(adminParty).map(_.filter(_._2.permission.canConfirm)).map {
      adminParticipants =>
        if (adminParticipants.sizeCompare(1) != 0)
          logger.warn(
            s"Admin party $adminParty hosted with confirmation permission on ${adminParticipants.keySet.toString}."
          )
        adminParticipants
    }
  }

  /** Computes the transfer-out participants for the transfers and checks the following hosting requirements for each party:
    * <ul>
    * <li>If the party is hosted on a participant with [[identity.ParticipantPermission$.Submission$]] permission,
    * then at least one such participant must also have [[identity.ParticipantPermission$.Submission$]] permission
    * for that party on the target domain.
    * This ensures that the party can initiate the transfer-in if needed and continue to use the contract on the
    * target domain, unless the permissions change in between.
    * </li>
    * <li>The party must be hosted on a participant that has [[identity.ParticipantPermission$.Confirmation$]] permission
    * on both domains for this party.
    * </li>
    * </ul>
    */
  private def transferOutParticipants(
      participantsByParty: Map[LfPartyId, (PartyParticipants, PartyParticipants)]
  ): Either[TransferProcessorError, Set[ParticipantId]] = {

    participantsByParty.toList
      .traverse { case (party, (origin, target)) =>
        val submissionPermissionCheck = Validated.condNec(
          origin.submitters.isEmpty || origin.submitters.exists(target.submitters.contains),
          (),
          show"For party $party, no participant with submission permission on origin domain has submission permission on target domain.",
        )

        val transferOutParticipants = origin.confirmers.intersect(target.confirmers)
        val confirmersOverlap =
          Validated.condNec(
            transferOutParticipants.nonEmpty,
            transferOutParticipants,
            show"No participant of the party $party has confirmation permission on both domains.",
          )

        Applicative[ValidatedNec[String, *]]
          .productR(submissionPermissionCheck)(confirmersOverlap)
      }
      .bimap(errors => PermissionErrors(s"${stringOfNec(errors)}"), MonoidK[Set].algebra.combineAll)
      .toEither
  }

  private case class PartyParticipants(
      submission: Set[ParticipantId],
      confirmation: Set[ParticipantId],
      other: Set[ParticipantId],
  ) {
    require(
      !submission.exists(confirmation.contains),
      "submission and confirmation permissions must be disjoint.",
    )
    require(
      !submission.exists(other.contains),
      "submission and other permissions must be disjoint.",
    )
    require(
      !confirmation.exists(other.contains),
      "confirmation and other permissions must be disjoint.",
    )

    def submitters: Set[ParticipantId] = submission

    def confirmers: Set[ParticipantId] = submission ++ confirmation

    def all: Set[ParticipantId] = submission ++ confirmation ++ other
  }

  private def partyParticipants(ips: TopologySnapshot, party: LfPartyId)(implicit
      ec: ExecutionContext
  ): Future[PartyParticipants] = {

    val submission = mutable.Set.newBuilder[ParticipantId]
    val confirmation = mutable.Set.newBuilder[ParticipantId]
    val other = mutable.Set.newBuilder[ParticipantId]

    ips.activeParticipantsOf(party).map { partyParticipants =>
      partyParticipants.foreach {
        case (participantId, ParticipantAttributes(permission, _trustLevel)) =>
          permission match {
            case Submission => submission += participantId
            case Confirmation => confirmation += participantId
            case Observation => other += participantId
            case Disabled =>
              throw new IllegalStateException(
                s"activeParticipantsOf($party) returned a disabled participant $participantId"
              )
          }
      }
      PartyParticipants(
        submission.result().toSet,
        confirmation.result().toSet,
        other.result().toSet,
      )
    }

  }

  def autoTransferIn(
      id: TransferId,
      targetDomain: DomainId,
      transferCoordination: TransferCoordination,
      stks: Set[LfPartyId],
      participantId: ParticipantId,
      t0: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val logger = elc.logger
    implicit val tc = elc.traceContext

    def hostedStakeholders(snapshot: TopologySnapshot): Future[Set[LfPartyId]] = {
      stks.toList
        .traverseFilter { partyId =>
          snapshot
            .hostedOn(partyId, participantId)
            .map(x => x.filter(_.permission == ParticipantPermission.Submission).map(_ => partyId))
        }
        .map(_.toSet)
    }

    def performAutoInOnce: EitherT[Future, TransferProcessorError, com.google.rpc.status.Status] = {
      for {
        targetIps <- transferCoordination
          .getTimeProofAndSnapshot(targetDomain)
          .map(_._2)
          .onShutdown(Left(DomainNotReady(targetDomain, "Shutdown of time tracker")))
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetIps.ipsSnapshot))
        inParty <- EitherT.fromOption[Future](
          possibleSubmittingParties.headOption,
          AutomaticTransferInError("No possible submitting party for automatic transfer-in"),
        )
        submissionResult <- transferCoordination
          .transferIn(targetDomain, inParty, id)(TraceContext.empty)
        TransferInProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.liftF(completionF)
      } yield status
    }

    def performAutoInRepeatedly: EitherT[Future, TransferProcessorError, Unit] = {
      case class StopRetry(result: Either[TransferProcessorError, com.google.rpc.status.Status])
      val retryCount = 5

      def tryAgain(
          previous: com.google.rpc.status.Status
      ): EitherT[Future, StopRetry, com.google.rpc.status.Status] = {
        if (BaseCantonError.isStatusErrorCode(MediatorReject.Timeout, previous))
          performAutoInOnce.leftMap(error => StopRetry(Left(error)))
        else EitherT.leftT[Future, com.google.rpc.status.Status](StopRetry(Right(previous)))
      }

      val initial = performAutoInOnce.leftMap(error => StopRetry(Left(error)))
      val result = MonadUtil.repeatFlatmap(initial, tryAgain, retryCount)

      result.transform {
        case Left(StopRetry(Left(error))) => Left(error)
        case Left(StopRetry(Right(verdict))) => Right(())
        case Right(verdict) => Right(())
      }
    }

    def triggerAutoIn(
        targetSnapshot: TopologySnapshot,
        targetDomainParameters: DynamicDomainParameters,
    ): Unit = {
      val timeoutTimestamp = targetDomainParameters.transferExclusivityLimitFor(t0)

      val autoIn = for {
        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _unit <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of transfer-in with ID ${id} at time $timeoutTimestamp, where base timestamp is $t0"
            )
            for {
              timeoutFuture <- EitherT.fromEither[Future](
                transferCoordination.awaitTimestamp(
                  targetDomain,
                  timeoutTimestamp,
                  waitForEffectiveTime = false,
                )
              )
              _ <- EitherT.liftF[Future, TransferProcessorError, Unit](timeoutFuture.getOrElse {
                logger.debug(s"Automatic transfer-in triggered immediately")
                Future.unit
              })
              _unit <- EitherTUtil.leftSubflatMap(performAutoInRepeatedly) {
                // Filter out submission errors occurring because the transfer is already completed
                case NoTransferData(id, TransferCompleted(transferId, timeOfCompletion)) =>
                  Right(())
                // Filter out the case that the participant has disconnected from the target domain in the meantime.
                case UnknownDomain(domain, _reason) if domain == targetDomain =>
                  Right(())
                case DomainNotReady(domain, _reason) if domain == targetDomain =>
                  Right(())
                // Filter out the case that the target domain is closing right now
                case other => Left(other)
              }
            } yield ()
          } else EitherT.pure[Future, TransferProcessorError](())
      } yield ()

      EitherTUtil.doNotAwait(autoIn, "Automatic transfer-in failed", Level.INFO)
    }

    for {
      targetIps <- transferCoordination.cryptoSnapshot(targetDomain, t0)
      targetSnapshot = targetIps.ipsSnapshot
      targetDomainParameters <- EitherTUtil
        .fromFuture(
          targetSnapshot.findDynamicDomainParametersOrDefault(),
          _ => UnknownDomain(targetDomain, "When fetching domain parameters"),
        )
        .leftWiden[TransferProcessorError]
    } yield {

      if (targetDomainParameters.automaticTransferInEnabled)
        triggerAutoIn(targetSnapshot, targetDomainParameters)
      else ()
    }
  }

  private def stringOfNec[A](chain: NonEmptyChain[String]): String = chain.toList.mkString(", ")

  case class PendingDataAndResponseArgs(
      txOutRequest: FullTransferOutTree,
      recipients: Recipients,
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      originSnapshot: DomainSnapshotSyncCryptoApi,
  )

  sealed trait TransferOutProcessorError extends TransferProcessorError

  case class UnexpectedDomain(transferId: TransferId, receivedOn: DomainId)
      extends TransferOutProcessorError

  case class TargetDomainIsOriginDomain(domain: DomainId, contractId: LfContractId)
      extends TransferOutProcessorError

  case class UnknownContract(contractId: LfContractId) extends TransferOutProcessorError

  case class InvalidResult(result: DeliveredTransferOutResult.InvalidTransferOutResult)
      extends TransferOutProcessorError

  case class AutomaticTransferInError(message: String) extends TransferOutProcessorError

  case class PermissionErrors(message: String) extends TransferOutProcessorError

  case class AdminPartyPermissionErrors(message: String) extends TransferOutProcessorError

  case class StakeholderHostingErrors(message: String) extends TransferOutProcessorError

  case class AdminPartiesMismatch(expected: Set[LfPartyId], declared: Set[LfPartyId])
      extends TransferOutProcessorError

  case class RecipientsMismatch(expected: Option[Recipients], declared: Recipients)
      extends TransferOutProcessorError

  case class ContractStoreFailed(error: ContractStoreError) extends TransferProcessorError

}
