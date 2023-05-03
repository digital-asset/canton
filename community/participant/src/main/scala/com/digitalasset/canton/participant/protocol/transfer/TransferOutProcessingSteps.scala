// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps, Signature}
import com.digitalasset.canton.data.ViewType.TransferOutViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransferOutTree,
  TransferSubmitterMetadata,
  ViewType,
}
import com.digitalasset.canton.ledger.participant.state.v2.CompletionInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.PendingRequestData
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
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps.*
import com.digitalasset.canton.participant.protocol.transfer.TransferOutRequestValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{ProcessingSteps, ProtocolProcessor}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  TransferCounter,
  checked,
}

import scala.concurrent.{ExecutionContext, Future}

class TransferOutProcessingSteps(
    domainId: SourceDomainId,
    val participantId: ParticipantId,
    val engine: DAMLe,
    transferCoordination: TransferCoordination,
    seedGenerator: SeedGenerator,
    sourceDomainProtocolVersion: SourceProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransferProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      TransferOutViewType,
      TransferOutResult,
      PendingTransferOut,
    ]
    with NamedLogging {

  override type SubmissionResultArgs = PendingTransferSubmission

  override type PendingDataAndResponseArgs = TransferOutProcessingSteps.PendingDataAndResponseArgs

  override type RequestType = ProcessingSteps.RequestType.TransferOut
  override val requestType = ProcessingSteps.RequestType.TransferOut

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
      sourceRecentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, Submission] = {
    val SubmissionParam(
      submitterMetadata,
      contractId,
      targetDomain,
      targetProtocolVersion,
    ) = param
    val pureCrypto = sourceRecentSnapshot.pureCrypto

    def withDetails(message: String) = s"Transfer-out $contractId to $targetDomain: $message"

    for {
      _ <- condUnitET[FutureUnlessShutdown](
        targetDomain.unwrap != domainId.id,
        TargetDomainIsSourceDomain(domainId.unwrap, contractId),
      )
      storedContract <- getStoredContract(ephemeralState.contractLookup, contractId).mapK(
        FutureUnlessShutdown.outcomeK
      )
      stakeholders = storedContract.contract.metadata.stakeholders
      templateId = storedContract.contract.rawContractInstance.contractInstance.unversioned.template

      /*
        In PV=4, we introduced the sourceProtocolVersion in TransferInView, which is needed for
        proper deserialization. Hence, we disallow some transfers
       */
      missingSourceProtocolVersionInTransferIn = targetProtocolVersion.v <= ProtocolVersion.v3
      isSourceProtocolVersionRequired = sourceDomainProtocolVersion.v >= ProtocolVersion.v4

      _ <- condUnitET[FutureUnlessShutdown](
        !(missingSourceProtocolVersionInTransferIn && isSourceProtocolVersionRequired),
        IncompatibleProtocolVersions(contractId, sourceDomainProtocolVersion, targetProtocolVersion),
      )

      timeProofAndSnapshot <- transferCoordination.getTimeProofAndSnapshot(targetDomain)
      (timeProof, targetCrypto) = timeProofAndSnapshot
      _ = logger.debug(withDetails(s"Picked time proof ${timeProof.timestamp}"))

      transferOutRequestAndRecipients <- TransferOutProcessingSteps
        .createTransferOutRequest(
          participantId,
          timeProof,
          contractId,
          templateId,
          submitterMetadata,
          stakeholders,
          domainId,
          sourceDomainProtocolVersion,
          mediatorId,
          targetDomain,
          targetProtocolVersion,
          sourceRecentSnapshot.ipsSnapshot,
          targetCrypto.ipsSnapshot,
          TransferCounter.Genesis, // TODO(#12286): replace by the value from the stored contract
          logger,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      (transferOutRequest, recipients) = transferOutRequestAndRecipients

      transferOutUuid = seedGenerator.generateUuid()
      seed = seedGenerator.generateSaltSeed()
      fullTree = transferOutRequest.toFullTransferOutTree(
        pureCrypto,
        pureCrypto,
        seed,
        transferOutUuid,
      )
      mediatorMessage = fullTree.mediatorMessage
      rootHash = fullTree.rootHash
      viewMessage <- EncryptedViewMessageFactory
        .create(TransferOutViewType)(fullTree, sourceRecentSnapshot, sourceDomainProtocolVersion.v)
        .leftMap[TransferProcessorError](EncryptionError(contractId, _))
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
          domainId.unwrap,
          sourceDomainProtocolVersion.v,
          ViewType.TransferOutViewType,
          EmptyRootHashMessagePayload,
        )
      val rootHashRecipients =
        Recipients.groups(
          checked(
            NonEmptyUtil.fromUnsafe(
              recipients.toSeq.map(participant => NonEmpty(Set, mediatorId, participant: Member))
            )
          )
        )
      // Each member gets a message sent to itself and to the mediator
      val messages = Seq[(ProtocolMessage, Recipients)](
        mediatorMessage -> Recipients.cc(mediatorId),
        viewMessage -> recipientsT,
        rootHashMessage -> rootHashRecipients,
      )
      TransferSubmission(Batch.of(sourceDomainProtocolVersion.v, messages: _*), rootHash)
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

  private[this] def getStoredContract(
      contractLookup: ContractLookup,
      contractId: LfContractId,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, StoredContract] =
    contractLookup
      .lookup(contractId)
      .toRight[TransferProcessorError](TransferOutRequestValidation.UnknownContract(contractId))

  override protected def decryptTree(sourceSnapshot: DomainSnapshotSyncCryptoApi)(
      envelope: OpenEnvelope[EncryptedViewMessage[TransferOutViewType]]
  ): EitherT[Future, EncryptedViewMessageDecryptionError[TransferOutViewType], WithRecipients[
    FullTransferOutTree
  ]] = {
    EncryptedViewMessage
      .decryptFor(
        sourceSnapshot,
        envelope.protocolMessage,
        participantId,
        sourceDomainProtocolVersion.v,
      ) { bytes =>
        FullTransferOutTree
          .fromByteString(sourceSnapshot.pureCrypto)(bytes)
          .leftMap(e => DefaultDeserializationError(e.toString))
      }
      .map(WithRecipients(_, envelope.recipients))
  }

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[FullTransferOutTree], Option[Signature])]
      ],
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload],
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CheckActivenessAndWritePendingContracts] = {
    val correctRootHashes = decryptedViewsWithSignatures.map { case (rootHashes, _) => rootHashes }
    // TODO(M40): Send a rejection if malformedPayloads is non-empty
    for {
      txOutRequestAndRecipients <- EitherT.cond[Future](
        correctRootHashes.toList.sizeCompare(1) == 0,
        correctRootHashes.head1,
        ReceivedMultipleRequests(correctRootHashes.map(_.unwrap.viewHash)): TransferProcessorError,
      )
      WithRecipients(txOutRequest, recipients) = txOutRequestAndRecipients
      contractId = txOutRequest.contractId
      _ <- condUnitET[Future](
        txOutRequest.sourceDomain == domainId,
        UnexpectedDomain(
          TransferId(txOutRequest.sourceDomain, ts),
          domainId.unwrap,
        ),
      ).leftWiden[TransferProcessorError]
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
      PendingDataAndResponseArgs(txOutRequest, recipients, ts, rc, sc, sourceSnapshot),
    )
  }

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      activenessF: FutureUnlessShutdown[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    val PendingDataAndResponseArgs(fullTree, recipients, ts, rc, sc, sourceSnapshot) =
      pendingDataAndResponseArgs

    val transferId: TransferId = TransferId(fullTree.sourceDomain, ts)

    for {
      // Wait for earlier writes to the contract store having completed
      _ <- EitherT.right(FutureUnlessShutdown.outcomeF(pendingCursor))

      // Since the transfer out request should be sent only to participants that host a stakeholder of the contract,
      // we can expect to find the contract in the contract store.
      storedContract <- getStoredContract(contractLookup, fullTree.contractId).mapK(
        FutureUnlessShutdown.outcomeK
      )

      sourceIps = sourceSnapshot.ipsSnapshot
      validationRes <- validateTransferOutRequest(
        fullTree,
        storedContract.contract.metadata.stakeholders,
        sourceIps,
        recipients,
      ).mapK(FutureUnlessShutdown.outcomeK)
      (transferringParticipant, transferInExclusivity) = validationRes
      // Since the participant hosts a stakeholder, it should find the creating transaction ID in the contract store
      creatingTransactionId <- EitherT.fromEither[FutureUnlessShutdown](
        storedContract.creatingTransactionIdO
          .toRight(CreatingTransactionIdNotFound(storedContract.contractId))
      )

      activenessResult <- EitherT.right(activenessF)

      hostedStks <- EitherT.liftF(
        FutureUnlessShutdown.outcomeF(hostedStakeholders(fullTree.stakeholders.toList, sourceIps))
      )

      requestId = RequestId(ts)
      entry = PendingTransferOut(
        requestId,
        rc,
        sc,
        fullTree.tree.rootHash,
        WithContractHash.fromContract(storedContract.contract, fullTree.contractId),
        storedContract.contract.rawContractInstance.contractInstance.unversioned.template,
        transferringParticipant,
        fullTree.submitterMetadata,
        transferId,
        fullTree.targetDomain,
        fullTree.stakeholders,
        hostedStks.toSet,
        fullTree.targetTimeProof,
        transferInExclusivity,
        mediatorId,
      )

      transferOutDecisionTime <- ProcessingSteps
        .getDecisionTime(sourceIps, ts)
        .leftMap(TransferParametersError(domainId.unwrap, _))
        .mapK(FutureUnlessShutdown.outcomeK)

      transferData = TransferData(
        sourceProtocolVersion = sourceDomainProtocolVersion,
        transferOutTimestamp = ts,
        transferOutRequestCounter = rc,
        transferOutRequest = fullTree,
        transferOutDecisionTime = transferOutDecisionTime,
        contract = storedContract.contract,
        transferCounter =
          TransferCounter.Genesis, // TODO(#12286) add a transfer counter to the StoredContract
        creatingTransactionId = creatingTransactionId,
        transferOutResult = None,
      )
      _ <- ifThenET(transferringParticipant) {
        transferCoordination.addTransferOutRequest(transferData).mapK(FutureUnlessShutdown.outcomeK)
      }
      confirmingStakeholders <- EitherT.right(
        storedContract.contract.metadata.stakeholders.toList.parTraverseFilter(stakeholder =>
          FutureUnlessShutdown.outcomeF(
            sourceIps.canConfirm(participantId, stakeholder).map(if (_) Some(stakeholder) else None)
          )
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
      RejectionArgs(
        entry,
        LocalReject.TimeRejects.LocalTimeout.Reject(sourceDomainProtocolVersion.v),
      ),
    )
  }

  private[this] def validateTransferOutRequest(
      txOutRequest: FullTransferOutTree,
      actualStakeholders: Set[LfPartyId],
      sourceIps: TopologySnapshot,
      recipients: Recipients,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, (Boolean, Option[CantonTimestamp])] = {
    val isTransferringParticipant =
      txOutRequest.adminParties.contains(participantId.adminParty.toLf)
    val targetTimestamp = txOutRequest.targetTimeProof.timestamp
    val maybeTargetIpsF =
      if (isTransferringParticipant) {
        for {
          awaitO <- EitherT.fromEither[Future](
            transferCoordination.awaitTimestamp(
              txOutRequest.targetDomain.unwrap,
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
            txOutRequest.targetDomain.unwrap,
            targetTimestamp,
          )
          // TODO(M40): Verify sequencer signature on time proof
        } yield Some(targetCrypto.ipsSnapshot)
      } else EitherT.pure[Future, TransferProcessorError](None)

    for {
      targetIps <- maybeTargetIpsF
      transferInExclusivity <- validateTransferOutRequest(
        txOutRequest,
        actualStakeholders,
        sourceIps,
        targetIps,
        recipients,
      )
    } yield isTransferringParticipant -> transferInExclusivity
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[MalformedMediatorRequestResult, TransferOutResult],
      pendingRequestData: PendingTransferOut,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val PendingTransferOut(
      requestId,
      requestCounter,
      requestSequencerCounter,
      rootHash,
      WithContractHash(contractId, contractHash),
      templateId,
      transferringParticipant,
      submitterMetadata,
      transferId,
      targetDomain,
      stakeholders,
      hostedStakeholders,
      _targetTimeProof,
      transferInExclusivity,
      _,
    ) = pendingRequestData

    val pendingSubmissionData = pendingSubmissionMap.get(rootHash)

    import scala.util.Either.MergeableEither
    MergeableEither[MediatorResult](resultE).merge.verdict match {
      case _: Verdict.Approve =>
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
              .fromEither[Future](DeliveredTransferOutResult.create(eventE))
              .leftMap(err => InvalidResult(transferId, err))
              .flatMap(deliveredResult =>
                transferCoordination.addTransferOutResult(targetDomain, deliveredResult)
              )
          }

          notInitiator = pendingSubmissionData.isEmpty
          _ <-
            if (notInitiator && transferringParticipant)
              triggerTransferInWhenExclusivityTimeoutExceeded(pendingRequestData)
            else EitherT.pure[Future, TransferProcessorError](())

          transferOutEvent <- createTransferredOut(
            requestId.unwrap,
            contractId,
            templateId,
            stakeholders,
            submitterMetadata,
            transferId,
            targetDomain,
            rootHash,
            transferInExclusivity,
          )
        } yield CommitAndStoreContractsAndPublishEvent(
          commitSetFO,
          Set(),
          Some(
            TimestampedEvent(
              transferOutEvent,
              requestCounter.asLocalOffset,
              Some(requestSequencerCounter),
            )
          ),
        )

      case reasons: Verdict.ParticipantReject =>
        for {
          _ <- ifThenET(transferringParticipant) {
            deleteTransfer(targetDomain, requestId)
          }

          // TODO(M40): Implement checks against malicious rejections and scrutinize the reasons such that an alarm is raised if necessary
          tsEventO <- EitherT
            .fromEither[Future](
              createRejectionEvent(RejectionArgs(pendingRequestData, reasons.keyEvent))
            )
        } yield CommitAndStoreContractsAndPublishEvent(None, Set(), tsEventO)

      case _: MediatorReject =>
        for {
          _ <- ifThenET(transferringParticipant) {
            deleteTransfer(targetDomain, requestId)
          }
        } yield CommitAndStoreContractsAndPublishEvent(None, Set(), None)
    }
  }

  private def createTransferredOut(
      recordTime: CantonTimestamp,
      contractId: LfContractId,
      templateId: LfTemplateId,
      contractStakeholders: Set[LfPartyId],
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      targetDomain: TargetDomainId,
      rootHash: RootHash,
      transferInExclusivity: Option[CantonTimestamp],
  ): EitherT[Future, TransferProcessorError, LedgerSyncEvent.TransferredOut] = {
    for {
      updateId <- EitherT
        .fromEither[Future](rootHash.asLedgerTransactionId)
        .leftMap[TransferProcessorError](FieldConversionError(transferId, "Transaction Id", _))

      completionInfo =
        Option.when(participantId.toLf == submitterMetadata.submittingParticipant)(
          CompletionInfo(
            actAs = List(submitterMetadata.submitter),
            applicationId = submitterMetadata.applicationId,
            commandId = submitterMetadata.commandId,
            optDeduplicationPeriod = None,
            submissionId = submitterMetadata.submissionId,
            statistics = None,
          )
        )
    } yield LedgerSyncEvent.TransferredOut(
      updateId = updateId,
      optCompletionInfo = completionInfo,
      submitter = submitterMetadata.submitter,
      recordTime = recordTime.toLf,
      contractId = contractId,
      templateId = Some(templateId),
      contractStakeholders = contractStakeholders,
      sourceDomainId = transferId.sourceDomain,
      targetDomainId = targetDomain,
      transferInExclusivity = transferInExclusivity.map(_.toLf),
      workflowId = submitterMetadata.workflowId,
    )
  }

  private[this] def triggerTransferInWhenExclusivityTimeoutExceeded(
      pendingRequestData: RequestType#PendingRequestData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {

    val targetDomain = pendingRequestData.targetDomain
    val t0 = pendingRequestData.targetTimeProof.timestamp

    AutomaticTransferIn.perform(
      pendingRequestData.transferId,
      targetDomain,
      transferCoordination,
      pendingRequestData.stakeholders,
      pendingRequestData.submitterMetadata,
      participantId,
      t0,
    )
  }

  private[this] def deleteTransfer(targetDomain: TargetDomainId, transferOutRequestId: RequestId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val transferId = TransferId(domainId, transferOutRequestId.unwrap)
    transferCoordination.deleteTransfer(targetDomain, transferId)
  }

  private[this] def validateTransferOutRequest(
      request: FullTransferOutTree,
      expectedStakeholders: Set[LfPartyId],
      sourceIps: TopologySnapshot,
      maybeTargetIps: Option[TopologySnapshot],
      recipients: Recipients,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[CantonTimestamp]] = {
    val stakeholders = request.stakeholders
    val adminParties = request.adminParties

    def checkStakeholderHasTransferringParticipant(
        stakeholder: LfPartyId
    ): Future[ValidatedNec[String, Unit]] =
      sourceIps
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
      StakeholdersMismatch(
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
              .parTraverse(
                TransferOutRequestValidation.checkAdminParticipantCanConfirm(sourceIps, logger)
              )
              .map(
                _.sequence.toEither.leftMap(errors =>
                  AdminPartyPermissionErrors(stringOfNec(errors))
                )
              )
          ).leftWiden[TransferProcessorError]
          _ <- EitherT(
            stakeholders.toList
              .parTraverse(checkStakeholderHasTransferringParticipant)
              .map(
                _.sequence.toEither.leftMap(errors => StakeholderHostingErrors(stringOfNec(errors)))
              )
          ).leftWiden[TransferProcessorError]
        } yield None
      case Some(targetIps) =>
        for {
          _ <- EitherT.fromEither[Future](checkStakeholders)
          adminPartiesAndParticipants <- TransferOutRequestValidation
            .adminPartiesWithoutSubmitterCheck(
              request.contractId,
              request.submitter,
              expectedStakeholders,
              sourceIps,
              targetIps,
              logger,
            )
          AdminPartiesAndParticipants(expectedAdminParties, expectedParticipants) =
            adminPartiesAndParticipants

          transferInExclusivity <- ProcessingSteps
            .getTransferInExclusivity(
              targetIps,
              request.targetTimeProof.timestamp,
            )
            .leftMap(TransferParametersError(request.targetDomain.unwrap, _))

          _ <- EitherTUtil.condUnitET[Future](
            adminParties == expectedAdminParties,
            AdminPartiesMismatch(
              contractId = request.contractId,
              expected = expectedAdminParties,
              declared = adminParties,
            ),
          )
          expectedRecipientsTree = Recipients.ofSet(expectedParticipants)
          _ <- EitherTUtil
            .condUnitET[Future](
              expectedRecipientsTree.contains(recipients),
              RecipientsMismatch(
                contractId = request.contractId,
                expected = expectedRecipientsTree,
                declared = recipients,
              ),
            )
            .leftWiden[TransferProcessorError]
        } yield Some(transferInExclusivity)
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
        if (activenessResult.isSuccessful) LocalApprove(sourceDomainProtocolVersion.v)
        else
          LocalReject.TransferOutRejects.ActivenessCheckFailed.Reject(s"$activenessResult")(
            LocalVerdict.protocolVersionRepresentativeFor(sourceDomainProtocolVersion.v)
          )
      val response = checked(
        MediatorResponse.tryCreate(
          requestId,
          participantId,
          Some(viewHash),
          localVerdict,
          Some(rootHash),
          confirmingParties,
          domainId.unwrap,
          sourceDomainProtocolVersion.v,
        )
      )
      Some(response)
    } else None

}

object TransferOutProcessingSteps {
  private def stringOfNec[A](chain: NonEmptyChain[String]): String = chain.toList.mkString(", ")

  final case class SubmissionParam(
      submitterMetadata: TransferSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
  ) {
    val submittingParty: LfPartyId = submitterMetadata.submitter
  }

  final case class SubmissionResult(
      transferId: TransferId,
      transferOutCompletionF: Future[com.google.rpc.status.Status],
  )

  final case class PendingTransferOut(
      override val requestId: RequestId,
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      rootHash: RootHash,
      contractIdAndHash: WithContractHash[LfContractId],
      templateId: LfTemplateId,
      transferringParticipant: Boolean,
      submitterMetadata: TransferSubmitterMetadata,
      transferId: TransferId,
      targetDomain: TargetDomainId,
      stakeholders: Set[LfPartyId],
      hostedStakeholders: Set[LfPartyId],
      targetTimeProof: TimeProof,
      transferInExclusivity: Option[CantonTimestamp],
      mediatorId: MediatorId,
  ) extends PendingTransfer
      with PendingRequestData {
    override def pendingContracts: Set[LfContractId] = Set()
  }

  def createTransferOutRequest(
      participantId: ParticipantId,
      timeProof: TimeProof,
      contractId: LfContractId,
      templateId: LfTemplateId,
      submitterMetadata: TransferSubmitterMetadata,
      stakeholders: Set[LfPartyId],
      sourceDomain: SourceDomainId,
      sourceProtocolVersion: SourceProtocolVersion,
      sourceMediator: MediatorId,
      targetDomain: TargetDomainId,
      targetProtocolVersion: TargetProtocolVersion,
      sourceIps: TopologySnapshot,
      targetIps: TopologySnapshot,
      transferCounter: TransferCounter,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransferProcessorError, (TransferOutRequest, Set[ParticipantId])] =
    for {
      adminPartiesAndRecipients <- TransferOutRequestValidation.adminPartiesWithSubmitterCheck(
        participantId,
        contractId,
        submitterMetadata.submitter,
        stakeholders,
        sourceIps,
        targetIps,
        logger,
      )
    } yield {
      val transferOutRequest = TransferOutRequest(
        submitterMetadata,
        stakeholders,
        adminPartiesAndRecipients.adminParties,
        contractId,
        templateId,
        sourceDomain,
        sourceProtocolVersion,
        sourceMediator,
        targetDomain,
        targetProtocolVersion,
        timeProof,
        transferCounter,
      )

      (transferOutRequest, adminPartiesAndRecipients.participants)
    }

  final case class PendingDataAndResponseArgs(
      txOutRequest: FullTransferOutTree,
      recipients: Recipients,
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      sourceSnapshot: DomainSnapshotSyncCryptoApi,
  )
}
