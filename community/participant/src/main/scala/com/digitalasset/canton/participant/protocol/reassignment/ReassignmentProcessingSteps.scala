// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution, RpcError}
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{
  Signature,
  SyncCryptoError,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.ViewType.ReassignmentViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullReassignmentViewTree,
  ReassignmentRef,
  ReassignmentSubmitterMetadata,
  ViewPosition,
}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.ReassignmentErrorGroup.SubmissionErrorGroup
import com.digitalasset.canton.error.{
  CantonBaseError,
  CantonError,
  ReassignmentErrorImpl,
  TransactionError,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.digitalasset.canton.ledger.participant.state.{
  ChangeId,
  CompletionInfo,
  SequencedEventUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  DecryptedViewData,
  DecryptedViews,
  ParsedRequest,
  PendingRequestData,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  ProcessorError,
  SequencerRequestError,
  ViewMessageError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  SequencerBackpressure,
  SequencerRequest,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory.EncryptedViewMessageCreationError
import com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.CauseWithTemplate
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  InFlightSubmissionTracker,
  ReassignmentSubmissionTrackingData,
  SubmissionTrackingData,
}
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ProtocolProcessor,
  TransactionProcessor,
}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend.TrafficRegistration
import com.digitalasset.canton.platform.apiserver.services.command.{
  SubmissionAccounting,
  TrafficEnforcementBackend,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.ModelConformance
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.InvalidContractIdInView
import com.digitalasset.canton.protocol.messages.Verdict.{
  Approve,
  MediatorReject,
  ParticipantReject,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.{ConfirmationRequestSessionKeyStore, SessionKeyStore}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ContractValidator, MonadUtil, ReassignmentTag}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LedgerSubmissionId,
  LfPackageName,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  checked,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.rpc.status.Status as RpcStatus

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Promise}

private[reassignment] trait ReassignmentProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ReassignmentViewType,
    PendingReassignmentType <: PendingReassignment,
] extends ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      ReassignmentProcessorError,
    ]
    with NamedLogging {

  val participantId: ParticipantId

  val psid: ReassignmentTag[PhysicalSynchronizerId]

  val protocolVersion: ReassignmentTag[ProtocolVersion]

  protected def contractValidator: ContractValidator

  protected implicit def ec: ExecutionContext

  // TODO(#37720): Narrow SubmissionSendError to GenericStepsError[SubmissionSendFailure] once
  //  the untracked submission is removed with protocol version 37
  override type SubmissionSendError = GenericStepsError[ProtocolProcessor.SubmissionProcessingError]

  override type PendingSubmissionId = RootHash

  override type PendingSubmissions = concurrent.Map[RootHash, PendingReassignmentSubmission]

  override type PendingSubmissionData = Some[PendingReassignmentSubmission]

  override type RequestError = ReassignmentProcessorError

  override type ResultError = ReassignmentProcessorError

  override type RejectionArgs = ReassignmentProcessingSteps.RejectionArgs[PendingReassignmentType]

  override type RequestType <: ProcessingSteps.RequestType.Reassignment
  override val requestType: RequestType

  override type FullView <: FullReassignmentViewTree

  override type ViewAbsoluteLedgerEffects = Unit
  override type FullViewAbsoluteLedgerEffects = Unit

  override type ParsedRequestType = ParsedReassignmentRequest[FullView]

  protected def reassignmentId(
      fullViewTree: FullView,
      requestTimestamp: CantonTimestamp,
  ): ReassignmentId

  override def embedNoMediatorError(error: NoMediatorError): ReassignmentProcessorError =
    GenericStepsError(error)

  override def removePendingSubmission(
      pendingSubmissions: concurrent.Map[RootHash, PendingReassignmentSubmission],
      pendingSubmissionId: RootHash,
  ): Option[Some[PendingReassignmentSubmission]] =
    pendingSubmissions.remove(pendingSubmissionId).map { pending =>
      pending.decisionTimeTickRequestTracker.cancel()
      Some(pending)
    }

  override def setDecisionTimeTickRequest(
      pendingSubmissionData: Some[PendingReassignmentSubmission],
      requestedTick: SynchronizerTimeTracker.TickRequest,
  ): Unit =
    pendingSubmissionData.value.decisionTimeTickRequestTracker.setRequest(requestedTick)

  override def postProcessSubmissionRejectedCommand(
      error: TransactionError,
      pendingSubmission: Some[PendingReassignmentSubmission],
  )(implicit traceContext: TraceContext): Unit =
    pendingSubmission.value.reassignmentCompletion.success(error.rpcStatus())

  override def postProcessResult(
      verdict: Verdict,
      pendingSubmission: Some[PendingReassignmentSubmission],
  )(implicit traceContext: TraceContext): Unit = {
    val status = verdict match {
      case _: Approve =>
        com.google.rpc.status.Status(com.google.rpc.Code.OK_VALUE)
      case reject: MediatorReject =>
        reject.reason
      case reasons: ParticipantReject =>
        reasons.keyErrorDetails.reason
    }
    pendingSubmission.value.reassignmentCompletion.success(status)
  }

  def localRejectFromActivenessCheck(
      requestId: RequestId,
      validationResult: ReassignmentValidationResult,
  ): Option[LocalRejectError]

  private def performPendingSubmissionMapUpdate(
      pendingSubmissionMap: concurrent.Map[RootHash, PendingReassignmentSubmission],
      reassignmentRef: ReassignmentRef,
      submitterLf: LfPartyId,
      rootHash: RootHash,
      pendingSubmission: PendingReassignmentSubmission,
  ): Either[ReassignmentProcessorError, Unit] = {
    val existing = pendingSubmissionMap.putIfAbsent(rootHash, pendingSubmission)
    Either.cond(
      existing.isEmpty,
      (),
      DuplicateReassignmentTreeHash(
        reassignmentRef,
        submitterLf,
        rootHash,
      ),
    )
  }

  /** The submission result when the submission fails definitely before it is sequenced */
  protected def submissionFailure(cause: Option[RpcStatus]): SubmissionResult

  /** The submission result when the submission may have been sent to the sequencer */
  protected def submissionUnknown(maxSequencingTime: CantonTimestamp): SubmissionResult

  protected def decryptTree(
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(
      envelope: OpenEnvelope[EncryptedViewMessage[RequestViewType]]
  )(implicit
      tc: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    EncryptedViewMessageError,
    (WithRecipients[DecryptedView], Option[Signature]),
  ]

  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      synchronizerLimits: SynchronizerLimits,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, DecryptedViews[DecryptedView]] = {
    val result = MonadUtil
      .parTraverseWithLimit(snapshot.pureCrypto.encryptionParallelism)(batch.toSeq) { envelope =>
        decryptTree(snapshot, sessionKeyStore)(envelope).value
      }
      .map(DecryptedViews.fromViewsWithSignature)
    EitherT.right(result)
  }

  override def absolutizeLedgerEffects(
      viewsWithCorrectRootHashAndRecipientsAndSignature: Seq[DecryptedViewData[DecryptedView]]
  ): (
      Seq[(DecryptedViewData[DecryptedView], ViewAbsoluteLedgerEffects)],
      Seq[MalformedPayload],
  ) =
    // Merely check that all reassigned contract IDs are absolute.
    viewsWithCorrectRootHashAndRecipientsAndSignature.partitionMap { decryptedView =>
      val view = decryptedView.view.unwrap
      val invalidContractIds =
        view.contracts.contracts.view.map(_.contract.contractId).filterNot(_.isAbsolute).toSeq
      Either.cond(
        invalidContractIds.nonEmpty,
        ViewMessageError(
          InvalidContractIdInView(
            s"Invalid contract IDs in view at position ${view.viewPosition}: $invalidContractIds"
          )
        ),
        (decryptedView, ()),
      )
    }

  override def computeFullViews(
      decryptedViewsWithSignatures: Seq[
        (DecryptedViewData[DecryptedView], ViewAbsoluteLedgerEffects)
      ]
  ): (
      Seq[(WithRecipients[FullView], Option[Signature], FullViewAbsoluteLedgerEffects)],
      Seq[ProtocolProcessor.MalformedPayload],
  ) =
    (
      decryptedViewsWithSignatures.map { case (decryptedView, effects) =>
        (decryptedView.view, decryptedView.signatureO, effects)
      },
      Seq.empty,
    )

  override def computeParsedRequest(
      rc: RequestCounter,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      rootViewsWithMetadata: NonEmpty[
        Seq[(WithRecipients[FullView], Option[Signature], FullViewAbsoluteLedgerEffects)]
      ],
      submitterMetadataO: Option[ViewSubmitterMetadata],
      isFreshOwnTimelyRequest: Boolean,
      malformedPayloads: Seq[MalformedPayload],
      mediator: MediatorGroupRecipient,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
      trafficCost: NonNegativeLong,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ParsedReassignmentRequest[FullView]] = {

    val numberOfViews = rootViewsWithMetadata.size
    if (numberOfViews > 1) {
      // The root hash check ensures that all views have the same contents.
      // The recipients check ensures that the first view has the right recipients.
      // Therefore, we can discard the remaining views.
      SyncServiceAlarm
        .Warn(
          s"Received $numberOfViews instead of 1 views in Request $ts. Discarding all but the first view."
        )
        .report()
    }

    val (WithRecipients(viewTree, recipients), signature, ()) = rootViewsWithMetadata.head1

    contractsMaybeUnknown(viewTree, snapshot).map(contractsMaybeUnknown =>
      ParsedReassignmentRequest(
        rc,
        ts,
        sc,
        viewTree,
        recipients,
        signature,
        submitterMetadataO,
        isFreshOwnTimelyRequest,
        contractsMaybeUnknown,
        malformedPayloads,
        mediator,
        snapshot,
        synchronizerParameters,
        reassignmentId(viewTree, ts),
        trafficCost,
      )
    )
  }

  protected def contractsMaybeUnknown(
      fullView: FullView,
      snapshot: SynchronizerSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      rootHash: RootHash,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit traceContext: TraceContext): Option[ConfirmationResponses] =
    ProcessingSteps.constructResponsesForMalformedPayloads(
      requestId = requestId,
      rootHash = rootHash,
      malformedPayloads = malformedPayloads,
      synchronizerId = psid.unwrap,
      participantId = participantId,
      protocolVersion = protocolVersion.unwrap,
    )

  override def eventAndSubmissionIdForRejectedCommand(
      ts: CantonTimestamp,
      sc: SequencerCounter,
      submitterMetadata: ViewSubmitterMetadata,
      rootHash: RootHash,
      freshOwnTimelyTx: Boolean,
      error: TransactionError,
      trafficCost: NonNegativeLong,
  )(implicit
      traceContext: TraceContext
  ): (Option[SequencedEventUpdate], Option[PendingSubmissionId]) = {
    val rejection = Update.CommandRejected.FinalReason(error.rpcStatus())

    lazy val completionInfo = CompletionInfo(
      actAs = List(submitterMetadata.submitter),
      userId = submitterMetadata.userId,
      commandId = submitterMetadata.commandId,
      optDeduplicationPeriod = None,
      submissionId = submitterMetadata.submissionId,
      paidTrafficCost = trafficCost,
    )
    val isOwnSubmission = shouldEmitCompletion(submitterMetadata, freshOwnTimelyTx)
    val updateO = Option.when(isOwnSubmission)(
      Update.SequencedCommandRejected(
        completionInfo = completionInfo,
        reasonTemplate = rejection,
        synchronizerId = psid.unwrap.logical,
        recordTime = ts,
        isTransaction = false,
        transactionHash = None,
        traceContext = traceContext,
      )
    )
    (updateO, Option.when(isOwnSubmission)(rootHash))
  }

  override def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[SequencedEventUpdate]] = {

    val RejectionArgs(pendingReassignment, errorDetails) = rejectionArgs

    val completionInfoO = Option.when(
      shouldEmitCompletion(
        pendingReassignment.submitterMetadata,
        pendingReassignment.freshOwnTimelyTx,
      )
    )(
      CompletionInfo(
        actAs = List(pendingReassignment.submitterMetadata.submitter),
        userId = pendingReassignment.submitterMetadata.userId,
        commandId = pendingReassignment.submitterMetadata.commandId,
        optDeduplicationPeriod = None,
        submissionId = pendingReassignment.submitterMetadata.submissionId,
        paidTrafficCost = pendingReassignment.trafficCost,
      )
    )
    errorDetails.logRejection(Map("requestId" -> pendingReassignment.requestId.toString))
    val rejection = Update.CommandRejected.FinalReason(errorDetails.reason)
    val updateO = completionInfoO.map(info =>
      Update.SequencedCommandRejected(
        completionInfo = info,
        reasonTemplate = rejection,
        synchronizerId = psid.unwrap.logical,
        recordTime = pendingReassignment.requestId.unwrap,
        isTransaction = false,
        transactionHash = None,
        traceContext = traceContext,
      )
    )
    Right(updateO)
  }

  /** Whether this participant emits the command completion of the request. No completion is emitted
    * for a replayed request.
    */
  protected def shouldEmitCompletion(
      submitterMetadata: ReassignmentSubmitterMetadata,
      freshOwnTimelyTx: Boolean,
  ): Boolean =
    submitterMetadata.submittingParticipant == participantId &&
      ReassignmentValidation
        .checkReplayedReassignment(
          participantId,
          submitterMetadata,
          protocolVersion.unwrap,
          isFreshOwnTimelyRequest = freshOwnTimelyTx,
        )
        .isEmpty

  override def getSubmitterInformation(views: Seq[DecryptedView]): Option[ViewSubmitterMetadata] =
    views.map(_.submitterMetadata).headOption

  /** Reassignments are tracked in flight from
    * [[com.digitalasset.canton.version.ProtocolVersion.v38]].
    */
  protected def trackedFromReplayProtection(
      submission: ReassignmentsSubmission
  ): Either[ReassignmentProcessorError, (Submission, PendingSubmissionData)] = {
    // TODO(#37720): Remove the untracked submission once protocol version 37 is removed
    val pendingSubmissionData = Some(submission.pendingSubmission)
    if (protocolVersion.unwrap >= ProtocolVersion.v38)
      Right(submission -> pendingSubmissionData)
    else
      submission.recordPendingSubmission.map(_ =>
        UntrackedReassignmentsSubmission(
          submission.batch,
          submission.rootHash,
          submission.approximateTimestampForSigning,
          submission.maxSequencingTime,
        ) -> pendingSubmissionData
      )
  }

  case class ReassignmentsSubmission(
      batch: Batch[DefaultOpenEnvelope],
      rootHash: RootHash,
      reassignmentRef: ReassignmentRef,
      submissionInfo: ReassignmentSubmissionInfo,
      pendingSubmissionMap: concurrent.Map[RootHash, PendingReassignmentSubmission],
      pendingSubmission: PendingReassignmentSubmission,
      override val changeIdHash: ChangeIdHash,
      override val approximateTimestampForSigning: CantonTimestamp,
      override val maxSequencingTime: CantonTimestamp,
  )(implicit traceContext: TraceContext)
      extends TrackedSubmissionWithoutDeduplication {

    override def submissionId: Option[LedgerSubmissionId] = submissionInfo.submissionId

    override def submissionTimeoutTrackingData: SubmissionTrackingData =
      trackingData(SubmissionTrackingData.TimeoutCause)

    override def embedInFlightSubmissionTrackerError(
        error: InFlightSubmissionTracker.InFlightSubmissionTrackerError
    ): ReassignmentProcessorError = error match {
      case InFlightSubmissionTracker.SubmissionAlreadyInFlight(
            _newSubmission,
            existingSubmission,
          ) =>
        ReassignmentSubmissionErrors.SubmissionAlreadyInFlight(
          existingSubmission.submissionId,
          existingSubmission.submissionSynchronizerId,
        )
      case InFlightSubmissionTracker.TimeoutTooLow(_submission, _lowerBound) =>
        ReassignmentSubmissionErrors.TimeoutError.Reject()
    }

    override def prepareBatch(
        approximateTimestampForSigning: CantonTimestamp,
        maxSequencingTime: CantonTimestamp,
        sessionKeyStore: SessionKeyStore,
    ): EitherT[FutureUnlessShutdown, SubmissionTrackingData, PreparedBatch] =
      EitherT
        .fromEither[FutureUnlessShutdown](recordPendingSubmission)
        .bimap(
          // The root hash is unique because the salt seed is sampled for each submission
          error => internalErrorTrackingData(new IllegalStateException(error.message)),
          _ =>
            new PreparedReassignmentBatch(
              batch,
              rootHash,
              completionInfo(paidTrafficCost = NonNegativeLong.zero),
            ): PreparedBatch,
        )

    override def shutdownDuringInFlightRegistration: ReassignmentProcessorError =
      ReassignmentSubmissionErrors.SubmissionDuringShutdown()

    override def onDefinitiveFailure(
        trackingData: Option[SubmissionTrackingData]
    ): SubmissionResult =
      submissionFailure(trackingData.collect {
        case ReassignmentSubmissionTrackingData(_, CauseWithTemplate(template), _) =>
          template.status
      })

    override def definiteFailureTrackingData(
        failure: UnlessShutdown[Throwable]
    ): SubmissionTrackingData = failure match {
      case UnlessShutdown.AbortedDueToShutdown =>
        trackingData(
          CauseWithTemplate(
            TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.Rejection()
          )
        )
      case UnlessShutdown.Outcome(exception) => internalErrorTrackingData(exception)
    }

    override def onPotentialFailure(maxSequencingTime: CantonTimestamp): SubmissionResult =
      submissionUnknown(maxSequencingTime)

    private[ReassignmentProcessingSteps] def recordPendingSubmission
        : Either[ReassignmentProcessorError, Unit] =
      performPendingSubmissionMapUpdate(
        pendingSubmissionMap,
        reassignmentRef,
        submissionInfo.submitter,
        rootHash,
        pendingSubmission,
      )

    private def completionInfo(paidTrafficCost: NonNegativeLong): CompletionInfo =
      CompletionInfo(
        actAs = List(submissionInfo.submitter),
        userId = submissionInfo.userId,
        commandId = submissionInfo.commandId,
        optDeduplicationPeriod = None,
        submissionId = submissionInfo.submissionId,
        paidTrafficCost = paidTrafficCost,
      )

    private def trackingData(
        rejectionCause: SubmissionTrackingData.RejectionCause
    ): SubmissionTrackingData =
      ReassignmentSubmissionTrackingData.create(
        completionInfo(paidTrafficCost = NonNegativeLong.zero),
        rejectionCause,
        psid.unwrap,
      )

    private def internalErrorTrackingData(throwable: Throwable): SubmissionTrackingData =
      trackingData(
        CauseWithTemplate(ReassignmentSubmissionErrors.SubmissionInternalError.Failure(throwable))
      )
  }

  case class UntrackedReassignmentsSubmission(
      override val batch: Batch[DefaultOpenEnvelope],
      override val pendingSubmissionId: PendingSubmissionId,
      override val approximateTimestampForSigning: CantonTimestamp,
      override val maxSequencingTime: CantonTimestamp,
  ) extends UntrackedSubmission {

    override def embedSubmissionError(
        err: ProtocolProcessor.SubmissionProcessingError
    ): SubmissionSendError =
      GenericStepsError(err)

    override def toSubmissionError(err: SubmissionSendError): ReassignmentProcessorError =
      err
  }

  private class PreparedReassignmentBatch(
      override val batch: Batch[DefaultOpenEnvelope],
      override val rootHash: RootHash,
      completionInfo: CompletionInfo,
  ) extends PreparedBatch {
    override def pendingSubmissionId: RootHash = rootHash

    override def submissionErrorTrackingData(
        error: ProtocolProcessor.SubmissionSendFailure
    )(implicit traceContext: TraceContext): SubmissionTrackingData = {
      val rejectionCause = error match {
        case SequencerRequestError(refused @ SendAsyncClientError.RequestRefused(refusal)) =>
          CauseWithTemplate(
            if (refusal.isOverload) SequencerBackpressure.Rejection(refusal.toString)
            else if (refusal.hasMaxSequencingTimeElapsed)
              ReassignmentSubmissionErrors.TimeoutError.Error()
            else SequencerRequest.Error(refused): CantonBaseError
          )
        case SequencerRequestError(otherSendError) =>
          CauseWithTemplate(SequencerRequest.Error(otherSendError))
        case ProtocolProcessor.TrafficEnforcementRejected(reason) =>
          // Traffic enforcement already decided what the client should see, this only renders it.
          CauseWithTemplate(RpcStatus.fromJavaProto(reason.asGrpcStatus))
      }
      ReassignmentSubmissionTrackingData.create(completionInfo, rejectionCause, psid.unwrap)
    }
  }

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): ReassignmentProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): ReassignmentProcessorError =
    GenericStepsError(err)

  protected def createConfirmationResponses(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
      protocolVersion: ProtocolVersion,
      validationResult: ReassignmentValidationResult,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ConfirmationResponses]] =
    if (malformedPayloads.nonEmpty) {
      FutureUnlessShutdown.pure(
        ProcessingSteps.constructResponsesForMalformedPayloads(
          requestId = requestId,
          rootHash = validationResult.rootHash,
          malformedPayloads = malformedPayloads,
          synchronizerId = psid.unwrap,
          participantId = participantId,
          protocolVersion = protocolVersion,
        )
      )
    } else {
      responsesForWellFormedPayloads(
        requestId,
        protocolVersion,
        validationResult,
      )
    }

  private def responsesForWellFormedPayloads(
      requestId: RequestId,
      protocolVersion: ProtocolVersion,
      validationResult: ReassignmentValidationResult,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[ConfirmationResponses]] =
    NonEmpty.from(validationResult.hostedConfirmingParties).traverse { hostedConfirmingParties =>
      if (!validationResult.isReassigningParticipant) {
        logger.debug(
          s"Sending an abstain verdict for reassignment ${validationResult.reassignmentId}: the participant is not reassigning"
        )
        FutureUnlessShutdown.pure(
          responseWithVerdict(
            requestId,
            validationResult,
            protocolVersion,
            LocalAbstainError.CannotPerformAllValidations
              .Abstain("The participant is not a reassigning participant")
              .toLocalAbstain(protocolVersion),
            hostedConfirmingParties.forgetNE,
          )
        )
      } else
        for {
          commonContractAuthenticationResult <-
            validationResult.commonValidationResult.contractAuthenticationResultF.value
          reassignmentContractAuthenticationResult <-
            validationResult.reassigningParticipantValidationResult.contractAuthenticationResultF.value
        } yield {
          val authenticationErrorO =
            validationResult.commonValidationResult.participantSignatureVerificationResult

          val authenticationRejection = authenticationErrorO.map(err =>
            LocalRejectError.MalformedRejects.MalformedRequest
              .Reject(err.message)
          )

          val modelConformanceRejection: Seq[ModelConformance.Reject] =
            Seq(
              commonContractAuthenticationResult.swap.toOption,
              validationResult.commonValidationResult.packageVettingResult,
              validationResult.commonValidationResult.contractSerializationResult,
            ).flatten
              .map(err => LocalRejectError.MalformedRejects.ModelConformance.Reject(err.toString))

          val submitterCheckRejection = validationResult.commonValidationResult.submitterCheckResult
            .map(err => LocalRejectError.ReassignmentRejects.ValidationFailed.Reject(err.message))

          val reassignmentIdRejection = validationResult.commonValidationResult.reassignmentIdResult
            .map(err =>
              LocalRejectError.ReassignmentRejects.InconsistentReassignmentId.Reject(err.message)
            )

          val reassigningErrors =
            validationResult.reassigningParticipantValidationResult.errors ++ reassignmentContractAuthenticationResult.swap.toOption

          val (abstainErrors, rejectingReassignmentErrors) =
            if (validationResult.reassigningParticipantValidationResult.isAbstain)
              (reassigningErrors, Seq.empty)
            else
              (Seq.empty, reassigningErrors)

          val failedValidationRejection =
            rejectingReassignmentErrors
              .map(err => LocalRejectError.ReassignmentRejects.ValidationFailed.Reject(err.message))

          val activenessRejection =
            localRejectFromActivenessCheck(requestId, validationResult)

          // As for transactions, a `Malformed` rejection ensures that this rejection or something at
          // least as strong reaches the mediator.
          val replayRejection = validationResult.commonValidationResult.replayCheckResult
            .map(err => LocalRejectError.MalformedRejects.MalformedRequest.Reject(err.message))

          val localRejections =
            (modelConformanceRejection ++ activenessRejection.toList ++
              authenticationRejection.toList ++ submitterCheckRejection ++ reassignmentIdRejection ++ failedValidationRejection ++ replayRejection.toList)
              .map { err =>
                err.logWithContext()
                err.toLocalReject(protocolVersion)
              }

          val reassigningParticipantAbstain =
            NonEmpty
              .from(abstainErrors)
              .map { errors =>
                val msg = errors.map(_.message).mkString(", ")
                logger.info(
                  s"Sending an abstain verdict for reassignment ${validationResult.reassignmentId} (parties: $hostedConfirmingParties): $msg"
                )
                LocalAbstainError.CannotPerformAllValidations
                  .Abstain(msg)
                  .toLocalAbstain(protocolVersion)
              }

          val (localVerdict, parties) = localRejections
            .collectFirst[(LocalVerdict, Set[LfPartyId])] {
              case malformed: LocalReject if malformed.isMalformed => malformed -> Set.empty
              case localReject: LocalReject =>
                localReject -> hostedConfirmingParties.forgetNE
            }
            .orElse(
              reassigningParticipantAbstain.map[(LocalVerdict, Set[LfPartyId])](
                _ -> hostedConfirmingParties.forgetNE
              )
            )
            .getOrElse(
              LocalApprove(protocolVersion) -> hostedConfirmingParties.forgetNE
            )

          responseWithVerdict(
            requestId,
            validationResult,
            protocolVersion,
            localVerdict,
            parties,
          )
        }
    }

  private def responseWithVerdict(
      requestId: RequestId,
      validationResult: ReassignmentValidationResult,
      protocolVersion: ProtocolVersion,
      localVerdict: LocalVerdict,
      parties: Set[LfPartyId],
  ): ConfirmationResponses =
    checked(
      ConfirmationResponses.tryCreate(
        requestId,
        validationResult.rootHash,
        psid.unwrap,
        participantId,
        NonEmpty.mk(
          Seq,
          ConfirmationResponse
            .tryCreate(
              Some(ViewPosition.root),
              localVerdict,
              parties,
            ),
        ),
        protocolVersion,
      )
    )

  /** During phase 7, the validations that should be checked are the validations that can be done on
    * all participants, whether reassigning or non-reassigning participants. These checks include:
    *   - Contract authentication check.
    *   - Target package vetting check.
    *   - Validation of the signature of the submitting participant.
    *   - Checks related to the submitter party:
    *     - Is the submitter a stakeholder?
    *     - Is the submitter hosted on the participant?
    *   - Is the reassignment id consistent with the reassignment data?
    *   - the multi-synchronizer topology feature flag should be set on all participants hosting a
    *     stakeholder.
    */
  def checkPhase7Validations(
      commonValidationResult: ReassignmentValidationResult.CommonValidationResult
  ): FutureUnlessShutdown[Option[LocalRejectError]] =
    commonValidationResult.contractAuthenticationResultF.value.map { contractAuthenticationResult =>
      val modelConformanceRejection = contractAuthenticationResult.swap.toOption
        .orElse(commonValidationResult.packageVettingResult)
        .orElse(commonValidationResult.contractSerializationResult)
        .map(err => LocalRejectError.MalformedRejects.ModelConformance.Reject(err.toString))

      val authenticationRejection =
        commonValidationResult.participantSignatureVerificationResult
          .map(err =>
            LocalRejectError.MalformedRejects.MalformedRequest
              .Reject(err.message)
          )

      val submitterCheckRejection =
        commonValidationResult.submitterCheckResult.map(err =>
          LocalRejectError.ReassignmentRejects.ValidationFailed.Reject(err.message)
        )

      val reassignmentIdResult =
        commonValidationResult.reassignmentIdResult.map(err =>
          LocalRejectError.ReassignmentRejects.InconsistentReassignmentId.Reject(err.message)
        )

      val multiSynchronizerIsNotEnabled =
        commonValidationResult.multiSynchronizerFeatureFlagCheckResult
          .map(err => LocalRejectError.ReassignmentRejects.ValidationFailed.Reject(err.message))

      modelConformanceRejection
        .orElse(authenticationRejection)
        .orElse(submitterCheckRejection)
        .orElse(reassignmentIdResult)
        .orElse(multiSynchronizerIsNotEnabled)
    }
}

object ReassignmentProcessingSteps {

  private[reassignment] def registerWithTrafficEnforcement(
      trafficEnforcementBackendO: Option[TrafficEnforcementBackend],
      submissionInfo: ReassignmentSubmissionInfo,
      accounting: SubmissionAccounting,
      trafficCost: Long,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, RpcError, Option[TrafficRegistration]] =
    TrafficEnforcementBackend.registerIfEnabled(
      trafficEnforcementBackendO,
      ChangeId(
        submissionInfo.userId,
        submissionInfo.commandId,
        // Not the routed transaction's actAs: TEA matches the reassignment completion by
        // change id, and its actAs is only the submitter.
        Set(submissionInfo.submitter),
      ),
      submissionInfo.submissionId,
      accounting,
      trafficCost,
    )(ErrorLoggingContext.fromTracedLogger(logger), ec)

  final case class PendingReassignmentSubmission(
      mkReassignmentId: CantonTimestamp => ReassignmentId,
      reassignmentCompletion: Promise[com.google.rpc.status.Status] =
        Promise[com.google.rpc.status.Status](),
      private[ReassignmentProcessingSteps] val decisionTimeTickRequestTracker: SynchronizerTimeTracker.TickRequestTracker =
        new SynchronizerTimeTracker.TickRequestTracker(),
  )

  final case class ParsedReassignmentRequest[VT <: FullReassignmentViewTree](
      override val rc: RequestCounter,
      override val requestTimestamp: CantonTimestamp,
      override val sc: SequencerCounter,
      fullViewTree: VT,
      recipients: Recipients,
      signatureO: Option[Signature],
      override val submitterMetadataO: Option[ReassignmentSubmitterMetadata],
      override val isFreshOwnTimelyRequest: Boolean,
      areContractsUnknown: Boolean,
      override val malformedPayloads: Seq[MalformedPayload],
      override val mediator: MediatorGroupRecipient,
      override val snapshot: SynchronizerSnapshotSyncCryptoApi,
      override val synchronizerParameters: DynamicSynchronizerParametersWithValidity,
      reassignmentId: ReassignmentId,
      override val trafficCost: NonNegativeLong,
  ) extends ParsedRequest[ReassignmentSubmitterMetadata] {
    override def rootHash: RootHash = fullViewTree.rootHash
  }

  trait PendingReassignment extends PendingRequestData with Product with Serializable {
    def requestId: RequestId

    def requestCounter: RequestCounter

    def requestSequencerCounter: SequencerCounter

    def submitterMetadata: ReassignmentSubmitterMetadata

    /** The resolved status from
      * [[com.digitalasset.canton.participant.protocol.SubmissionTracker.register]]
      */
    def freshOwnTimelyTx: Boolean

    /** Traffic cost of the (un)assignment event
      */
    def trafficCost: NonNegativeLong
  }

  final case class RejectionArgs[T <: PendingReassignment](
      pendingReassignment: T,
      errorDetails: ErrorDetails,
  )

  trait ReassignmentProcessorError
      extends WrapsProcessorError
      with Product
      with Serializable
      with PrettyPrintingFromCompanion {
    override def underlyingProcessorError(): Option[ProcessorError] = None

    override def prettyCompanion: PrettyPrintingCompanion[this.type] =
      ReassignmentProcessorError

    def message: String
  }

  object ReassignmentProcessorError extends PrettyPrintingCompanion[ReassignmentProcessorError] {
    override protected val pretty: Pretty[ReassignmentProcessorError] =
      adHocPrettyInstance
  }

  object ReassignmentSubmissionErrors extends SubmissionErrorGroup {

    @Explanation(
      """This error occurs when a reassignment is submitted on behalf of a party that is currently onboarding on the participant. Onboarding parties cannot initiate ledger changes until their onboarding is complete."""
    )
    @Resolution(
      """Wait for the party's onboarding to complete before submitting commands on their behalf."""
    )
    object PartyCurrentlyOnboarding
        extends ErrorCode(
          id = "REASSIGNMENT_PARTY_CURRENTLY_ONBOARDING",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Reject(party: LfPartyId)(implicit
          val loggingContext: com.digitalasset.canton.logging.ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"Submitting party $party is currently onboarding and cannot initiate reassignments."
          )
          with ReassignmentProcessorError {
        override def message: String = cause
      }
    }

    @Explanation(
      """This error occurs when the reassignment was not sequenced within the pre-defined max sequencing time
        |and has therefore timed out. The max sequencing time is derived from the submission time and the
        |sequencer client parameter default-max-sequencing-time-offset.
        |"""
    )
    @Resolution(
      """Resubmit if the delay is caused by high load.
        |Alternatively, the participant node operator can increase the sequencer client parameter default-max-sequencing-time-offset.
        |"""
    )
    object TimeoutError
        extends ErrorCode(
          id = "REASSIGNMENT_NOT_SEQUENCED_TIMEOUT",
          ErrorCategory.ContentionOnSharedResources,
        ) {
      final case class Error()
          extends ReassignmentErrorImpl(
            cause =
              "Reassignment was not sequenced within the pre-defined max sequencing time and has therefore timed out"
          )

      // TODO(#13240): Merge with Error() once reassignment submission errors keep their error code
      final case class Reject()(implicit
          val loggingContext: com.digitalasset.canton.logging.ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              "Reassignment was not sequenced within the pre-defined max sequencing time and has therefore timed out"
          )
          with ReassignmentProcessorError {
        override def message: String = cause
      }
    }

    @Explanation("An internal error occurred during reassignment submission.")
    @Resolution("Please contact support and provide the failure reason.")
    object SubmissionInternalError
        extends ErrorCode(
          id = "REASSIGNMENT_SUBMISSION_INTERNAL_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {
      final case class Failure(throwable: Throwable)(implicit
          val loggingContext: com.digitalasset.canton.logging.ErrorLoggingContext
      ) extends ReassignmentErrorImpl(
            cause = "internal error during reassignment submission",
            throwableO = Some(throwable),
          )
    }

    final case class SubmissionAlreadyInFlight(
        existingSubmissionId: Option[LedgerSubmissionId],
        existingSubmissionSynchronizerId: SynchronizerId,
    )(implicit val loggingContext: com.digitalasset.canton.logging.ErrorLoggingContext)
        extends CantonError.Impl(cause = "The reassignment submission is already in-flight")(
          ConsistencyErrors.SubmissionAlreadyInFlight.code
        )
        with ReassignmentProcessorError {
      override def message: String = cause
    }

    final case class SubmissionDuringShutdown()(implicit
        val loggingContext: com.digitalasset.canton.logging.ErrorLoggingContext
    ) extends CantonError.Impl(cause = "Reassignment submitted during shutdown or while passive.")(
          TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.code
        )
        with ReassignmentProcessorError {
      override def message: String = cause
    }
  }

  /** Used to convert ReassignmentValidationError to ReassignmentValidationError */
  final case class SubmissionValidationError(message: String) extends ReassignmentProcessorError

  final case class GenericStepsError[+E <: ProcessorError](error: E)
      extends ReassignmentProcessorError {
    override def underlyingProcessorError(): Option[E] = Some(error)

    override def message: String = error.toString
  }

  final case class InvalidReassignmentView(reason: String) extends ReassignmentProcessorError {
    override def message: String = s"Invalid reassignment common view: $reason"
  }

  final case class ContractError(message: String) extends ReassignmentProcessorError

  final case class NoCommonPackage(
      contractId: LfContractId,
      packageName: LfPackageName,
      participants: Set[ParticipantId],
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Contract $contractId requires package $packageName, which is not available on participants ${participants
          .mkString(", ")}"
  }

  final case class UnknownPhysicalSynchronizer(
      physicalSynchronizerId: PhysicalSynchronizerId,
      context: String,
  ) extends ReassignmentProcessorError
      with ReassignmentValidationError {
    override def message: String = s"Unknown synchronizer $physicalSynchronizerId when $context"

    override def prettyCompanion: PrettyPrintingCompanion[UnknownPhysicalSynchronizer] =
      UnknownPhysicalSynchronizer
  }

  object UnknownPhysicalSynchronizer extends PrettyPrintingCompanion[UnknownPhysicalSynchronizer] {
    override protected val pretty: Pretty[UnknownPhysicalSynchronizer] = prettyOfString(_.message)
  }

  final case class UnknownSynchronizer(
      synchronizerId: SynchronizerId,
      context: String,
  ) extends ReassignmentProcessorError {
    override def message: String = s"Unknown synchronizer $synchronizerId when $context"
  }

  case object ApplicationShutdown extends ReassignmentProcessorError {
    override def prettyCompanion: PrettyPrintingCompanion[ApplicationShutdown.this.type] =
      ApplicationShutdownPrettyPrintingCompanion
    override def message: String = "Application is shutting down"
  }

  private object ApplicationShutdownPrettyPrintingCompanion
      extends PrettyPrintingCompanion[ApplicationShutdown.type] {
    override protected val pretty: Pretty[ApplicationShutdown.type] =
      prettyOfObject[ApplicationShutdown.type]
  }

  final case class SynchronizerNotReady(synchronizerId: PhysicalSynchronizerId, context: String)
      extends ReassignmentProcessorError {
    override def message: String = s"Synchronizer $synchronizerId is not ready when $context"
  }

  final case class ReassignmentParametersError(
      synchronizerId: PhysicalSynchronizerId,
      context: String,
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Unable to compute reassignment parameters for $synchronizerId: $context"
  }

  final case class NoTimeProofFromSynchronizer(
      synchronizerId: PhysicalSynchronizerId,
      reason: String,
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Cannot fetch time proof for synchronizer `$synchronizerId`: $reason"
  }

  final case class ReassignmentSigningError(
      cause: SyncCryptoError
  ) extends ReassignmentProcessorError {
    override def message: String = show"Unable to sign reassignment request. $cause"
  }

  final case class NoStakeholders private (contractIds: Seq[LfContractId])
      extends ReassignmentProcessorError {
    override def message: String = s"Contracts $contractIds do not have any stakeholder"
  }

  object NoStakeholders {
    def logAndCreate(contracts: Seq[LfContractId], logger: TracedLogger)(implicit
        tc: TraceContext
    ): NoStakeholders = {
      logger.error(
        s"Attempting reassignment for contracts $contracts without stakeholders. All contracts should have stakeholders."
      )
      NoStakeholders(contracts)
    }
  }

  final case class EncryptionError(
      contractIds: Seq[LfContractId],
      error: EncryptedViewMessageCreationError,
  ) extends ReassignmentProcessorError {
    override def message: String = s"Cannot reassign contracts `$contractIds`: encryption error"
  }

  final case class DuplicateReassignmentTreeHash(
      reassignmentRef: ReassignmentRef,
      submitterLf: LfPartyId,
      hash: RootHash,
  ) extends ReassignmentProcessorError {
    override def message: String = s"For reassignment $reassignmentRef: duplicatehash"
  }

  final case class FieldConversionError(
      reassignmentId: ReassignmentId,
      field: String,
      error: String,
  ) extends ReassignmentProcessorError {
    override def message: String =
      s"Cannot reassign `$reassignmentId`: invalid conversion for `$field`"

    override def prettyCompanion: PrettyPrintingCompanion[FieldConversionError] =
      FieldConversionError
  }

  object FieldConversionError extends PrettyPrintingCompanion[FieldConversionError] {
    override protected val pretty: Pretty[FieldConversionError] = prettyOfClass(
      param("field", _.field.unquoted),
      param("error", _.error.unquoted),
    )
  }
}
