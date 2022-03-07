// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, NonEmptyList, OptionT}
import cats.syntax.alternative._
import com.daml.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.{LedgerSubmissionId, SequencerCounter}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashOps}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.protocol.ProcessingSteps.WrapsProcessorError
import com.digitalasset.canton.participant.protocol.ProtocolProcessor._
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessResult,
  ActivenessSet,
  CommitSet,
}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.{
  ChangeIdHash,
  SubmissionTrackingData,
  UnsequencedSubmission,
}
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError
import com.digitalasset.canton.participant.store.{
  ContractLookup,
  SyncDomainEphemeralState,
  SyncDomainEphemeralStateLookup,
  TransferLookup,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage.EncryptedViewMessageDecryptionError
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent
import scala.concurrent.Future

/** Interface for processing steps that are specific to request types.
  * The [[ProtocolProcessor]] wires up these steps with the necessary synchronization and state management,
  * including common processing steps.
  *
  * Every phase has one main entry method (Phase i, step 1), which produces data for the [[ProtocolProcessor]],
  * The phases also have methods to be called using the results from previous methods for each step.
  *
  * @tparam SubmissionParam  The bundled submission parameters
  * @tparam SubmissionResult The bundled submission results
  * @tparam RequestViewType  The type of view trees used by the request
  * @tparam Result           The specific type of the result message
  * @tparam SubmissionError  The type of errors that can occur during submission processing
  */
trait ProcessingSteps[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    Result <: SignedProtocolMessageContent,
    SubmissionError <: WrapsProcessorError,
] {

  /** The type of request messages */
  type RequestBatch = RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[RequestViewType]]]

  /** The submission errors that can occur during sending the batch to the sequencer and updating the pending submission map. */
  type SubmissionSendError

  /** A store of data on submissions that have been sent out, if any */
  type PendingSubmissions

  /** The data stored for submissions that have been sent out, if any */
  type PendingSubmissionData

  /** The type used for look-ups into the [[PendingSubmissions]] */
  type PendingSubmissionId

  /** The type of data stored after request processing to make it available for result processing */
  type PendingRequestData <: ProcessingSteps.PendingRequestData

  /** The type of data needed to generate the submission result in [[createSubmissionResult]].
    * The data is created by [[updatePendingSubmissions]].
    */
  type SubmissionResultArgs

  /** The type of decrypted view trees */
  type DecryptedView = RequestViewType#View

  /** The type of data needed to generate the pending data and response in [[constructPendingDataAndResponse]].
    * The data is created by [[decryptViews]]
    */
  type PendingDataAndResponseArgs

  /** The type of data needed to create a rejection event in [[createRejectionEvent]].
    * Created by [[constructPendingDataAndResponse]]
    */
  type RejectionArgs

  /** The type of errors that can occur during request processing */
  type RequestError <: WrapsProcessorError

  /** The type of errors that can occur during result processing */
  type ResultError <: WrapsProcessorError

  /** Wrap an error in request processing from the generic request processor */
  def embedRequestError(err: RequestProcessingError): RequestError

  /** Wrap an error in result processing from the generic request processor */
  def embedResultError(err: ResultProcessingError): ResultError

  /** Selector to get the [[PendingSubmissions]], if any */
  def pendingSubmissions(state: SyncDomainEphemeralState): PendingSubmissions

  /** Selector for the storage slot for [[PendingRequestData]] */
  def pendingRequestMap
      : SyncDomainEphemeralState => concurrent.Map[RequestId, PendingRequestDataOrReplayData[
        PendingRequestData
      ]]

  /** The kind of request, used for logging and error reporting */
  def requestKind: String

  /** Extract a description for a submission, used for logging and error reporting */
  def submissionDescription(param: SubmissionParam): String

  /** Extract the submission ID that corresponds to a pending request, if any */
  def submissionIdOfPendingRequest(pendingData: PendingRequestData): PendingSubmissionId

  // Phase 1: Submission

  /** Phase 1, step 1:
    *
    * @param param          The parameter object encapsulating the parameters of the submit method
    * @param mediatorId     The mediator ID to use for this submission
    * @param ephemeralState Read-only access to the [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState]]
    * @param recentSnapshot A recent snapshot of the topology state to be used for submission
    */
  def prepareSubmission(
      param: SubmissionParam,
      mediatorId: MediatorId,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SubmissionError, Submission]

  /** Convert [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.NoMediatorError]] into a submission error */
  def embedNoMediatorError(error: NoMediatorError): SubmissionError

  sealed trait Submission {

    /** Optional timestamp for the max sequencing time of the event.
      *
      * If possible, set it to the upper limit when the event could be successfully processed.
      * If [[scala.None]], then the sequencer client default will be used.
      */
    def maxSequencingTimeO: OptionT[Future, CantonTimestamp]
  }

  /** Submission to be sent off without tracking the in-flight submission and without deduplication. */
  trait UntrackedSubmission extends Submission {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    def pendingSubmissionId: PendingSubmissionId

    /** Wrap an error during submission from the generic request processor */
    def embedSubmissionError(err: SubmissionProcessingError): SubmissionSendError

    /** Convert a `SubmissionSendError` to a `SubmissionError` */
    def toSubmissionError(err: SubmissionSendError): SubmissionError
  }

  /** Submission to be tracked in-flight and with deduplication.
    *
    * The actual batch to be sent is computed only later by [[TrackedSubmission.prepareBatch]]
    * so that tracking information (e.g., the chosen deduplication period) can be incorporated
    * into the batch.
    */
  trait TrackedSubmission extends Submission {

    /** Change id hash to be used for deduplication of requests. */
    def changeIdHash: ChangeIdHash

    /** The submission ID of the submission, optional. */
    def submissionId: Option[LedgerSubmissionId]

    /** The deduplication period for the submission as specified in the [[com.daml.ledger.participant.state.v2.SubmitterInfo]]
      */
    def specifiedDeduplicationPeriod: DeduplicationPeriod

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted.
      * If the submission is not sequenced by the max sequencing time,
      * this data will be used to generate a timely rejection event via
      * [[com.digitalasset.canton.participant.protocol.submission.SubmissionTrackingData.rejectionEvent]].
      */
    def submissionTimeoutTrackingData: SubmissionTrackingData

    /** Convert a [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.InFlightSubmissionTrackerError]]
      * into a `SubmissionError` to be returned by the
      * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method.
      */
    def embedInFlightSubmissionTrackerError(error: InFlightSubmissionTrackerError): SubmissionError

    /** The submission tracking data to be used in case command deduplication failed */
    def commandDeduplicationFailure(failure: DeduplicationFailed): UnsequencedSubmission

    /** Phase 1, step 1a
      *
      * Prepare the batch of envelopes to be sent off
      * given the [[com.daml.ledger.api.DeduplicationPeriod.DeduplicationOffset]] chosen
      * by in-flight submission tracking and deduplication.
      *
      * Errors will be reported asynchronously by updating the
      * [[com.digitalasset.canton.participant.protocol.submission.UnsequencedSubmission]] for the [[changeIdHash]].
      *
      * Must not throw an exception.
      *
      * @param actualDeduplicationOffset The deduplication offset chosen by command deduplication
      */
    def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset
    ): EitherT[Future, UnsequencedSubmission, PreparedBatch]

    /** Produce a `SubmissionError` to be returned by the [[com.digitalasset.canton.participant.protocol.ProtocolProcessor.submit]] method
      * to indicate that a shutdown has happened during in-flight registration.
      * The resulting `SubmissionError` must convey neither that the submission is in-flight nor that it is not in-flight.
      */
    def shutdownDuringInFlightRegistration: SubmissionError

    /** The `SubmissionResult` to return if something went wrong after having registered the submission for tracking.
      * This result must not generate a completion event.
      *
      * Must not throw an exception.
      */
    def onFailure: SubmissionResult
  }

  /** The actual batch to be sent for a [[TrackedSubmission]] */
  trait PreparedBatch {

    /** The envelopes to be sent */
    def batch: Batch[DefaultOpenEnvelope]

    def pendingSubmissionId: PendingSubmissionId

    def embedSequencerRequestError(error: SequencerRequestError): SubmissionSendError

    /** The tracking data for the submission to be persisted upon a submission send error,
      * along with the timestamp at which it is supposed to be published.
      */
    def submissionErrorTrackingData(error: SubmissionSendError)(implicit
        traceContext: TraceContext
    ): UnsequencedSubmission
  }

  /** Phase 1, step 2:
    *
    * @param pendingSubmissions Stateful store to be updated with data on the pending submission
    * @param submissionParam Implementation-specific details on the submission, used for logging
    * @param pendingSubmissionId The key used for updates to the [[pendingSubmissions]]
    */
  def updatePendingSubmissions(
      pendingSubmissions: PendingSubmissions,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, SubmissionSendError, SubmissionResultArgs]

  /** Phase 1, step 3:
    */
  def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      submissionResultArgs: SubmissionResultArgs,
  ): SubmissionResult

  /** Phase 1, step 4; and Phase 7, step 1:
    *
    * Remove the pending submission from the pending submissions.
    * Called when sending the submission failed or did not lead to a result in time or a result has arrived for the request.
    */
  def removePendingSubmission(
      pendingSubmissions: PendingSubmissions,
      pendingSubmissionId: PendingSubmissionId,
  ): Option[PendingSubmissionData]

  // Phase 3: Request processing

  /** Phase 3, step 1:
    *
    * @param batch    The batch of messages in the request to be processed
    * @param snapshot Snapshot of the topology state at the request timestamp
    * @return The decrypted views and the errors encountered during decryption
    */
  def decryptViews(
      batch: NonEmptyList[OpenEnvelope[EncryptedViewMessage[RequestViewType]]],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, DecryptedViews]

  /** Phase 3:
    *
    * @param views The successfully decrypted views
    * @param decryptionErrors The decryption errors while trying to decrypt the views
    */
  case class DecryptedViews(
      views: Seq[WithRecipients[DecryptedView]],
      decryptionErrors: Seq[EncryptedViewMessageDecryptionError[RequestViewType]],
  )

  object DecryptedViews {
    def apply(
        all: Seq[Either[EncryptedViewMessageDecryptionError[RequestViewType], WithRecipients[
          DecryptedView
        ]]]
    ): DecryptedViews = {
      val (errors, views) = all.separate
      DecryptedViews(views, errors)
    }
  }

  /** Phase 3, step 2 (some good views):
    *
    * @param ts         The timestamp of the request
    * @param rc         The [[com.digitalasset.canton.participant.RequestCounter]] of the request
    * @param sc         The [[com.digitalasset.canton.SequencerCounter]] of the request
    * @param decryptedViews The decrypted views from step 1 with the right root hash
    * @param malformedPayloads The decryption errors and decrypted views with a wrong root hash
    * @param snapshot Snapshot of the topology state at the request timestamp
    * @return The activeness set and the pending contracts to add to the
    *         [[com.digitalasset.canton.participant.store.StoredContractManager]],
    *         and the arguments for step 2.
    */
  def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmptyList[WithRecipients[DecryptedView]],
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, CheckActivenessAndWritePendingContracts]

  /** Phase 3, step 2 (only malformed payloads):
    *
    * Called when no decrypted view has the right root hash.
    * @return [[scala.Left$]] aborts processing and
    *         [[scala.Right$]] continues processing with an empty activeness set and no pending contracts
    */
  def pendingDataAndResponseArgsForMalformedPayloads(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  ): Either[RequestError, PendingDataAndResponseArgs]

  /** Phase 3, step 2 (some good views, but the chosen mediator is inactive)
    *
    * @param ts         The timestamp of the request
    * @param rc         The [[com.digitalasset.canton.participant.RequestCounter]] of the request
    * @param sc         The [[com.digitalasset.canton.SequencerCounter]] of the request
    * @param decryptedViews The decrypted views from step 1 with the right root hash
    * @return The optional rejection event to be published in the event log,
    *         and the optional submission ID corresponding to this request
    */
  def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmptyList[WithRecipients[DecryptedView]],
  )(implicit traceContext: TraceContext): (Option[TimestampedEvent], Option[PendingSubmissionId])

  /** Phase 3, step 2 (submission where the chosen mediator is inactive)
    *
    * Called if the chosen mediator is inactive and [[eventAndSubmissionIdForInactiveMediator]]
    * returned a submission ID that was pending.
    *
    * @param pendingSubmission The [[PendingSubmissionData]] for the submission ID returned by
    *                          [[eventAndSubmissionIdForInactiveMediator]]
    * @see com.digitalasset.canton.participant.protocol.ProcessingSteps.postProcessResult
    */
  def postProcessSubmissionForInactiveMediator(
      declaredMediator: MediatorId,
      timestamp: CantonTimestamp,
      pendingSubmission: PendingSubmissionData,
  )(implicit
      traceContext: TraceContext
  ): Unit

  /** Phase 3
    *
    * @param activenessSet              The activeness set for the activeness check
    * @param pendingContracts           The pending contracts to be added to the [[com.digitalasset.canton.participant.store.StoredContractManager]],
    *                                   along with the [[com.digitalasset.canton.protocol.TransactionId]] that created the contract
    * @param pendingDataAndResponseArgs The implementation-specific arguments needed to create the pending data and response
    */
  case class CheckActivenessAndWritePendingContracts(
      activenessSet: ActivenessSet,
      pendingContracts: Seq[WithTransactionId[SerializableContract]],
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
  )

  /** Phase 3, step 3:
    *
    * @param pendingDataAndResponseArgs Implementation-specific data passed from [[decryptViews]]
    * @param transferLookup             Read-only interface of the [[com.digitalasset.canton.participant.store.memory.TransferCache]]
    * @param contractLookup             Read-only interface to the [[com.digitalasset.canton.participant.store.StoredContractManager]],
    *                                   which includes the pending contracts
    * @param activenessResultFuture     Future of the result of the activeness check<
    * @param pendingCursor              Future to complete when the [[com.digitalasset.canton.participant.protocol.RequestJournal]]'s
    *                                   cursor for the [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Pending]]
    *                                   reaching the current request. Block on this future to ensure that all earlier contract store
    *                                   writes are visible.
    * @param mediatorId                 The mediator that handles this request
    * @return Returns the [[PendingRequestData]] to be stored until Phase 7 and the responses to be sent to the mediator.
    */
  def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      causalityLookup: SingleDomainCausalTracker,
      activenessResultFuture: Future[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, RequestError, StorePendingDataAndSendResponseAndCreateTimeout]

  /** Phase 3:
    *
    * @param pendingData   The [[PendingRequestData]] to be stored until Phase 7
    * @param mediatorResponses     The responses to be sent to the mediator
    * @param rejectionArgs The implementation-specific arguments needed to create a rejection event on timeout
    */
  case class StorePendingDataAndSendResponseAndCreateTimeout(
      pendingData: PendingRequestData,
      mediatorResponses: Seq[(MediatorResponse, Recipients)],
      causalityMessages: Seq[(CausalityMessage, Recipients)],
      rejectionArgs: RejectionArgs,
  )

  /** Phase 3, step 4:
    *
    * @param rejectionArgs The implementation-specific information needed for the creation of the rejection event
    */
  def createRejectionEvent(rejectionArgs: RejectionArgs)(implicit
      traceContext: TraceContext
  ): Either[ResultError, Option[TimestampedEvent]]

  // Phase 7: Result processing

  /** Phase 7, step 2:
    *
    * @param event              The signed [[com.digitalasset.canton.sequencing.protocol.Deliver]] event containing the mediator result.
    *                           It is ensured that the `event` contains exactly one [[com.digitalasset.canton.protocol.messages.MediatorResult]]
    * @param result             The unpacked mediator result that is contained in the `event`
    * @param pendingRequestData The [[PendingRequestData]] produced in Phase 3
    * @param pendingSubmissions The data stored on submissions in the [[PendingSubmissions]]
    * @return The [[com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet]],
    *         the contracts from Phase 3 to be persisted to the contract store,
    *         and the event to be published
    */
  def getCommitSetAndContractsToBeStoredAndEvent(
      event: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, Result],
      pendingRequestData: PendingRequestData,
      pendingSubmissions: PendingSubmissions,
      tracker: SingleDomainCausalTracker,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ResultError, CommitAndStoreContractsAndPublishEvent]

  /** Phase 7, step 3:
    *
    * @param commitSet           [[scala.None$]] if the request should be rejected
    *                            [[scala.Some$]] a future that will produce the commit set for updating the active contract store
    * @param contractsToBeStored The contracts to be persisted to the contract store.
    *                            Must be a subset of the contracts produced in Phase 3, step 2 in [[CheckActivenessAndWritePendingContracts]].
    * @param maybeEvent          The event to be published via the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]]
    */
  case class CommitAndStoreContractsAndPublishEvent(
      commitSet: Option[Future[CommitSet]],
      contractsToBeStored: Set[LfContractId],
      maybeEvent: Option[TimestampedEvent],
      update: Option[CausalityUpdate],
  )

  /** Phase 7, step 4:
    *
    * Called after the request reached the state [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]]
    * in the request journal, if the participant is the submitter.
    * Also called if a timeout occurs with [[com.digitalasset.canton.protocol.messages.Verdict.Timeout]].
    *
    * @param verdict The verdict on the request
    */
  def postProcessResult(verdict: Verdict, pendingSubmission: PendingSubmissionData)(implicit
      traceContext: TraceContext
  ): Unit

}

object ProcessingSteps {

  trait WrapsProcessorError {
    def underlyingProcessorError(): Option[ProcessorError]
  }

  /** Generic parts that must be passed from Phase 3 to Phase 7. */
  trait PendingRequestData {
    def requestCounter: RequestCounter
    def requestSequencerCounter: SequencerCounter
    def pendingContracts: Set[LfContractId]
  }

  object PendingRequestData {
    def unapply(
        arg: PendingRequestData
    ): Some[(RequestCounter, SequencerCounter, Set[LfContractId])] = {
      Some((arg.requestCounter, arg.requestSequencerCounter, arg.pendingContracts))
    }
  }
}
