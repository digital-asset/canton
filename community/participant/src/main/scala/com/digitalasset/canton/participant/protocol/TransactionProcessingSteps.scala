// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.functorFilter._
import cats.syntax.option._
import cats.syntax.traverse._
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.participant.state.v2._
import com.daml.lf.data.ImmArray
import com.digitalasset.canton.crypto.{
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  HashOps,
  HkdfInfo,
  ProtocolCryptoApi,
  SecureRandomness,
}
import com.digitalasset.canton.data.ViewPosition.ListIndex
import com.digitalasset.canton.data._
import com.digitalasset.canton.error.TransactionError
import com.daml.error.ErrorCode
import com.daml.nonempty.NonEmptyUtil
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
  PendingRequestDataOrReplayData,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps._
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  DomainWithoutMediatorError,
  SequencerRequest,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor._
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.{
  ConfirmationRequestCreationError,
  ContractKeyConsistencyError,
  ContractKeyDuplicateError,
  MalformedLfTransaction,
  MalformedSubmitter,
  TransactionTreeFactoryError,
}
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
  UnknownDomain,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.UnknownPackageError
import com.digitalasset.canton.participant.protocol.submission._
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.protocol.validation._
import com.digitalasset.canton.participant.store.{ContractKeyJournal, _}
import com.digitalasset.canton.participant.sync.{
  CommandDeduplicationError,
  LedgerEvent,
  SyncServiceInjectionError,
  TimestampedEvent,
  TransactionRoutingError,
  UpstreamOffsetConvert,
}
import com.digitalasset.canton.participant.{LedgerSyncEvent, RequestCounter}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageDecryptionError
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, IterableUtil}
import com.digitalasset.canton.{
  DiscardOps,
  LedgerSubmissionId,
  LfPartyId,
  SequencerCounter,
  WorkflowId,
  checked,
}
import com.google.protobuf.ByteString
import io.grpc.Status

import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
import scala.collection.{concurrent, mutable}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** The transaction processor that coordinates the Canton transaction protocol.
  *
  * @param participantId    The participant id hosting the transaction processor.
  */
@nowarn("msg=dead code following this construct")
class TransactionProcessingSteps(
    domainId: DomainId,
    participantId: ParticipantId,
    confirmationRequestFactory: ConfirmationRequestFactory,
    confirmationResponseFactory: ConfirmationResponseFactory,
    modelConformanceChecker: ModelConformanceChecker,
    staticDomainParameters: StaticDomainParameters,
    crypto: DomainSyncCryptoClient,
    storedContractManager: StoredContractManager,
    metrics: TransactionProcessingMetrics,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      SubmissionParam,
      TransactionSubmitted,
      TransactionViewType,
      TransactionResultMessage,
      TransactionSubmissionError,
    ]
    with NamedLogging {

  override type SubmissionSendError = TransactionProcessor.SubmissionErrors.SequencerRequest.Error
  override type PendingRequestData = PendingTransaction
  override type PendingSubmissions = Unit
  override type PendingSubmissionId = Unit
  override type PendingSubmissionData = Nothing

  override type SubmissionResultArgs = Unit

  override type PendingDataAndResponseArgs = TransactionProcessingSteps.PendingDataAndResponseArgs

  override type RejectionArgs = TransactionProcessingSteps.RejectionArgs

  override type RequestError = TransactionProcessorError
  override type ResultError = TransactionProcessorError

  private val alarmer = new LoggingAlarmStreamer(logger)

  override def pendingSubmissions(state: SyncDomainEphemeralState): Unit = ()

  override def pendingRequestMap
      : SyncDomainEphemeralState => concurrent.Map[RequestId, PendingRequestDataOrReplayData[
        PendingRequestData
      ]] =
    _.pendingTransactions

  override def requestKind: String = "Transaction"

  override def submissionDescription(param: SubmissionParam): String =
    s"Submitters ${param.submitterInfo.actAs.mkString(", ")}, command ${param.submitterInfo.commandId}"

  override def submissionIdOfPendingRequest(pendingData: PendingTransaction): Unit = ()

  override def removePendingSubmission(
      pendingSubmissions: Unit,
      pendingSubmissionId: Unit,
  ): Option[Nothing] = None

  override def prepareSubmission(
      param: SubmissionParam,
      mediatorId: MediatorId,
      ephemeralState: SyncDomainEphemeralStateLookup,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionSubmissionError, Submission] = {
    val SubmissionParam(submitterInfo, transactionMeta, wfTransaction) = param
    val tracked = new TrackedTransactionSubmission(
      submitterInfo,
      transactionMeta,
      wfTransaction,
      mediatorId,
      recentSnapshot,
      ephemeralState.contractLookup,
      ephemeralState.observedTimestampLookup,
    )
    EitherT.rightT[FutureUnlessShutdown, TransactionSubmissionError](tracked)
  }

  override def embedNoMediatorError(error: NoMediatorError): TransactionSubmissionError =
    DomainWithoutMediatorError.Error(error.topologySnapshotTimestamp, domainId)

  private class TrackedTransactionSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      mediatorId: MediatorId,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
      contractLookup: ContractLookup,
      watermarkLookup: WatermarkLookup[CantonTimestamp],
  )(implicit traceContext: TraceContext)
      extends TrackedSubmission {

    private def changeId: ChangeId = submitterInfo.changeId

    override val changeIdHash: ChangeIdHash = ChangeIdHash(changeId)

    override def specifiedDeduplicationPeriod: DeduplicationPeriod =
      submitterInfo.deduplicationPeriod

    override def commandDeduplicationFailure(
        failure: DeduplicationFailed
    ): UnsequencedSubmission = {
      // If the deduplication period is not supported, we report the empty deduplication period to be on the safe side
      // Ideally, we'd report the offset that is being assigned to the completion event,
      // but that is not supported in our current architecture as the MultiDomainEventLog assigns the global offset
      // only after the event has been inserted to the ParticipantEventLog.
      lazy val emptyDeduplicationPeriod =
        DeduplicationPeriod.DeduplicationDuration(java.time.Duration.ZERO)

      val (error, dedupInfo): (TransactionError, DeduplicationPeriod) = failure match {
        case CommandDeduplicator.AlreadyExists(completionOffset, accepted, submissionId) =>
          CommandDeduplicationError.DuplicateCommandReject(
            changeId,
            UpstreamOffsetConvert.fromGlobalOffset(completionOffset).toHexString,
            accepted,
            submissionId,
          ) ->
            DeduplicationPeriod.DeduplicationOffset(
              // Extend the reported deduplication period to include the conflicting submission,
              // as deduplication offsets are exclusive
              UpstreamOffsetConvert.fromGlobalOffset(completionOffset - 1L)
            )
        case CommandDeduplicator.DeduplicationPeriodTooEarly(requested, supported) =>
          val error: TransactionError = supported match {
            case DeduplicationPeriod.DeduplicationDuration(longestDuration) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithDuration(
                changeId,
                requested,
                longestDuration.toString,
              )
            case DeduplicationPeriod.DeduplicationOffset(earliestOffset) =>
              CommandDeduplicationError.DeduplicationPeriodStartsTooEarlyErrorWithOffset(
                changeId,
                requested,
                earliestOffset.toHexString,
              )
          }
          error -> emptyDeduplicationPeriod
        case CommandDeduplicator.MalformedOffset(error) =>
          CommandDeduplicationError.MalformedDeduplicationOffset.Error(
            error
          ) -> emptyDeduplicationPeriod
      }
      val tracking = TransactionSubmissionTrackingData(
        submitterInfo.toCompletionInfo().copy(optDeduplicationPeriod = dedupInfo.some),
        TransactionSubmissionTrackingData.CauseWithTemplate(error),
      )
      UnsequencedSubmission(timestampForUpdate(), tracking)
    }

    override def submissionId: Option[LedgerSubmissionId] = submitterInfo.submissionId

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.liftF(
      recentSnapshot.ipsSnapshot.findDynamicDomainParametersOrDefault().map { domainParameters =>
        CantonTimestamp(transactionMeta.ledgerEffectiveTime)
          .add(domainParameters.ledgerTimeRecordTimeTolerance.unwrap)
      }
    )

    override def prepareBatch(
        actualDeduplicationOffset: DeduplicationPeriod.DeduplicationOffset
    ): EitherT[Future, UnsequencedSubmission, PreparedBatch] = {
      logger.debug("Preparing batch for transaction submission")
      val submitterInfoWithDedupPeriod =
        submitterInfo.copy(deduplicationPeriod = actualDeduplicationOffset)

      def causeWithTemplate(message: String, reason: ConfirmationRequestCreationError) =
        TransactionSubmissionTrackingData.CauseWithTemplate(
          SubmissionErrors.MalformedRequest.Error(message, reason)
        )

      val result = for {
        _ <-
          if (staticDomainParameters.uniqueContractKeys) {
            // Daml engine does not check in UCK mode whether there are contract key inconsistencies
            // coming from non-byKey nodes. This is safe for computing the resolved keys for the `ViewParticipantData`
            // because the resolved keys are constrained to byKey nodes.
            // Yet, there is no point in submitting the transaction in the first place to a UCK domain
            // because either some input contracts have already been archived or there is a duplicate contract key conflict.
            // Unfortunately, we cannot distinguish inactive inputs from duplicate contract keys at this point
            // and therefore return a generic contract key consistency error.
            //
            // TODO(M40) As this is merely an optimization, ensure that we test validation with transactions
            //  that fail this check.

            val result = wfTransaction.withoutVersion.contractKeyInputs match {
              case Left(LfTransaction.DuplicateKeys(key)) =>
                causeWithTemplate(
                  "Domain with unique contract keys semantics",
                  ContractKeyDuplicateError(key),
                ).asLeft

              case Left(LfTransaction.InconsistentKeys(key)) =>
                causeWithTemplate(
                  "Domain with unique contract keys semantics",
                  ContractKeyConsistencyError(key),
                ).asLeft

              case Right(_) => ().asRight
            }

            EitherT.fromEither[Future](result)
          } else EitherT.pure[Future, TransactionSubmissionTrackingData.RejectionCause](())
        confirmationPolicy <- EitherT(
          ConfirmationPolicy
            .choose(wfTransaction.unwrap, recentSnapshot.ipsSnapshot)
            .map(
              _.headOption
                .toRight(
                  causeWithTemplate(
                    "Incompatible Domain",
                    MalformedLfTransaction("No confirmation policy applicable"),
                  )
                )
            )
        )

        _submitters <- submitterInfo.actAs
          .traverse(rawSubmitter =>
            EitherT
              .fromEither[Future](LfPartyId.fromString(rawSubmitter))
              .leftMap[TransactionSubmissionTrackingData.RejectionCause](msg =>
                causeWithTemplate(msg, MalformedSubmitter(rawSubmitter))
              )
          )

        confirmationRequestTimer = metrics.protocolMessages.confirmationRequestCreation
        // Perform phase 1 of the protocol that produces a confirmation request
        request <- confirmationRequestTimer.timeEitherT(
          confirmationRequestFactory
            .createConfirmationRequest(
              wfTransaction,
              confirmationPolicy,
              submitterInfoWithDedupPeriod,
              CantonTimestamp(transactionMeta.ledgerEffectiveTime),
              transactionMeta.workflowId.map(WorkflowId(_)),
              mediatorId,
              recentSnapshot,
              TransactionTreeFactory.contractInstanceLookup(contractLookup),
              None,
              staticDomainParameters.protocolVersion,
            )
            .leftMap[TransactionSubmissionTrackingData.RejectionCause] {
              case TransactionTreeFactoryError(UnknownPackageError(unknownTo)) =>
                TransactionSubmissionTrackingData
                  .CauseWithTemplate(SubmissionErrors.PackageNotVettedByRecipients.Error(unknownTo))
              case creationError =>
                causeWithTemplate("Confirmation request creation failed", creationError)
            }
        )
      } yield {
        val batch = request.asBatch
        val batchSize =
          batch.toProtoVersioned(staticDomainParameters.protocolVersion).serializedSize
        metrics.protocolMessages.confirmationRequestSize.metric.update(batchSize)

        new PreparedTransactionBatch(
          batch,
          submitterInfoWithDedupPeriod.toCompletionInfo(),
          watermarkLookup,
        ): PreparedBatch
      }

      def mkError(
          rejectionCause: TransactionSubmissionTrackingData.RejectionCause
      ): Success[Either[UnsequencedSubmission, PreparedBatch]] = {
        val trackingData = TransactionSubmissionTrackingData(
          submitterInfoWithDedupPeriod.toCompletionInfo(),
          rejectionCause,
        )
        Success(Left(UnsequencedSubmission(timestampForUpdate(), trackingData)))
      }

      // Make sure that we don't throw an error
      EitherT(result.value.transform {
        case Success(Right(preparedBatch)) => Success(Right(preparedBatch))
        case Success(Left(rejectionCause)) => mkError(rejectionCause)
        case Failure(PassiveInstanceException(_reason)) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.PassiveReplica.Error(
              applicationId = submitterInfo.applicationId,
              commandId = submitterInfo.commandId,
            )
          )
          mkError(rejectionCause)
        case Failure(exception) =>
          val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(
            SyncServiceInjectionError.InjectionFailure.Failure(exception)
          )
          mkError(rejectionCause)
      })
    }

    override def submissionTimeoutTrackingData: SubmissionTrackingData =
      TransactionSubmissionTrackingData(
        submitterInfo.toCompletionInfo().copy(optDeduplicationPeriod = None),
        TransactionSubmissionTrackingData.TimeoutCause,
      )

    override def embedInFlightSubmissionTrackerError(
        error: InFlightSubmissionTracker.InFlightSubmissionTrackerError
    ): TransactionSubmissionError = error match {
      case SubmissionAlreadyInFlight(_newSubmission, existingSubmission) =>
        TransactionProcessor.SubmissionErrors.SubmissionAlreadyInFlight(
          changeId,
          existingSubmission.submissionId,
          existingSubmission.submissionDomain,
        )
      case UnknownDomain(domainId) =>
        TransactionRoutingError.ConfigurationErrors.SubmissionDomainNotReady.Error(domainId)
      case TimeoutTooLow(submission, lowerBound) =>
        TransactionProcessor.SubmissionErrors.TimeoutError.Error(lowerBound)
    }

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SubmissionSendError =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def shutdownDuringInFlightRegistration: TransactionSubmissionError =
      TransactionProcessor.SubmissionErrors.SubmissionDuringShutdown.Rejection()

    override def onFailure: TransactionSubmitted = TransactionSubmitted

    private def timestampForUpdate(): CantonTimestamp =
      // Assign the currently observed domain timestamp so that the error will be published soon
      watermarkLookup.highWatermark
  }

  private class PreparedTransactionBatch(
      override val batch: Batch[DefaultOpenEnvelope],
      completionInfo: CompletionInfo,
      watermarkLookup: WatermarkLookup[CantonTimestamp],
  ) extends PreparedBatch {
    override def pendingSubmissionId: Unit = ()

    override def embedSequencerRequestError(
        error: ProtocolProcessor.SequencerRequestError
    ): SequencerRequest.Error =
      TransactionProcessor.SubmissionErrors.SequencerRequest.Error(error.sendError)

    override def submissionErrorTrackingData(
        error: SubmissionSendError
    )(implicit traceContext: TraceContext): UnsequencedSubmission = {
      // Assign the currently observed domain timestamp so that the error will be published soon
      val timestamp = watermarkLookup.highWatermark
      val errorCode: TransactionError = error.sendError match {
        case SendAsyncClientError.RequestRefused(SendAsyncError.Overloaded(_)) =>
          TransactionProcessor.SubmissionErrors.DomainBackpressure.Rejection(error.toString)
        case otherSendError =>
          TransactionProcessor.SubmissionErrors.SequencerRequest.Error(otherSendError)
      }
      val rejectionCause = TransactionSubmissionTrackingData.CauseWithTemplate(errorCode)
      val trackingData = TransactionSubmissionTrackingData(completionInfo, rejectionCause)
      UnsequencedSubmission(timestamp, trackingData)
    }
  }

  override def updatePendingSubmissions(
      pendingSubmissionMap: Unit,
      submissionParam: SubmissionParam,
      pendingSubmissionId: PendingSubmissionId,
  ): EitherT[Future, SubmissionSendError, SubmissionResultArgs] =
    EitherT.pure(())

  override def createSubmissionResult(
      deliver: Deliver[Envelope[_]],
      unit: Unit,
  ): TransactionSubmitted =
    TransactionSubmitted

  // TODO(#8057) extract the decryption into a helper class that can be unit-tested.
  override def decryptViews(
      batch: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType]]]],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, DecryptedViews] =
    metrics.protocolMessages.transactionMessageReceipt.timeEitherT {
      // even if we encounter errors, we process the good views as normal
      // such that the validation is available if the confirmation request gets approved nevertheless.

      val pureCrypto = snapshot.pureCrypto

      def lightTransactionViewTreeDeserializer(
          bytes: ByteString
      ): Either[DeserializationError, LightTransactionViewTree] =
        LightTransactionViewTree
          .fromByteString(pureCrypto)(bytes)
          .leftMap(err => DeserializationError(err.message, bytes))

      type DecryptionError = EncryptedViewMessageDecryptionError[TransactionViewType]

      def decryptTree(
          vt: TransactionViewMessage,
          optRandomness: Option[SecureRandomness],
      ): EitherT[Future, DecryptionError, LightTransactionViewTree] =
        EncryptedViewMessage.decryptFor(
          snapshot,
          vt,
          participantId,
          staticDomainParameters.protocolVersion,
          optRandomness,
        )(
          lightTransactionViewTreeDeserializer
        )

      // To recover parallel processing to the largest possible extent, we'll associate a promise to each received
      // view. The promise gets fulfilled once the randomness for that view is computed - either directly by decryption,
      // because the participant is an informee of the view, or indirectly, because the participant is an informee on an
      // ancestor view and it has derived the view randomness using the HKDF.

      // TODO(M40): a malicious submitter can send a bogus view whose randomness cannot be decrypted/derived,
      //  crashing the SyncDomain
      val randomnessMap =
        batch.foldLeft(Map.empty[ViewHash, Promise[SecureRandomness]]) { case (m, evt) =>
          m + (evt.protocolMessage.viewHash -> Promise[SecureRandomness]())
        }

      def extractRandomnessFromView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Unit = {
        if (transactionViewEnvelope.recipients.leafMembers.contains(participantId)) {
          val message = transactionViewEnvelope.protocolMessage
          val randomnessF = EncryptedViewMessage
            .decryptRandomness(
              snapshot,
              message,
              participantId,
            )
            .valueOr { e =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  s"Can't decrypt the randomness of the view with hash ${message.viewHash} " +
                    s"where I'm allegedly an informee. $e"
                )
              )
            }
          checked(randomnessMap(transactionViewEnvelope.protocolMessage.viewHash))
            .completeWith(randomnessF)
            .discard[Promise[SecureRandomness]]
        }
      }

      def deriveRandomnessForSubviews(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      )(
          subviewHashAndIndex: (ViewHash, Int)
      ): Either[DecryptionError, Unit] = {
        val (subviewHash, index) = subviewHashAndIndex
        val info = HkdfInfo.subview(ListIndex(index))
        for {
          subviewRandomness <-
            ProtocolCryptoApi
              .hkdf(pureCrypto, staticDomainParameters.protocolVersion)(
                randomness,
                randomness.unwrap.size,
                info,
              )
              .leftMap(error =>
                EncryptedViewMessageDecryptionError.HkdfExpansionError(error, viewMessage)
              )
        } yield {
          randomnessMap.get(subviewHash) match {
            case Some(promise) =>
              promise.tryComplete(Success(subviewRandomness))
            case None =>
              // TODO(M40): raise an alarm and don't confirm the request
              logger.error(
                s"View ${viewMessage.viewHash} lists a subview with hash $subviewHash, but I haven't received any views for this hash"
              )
          }
          ()
        }
      }

      def decryptViewWithRandomness(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      ): EitherT[Future, DecryptionError, DecryptedView] =
        for {
          ltvt <- decryptTree(viewMessage, Some(randomness))
          _ <- EitherT.fromEither[Future](
            ltvt.subviewHashes.zipWithIndex.traverse(
              deriveRandomnessForSubviews(viewMessage, randomness)
            )
          )
        } yield ltvt

      def decryptView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Future[Either[DecryptionError, WithRecipients[DecryptedView]]] = {
        extractRandomnessFromView(transactionViewEnvelope)
        for {
          randomness <- randomnessMap(transactionViewEnvelope.protocolMessage.viewHash).future
          lightViewTreeE <- decryptViewWithRandomness(
            transactionViewEnvelope.protocolMessage,
            randomness,
          ).value
        } yield lightViewTreeE.map(WithRecipients(_, transactionViewEnvelope.recipients))
      }

      val result = for {
        decryptionResult <- batch.toNEF.traverse(decryptView)
      } yield DecryptedViews(decryptionResult)
      EitherT.right(result)
    }

  override def pendingDataAndResponseArgsForMalformedPayloads(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  ): Either[RequestError, PendingDataAndResponseArgs] =
    Right(PendingDataAndResponseArgs(None, ts, malformedPayloads, rc, sc, snapshot))

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CheckActivenessAndWritePendingContracts] = {

    val lightViewTrees = decryptedViews.map(_.unwrap)

    // The transaction ID is the root hash and all `decryptedViews` have the same root hash
    // so we can take any.
    val transactionId = lightViewTrees.head1.transactionId

    val policies = lightViewTrees.map(_.confirmationPolicy).toSet
    val workflowId =
      IterableUtil.assertAtMostOne(lightViewTrees.forgetNE.mapFilter(_.workflowId), "workflow")
    val submitterMeta =
      IterableUtil.assertAtMostOne(
        lightViewTrees.forgetNE.mapFilter(_.tree.submitterMetadata.unwrap.toOption),
        "submitterMetadata",
      )
    // TODO(M40): don't die on a malformed light transaction list. Moreover, pick out the views that are valid
    val rootViewTrees = LightTransactionViewTree
      .toToplevelFullViewTrees(lightViewTrees)
      .valueOr(e =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Invalid (root) sequence of lightweight transaction trees: $e"
          )
        )
      )

    // TODO(M40): check that all non-root lightweight trees can be decrypted with the expected (derived) randomness
    //   Also, check that all the view's informees received the derived randomness
    val fut = extractUsedAndCreatedContracts(rootViewTrees.toList, snapshot.ipsSnapshot).map {
      usedAndCreated =>
        val enrichedTransaction =
          EnrichedTransaction(policies, usedAndCreated, workflowId, submitterMeta)

        // TODO(M40): Check that the creations don't overlap with archivals
        val activenessSet = usedAndCreated.activenessSet

        val pendingContracts =
          usedAndCreated.created.values.map(WithTransactionId(_, transactionId)).toList

        CheckActivenessAndWritePendingContracts(
          activenessSet,
          pendingContracts,
          PendingDataAndResponseArgs(
            Some(enrichedTransaction),
            ts,
            malformedPayloads,
            rc,
            sc,
            snapshot,
          ),
        )
    }
    EitherT.right(fut)
  }

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      tracker: SingleDomainCausalTracker,
      activenessResultFuture: Future[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, StorePendingDataAndSendResponseAndCreateTimeout] = {
    import cats.Order._

    val PendingDataAndResponseArgs(
      enrichedTransactionO,
      requestTimestamp,
      malformedPayloads,
      rc,
      sc,
      snapshot,
    ) =
      pendingDataAndResponseArgs

    val ipsSnapshot = snapshot.ipsSnapshot

    def doParallelChecks(enrichedTransaction: EnrichedTransaction): Future[ParallelChecksResult] = {
      val ledgerTime = enrichedTransaction.ledgerTime
      for {
        _ <- pendingCursor

        rootViewTrees = enrichedTransaction.rootViewsWithUsedAndCreated.rootViewsWithContractKeys
          .map(_._1)
        rootViews = rootViewTrees.map(_.view)
        consistencyResult = ContractConsistencyChecker.assertInputContractsInPast(
          rootViews.toList,
          ledgerTime,
        )

        domainParameters <- ipsSnapshot.findDynamicDomainParametersOrDefault()

        // `tryCommonData` should never throw here because all views have the same root hash
        // which already commits to the ParticipantMetadata and CommonMetadata
        commonData = checked(tryCommonData(rootViewTrees))

        amSubmitter = enrichedTransaction.submitterMetadata.fold(false)(meta =>
          meta.submitterParticipant == participantId
        )
        timeValidation = TimeValidator.checkTimestamps(
          commonData,
          requestTimestamp,
          domainParameters.ledgerTimeRecordTimeTolerance,
          amSubmitter,
          logger,
        )

        conformanceResult <- modelConformanceChecker
          .check(
            enrichedTransaction.rootViewsWithUsedAndCreated.rootViewsWithContractKeys,
            pendingDataAndResponseArgs.rc,
            ipsSnapshot,
            commonData,
          )
          .value
      } yield ParallelChecksResult(consistencyResult, conformanceResult, timeValidation)

    }

    def awaitActivenessResult: Future[ActivenessResult] = activenessResultFuture.map {
      activenessResult =>
        val contractResult = activenessResult.contracts

        if (contractResult.notFree.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a confirmation request contains already non-free contracts ${contractResult.notFree}"
          )
        if (activenessResult.inactiveTransfers.nonEmpty)
          throw new RuntimeException(
            s"Activeness result for a confirmation request contains inactive transfers ${activenessResult.inactiveTransfers}"
          )
        activenessResult
    }

    def computeValidationResult(
        enrichedTransaction: EnrichedTransaction,
        parallelChecksResult: ParallelChecksResult,
        activenessResult: ActivenessResult,
    ): TransactionValidationResult = {
      val viewResults = SortedMap.newBuilder[ViewHash, ViewValidationResult]

      enrichedTransaction.rootViewsWithUsedAndCreated.rootViewsWithContractKeys.forgetNE
        .flatMap(_._1.flatten)
        .foreach { view =>
          val viewParticipantData = view.viewParticipantData
          val createdCore = viewParticipantData.createdCore.map(_.contract.contractId).toSet
          /* Since `viewParticipantData.coreInputs` contains all input contracts (archivals and usage only),
           * it suffices to check for `coreInputs` here.
           * We don't check for `viewParticipantData.archivedFromSubviews` in this view
           * because it suffices to check them in the subview where the contract is created.
           */
          val coreInputs = viewParticipantData.coreInputs.keySet

          // No need to check for created contracts being locked because then they'd be reported as existing.
          val contractResult = activenessResult.contracts
          val alreadyLocked = contractResult.alreadyLocked intersect coreInputs
          val existing = contractResult.notFresh.intersect(createdCore)
          val unknown = contractResult.unknown intersect coreInputs
          val notActive = contractResult.notActive.keySet intersect coreInputs
          val inactive = unknown ++ notActive

          val keyResult = activenessResult.keys
          // We don't look at keys modified by `viewParticipantData.archivedFromSubviews`
          // because it is enough to consider them in the subview where the contract is created.
          val keysOfCoreInputs =
            viewParticipantData.coreInputs.to(LazyList).mapFilter { case (_cid, inputContract) =>
              inputContract.contract.metadata.maybeKeyWithMaintainers
            }
          val freeResolvedKeysWithMaintainers =
            viewParticipantData.resolvedKeys.to(LazyList).mapFilter {
              case (key, FreeKey(maintainers)) => Some(LfGlobalKeyWithMaintainers(key, maintainers))
              case (key, AssignedKey(cid)) => None
            }
          val createdKeysWithMaintainers =
            viewParticipantData.createdCore
              .to(LazyList)
              .mapFilter(_.contract.metadata.maybeKeyWithMaintainers)

          def filterKeys(
              keysWithMaintainers: Seq[LfGlobalKeyWithMaintainers]
          )(pred: LfGlobalKey => Boolean): Map[LfGlobalKey, Set[LfPartyId]] =
            keysWithMaintainers.mapFilter { case LfGlobalKeyWithMaintainers(key, maintainers) =>
              if (pred(key)) (key -> maintainers).some else None
            }.toMap

          val duplicateKeys =
            filterKeys(createdKeysWithMaintainers)(keyResult.notFree.contains)
          val inconsistentKeys =
            filterKeys(freeResolvedKeysWithMaintainers)(keyResult.notFree.contains)
          val allKeys =
            keysOfCoreInputs ++ freeResolvedKeysWithMaintainers ++ createdKeysWithMaintainers
          val lockedKeys = filterKeys(allKeys)(keyResult.alreadyLocked.contains)

          val viewActivenessResult = ViewActivenessResult(
            inactiveContracts = alreadyLocked,
            alreadyLockedContracts = existing,
            existingContracts = inactive,
            duplicateKeys = duplicateKeys,
            inconsistentKeys = inconsistentKeys,
            lockedKeys = lockedKeys,
          )

          viewResults += (view.unwrap.viewHash -> ViewValidationResult(view, viewActivenessResult))
        }

      validation.TransactionValidationResult(
        transactionId = enrichedTransaction.transactionId,
        confirmationPolicies = enrichedTransaction.policies,
        submitterMetadata = enrichedTransaction.submitterMetadata,
        workflowId = enrichedTransaction.workflowId,
        contractConsistencyResult = parallelChecksResult.consistencyResult,
        modelConformanceResult = parallelChecksResult.conformanceResult,
        consumedInputsOfHostedParties =
          enrichedTransaction.rootViewsWithUsedAndCreated.consumedInputsOfHostedStakeholders,
        witnessedAndDivulged = enrichedTransaction.rootViewsWithUsedAndCreated.witnessedAndDivulged,
        createdContracts = enrichedTransaction.rootViewsWithUsedAndCreated.created,
        transient = enrichedTransaction.rootViewsWithUsedAndCreated.transient,
        keyUpdates =
          enrichedTransaction.rootViewsWithUsedAndCreated.uckUpdatedKeysOfHostedMaintainers,
        successfulActivenessCheck = activenessResult.isSuccessful,
        viewValidationResults = viewResults.result(),
        timeValidationResult = parallelChecksResult.timeValidationResult,
        hostedInformeeStakeholders =
          enrichedTransaction.rootViewsWithUsedAndCreated.hostedInformeeStakeholders,
      )
    }

    val requestId = RequestId(requestTimestamp)
    val result = enrichedTransactionO match {
      case None =>
        for {
          _activenessResult <- awaitActivenessResult
        } yield {
          // TODO(M40): Gracefully handle the case that the batch contains no parseable transaction view tree with the expected root hash
          throw new IllegalArgumentException(
            s"Cannot handle confirmation request with malformed payloads: $malformedPayloads"
          )
        }
      case Some(enrichedTransaction) =>
        for {
          parallelChecksResult <- doParallelChecks(enrichedTransaction)
          activenessResult <- awaitActivenessResult
          transactionValidationResult = computeValidationResult(
            enrichedTransaction,
            parallelChecksResult,
            activenessResult,
          )
          responses <- confirmationResponseFactory.createConfirmationResponses(
            requestId,
            malformedPayloads,
            transactionValidationResult,
            ipsSnapshot,
          )
        } yield {
          val mediatorRecipient = Recipients.cc(mediatorId)

          // TODO(M40): Handle malformed payloads
          ErrorUtil.requireArgument(
            malformedPayloads.isEmpty,
            s"Cannot handle confirmation request with malformed payloads: $malformedPayloads",
          )
          val pendingTransaction =
            createPendingTransaction(requestId, transactionValidationResult, rc, sc)
          StorePendingDataAndSendResponseAndCreateTimeout(
            pendingTransaction,
            responses.map(_ -> mediatorRecipient),
            Seq.empty,
            RejectionArgs(pendingTransaction, LocalReject.TimeRejects.LocalTimeout.Reject()),
          )
        }
    }
    EitherT.right(result)
  }

  override def eventAndSubmissionIdForInactiveMediator(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViews: NonEmpty[Seq[WithRecipients[DecryptedView]]],
  )(implicit
      traceContext: TraceContext
  ): (Option[TimestampedEvent], Option[PendingSubmissionId]) = {
    val someView = decryptedViews.head1
    val mediatorId = someView.unwrap.mediatorId
    val submitterMetadataO = someView.unwrap.tree.submitterMetadata.unwrap.toOption
    submitterMetadataO.flatMap(completionInfoFromSubmitterMetadata).map { completionInfo =>
      val rejection = LedgerSyncEvent.CommandRejected.FinalReason(
        TransactionProcessor.SubmissionErrors.InactiveMediatorError
          .Error(mediatorId, ts)
          .rpcStatus()
      )

      TimestampedEvent(
        LedgerSyncEvent.CommandRejected(ts.toLf, completionInfo, rejection),
        rc,
        Some(sc),
      )
    } -> None // Transaction processing doesn't use pending submissions
  }

  override def postProcessSubmissionForInactiveMediator(
      declaredMediator: MediatorId,
      ts: CantonTimestamp,
      pendingSubmission: Nothing,
  )(implicit
      traceContext: TraceContext
  ): Unit = ()

  override def createRejectionEvent(rejectionArgs: TransactionProcessingSteps.RejectionArgs)(
      implicit traceContext: TraceContext
  ): Either[TransactionProcessorError, Option[TimestampedEvent]] = {

    val RejectionArgs(pendingTransaction, rejectionReason) = rejectionArgs
    val PendingTransaction(
      _,
      _,
      _,
      requestTime,
      requestCounter,
      requestSequencerCounter,
      transactionValidationResult,
    ) =
      pendingTransaction
    val submitterMeta = transactionValidationResult.submitterMetadata
    val submitterParticipantSubmitterInfo =
      submitterMeta.flatMap(completionInfoFromSubmitterMetadata)

    rejectionReason.logWithContext(Map("requestId" -> pendingTransaction.requestId.toString))
    val rejection =
      rejectionReason match {
        // Turn canton-specific DuplicateKey and InconsistentKey errors into generic daml error code for duplicate/inconsistent keys for conformance.
        // TODO(#7866): As part of mediator privacy work, clean up this ugly error code remapping
        case duplicateKeyReject: LocalReject.ConsistencyRejections.DuplicateKey.Reject =>
          LedgerSyncEvent.CommandRejected.FinalReason(
            LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
              .Reject(duplicateKeyReject.cause)
              .rpcStatus()
          )
        case inconsistentKeyReject: LocalReject.ConsistencyRejections.InconsistentKey.Reject =>
          LedgerSyncEvent.CommandRejected.FinalReason(
            LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
              .Reject(inconsistentKeyReject.cause)
              .rpcStatus()
          )
        case reason => reason.createRejection
      }

    val tse = submitterParticipantSubmitterInfo.map(info =>
      TimestampedEvent(
        LedgerSyncEvent.CommandRejected(requestTime.toLf, info, rejection),
        requestCounter,
        Some(requestSequencerCounter),
      )
    )
    Right(tse)
  }

  private def completionInfoFromSubmitterMetadata(meta: SubmitterMetadata): Option[CompletionInfo] =
    if (meta.submitterParticipant == participantId) {
      Some(
        CompletionInfo(
          meta.actAs.toList,
          meta.applicationId.unwrap,
          meta.commandId.unwrap,
          Some(meta.dedupPeriod),
          meta.submissionId,
          statistics = None, // Statistics filled by ReadService, so we don't persist them
        )
      )
    } else None

  private[this] def createPendingTransaction(
      id: RequestId,
      transactionValidationResult: TransactionValidationResult,
      rc: RequestCounter,
      sc: SequencerCounter,
  )(implicit traceContext: TraceContext): PendingTransaction = {
    val TransactionValidationResult(
      transactionId,
      confirmationPolicies,
      submitterMeta,
      workflowId,
      contractConsistency,
      modelConformanceResult,
      consumedInputsOfHostedParties,
      witnessedAndDivulged,
      createdContracts,
      transient,
      keyUpdates,
      successfulActivenessCheck,
      viewValidationResults,
      timeValidation,
      hostedInformeeStakeholders,
    ) = transactionValidationResult

    ErrorUtil.requireArgument(
      contractConsistency.isRight,
      s"Cannot handle contract-inconsistent transaction $transactionId: $contractConsistency",
    )

    // TODO(Andreas): Do not discard the view validation results
    validation.PendingTransaction(
      transactionId,
      modelConformanceResult,
      workflowId,
      id.unwrap,
      rc,
      sc,
      transactionValidationResult,
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApproveConform(
      pendingRequestData: PendingRequestData,
      completionInfo: Option[CompletionInfo],
      modelConformance: ModelConformanceChecker.Result,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val commitSetF = Future {
      pendingRequestData.transactionValidationResult.commitSet(
        pendingRequestData.requestId,
        alarmer.alarm,
      )
    }
    val contractsToBeStored =
      pendingRequestData.transactionValidationResult.createdContracts.keySet

    val lfTx = modelConformance.suffixedTransaction

    def storeDivulgedContracts: EitherT[Future, TransactionProcessorError, Unit] =
      storedContractManager
        .storeDivulgedContracts(
          pendingRequestData.requestCounter,
          pendingRequestData.transactionValidationResult.witnessedAndDivulged.values.toSeq,
        )
        .leftMap(TransactionProcessor.FailedToStoreContract)

    // Store the divulged contracts in the contract store
    for {
      _ <- storeDivulgedContracts

      lfTxId <- EitherT
        .fromEither[Future](pendingRequestData.txId.asLedgerTransactionId)
        .leftMap[TransactionProcessorError](FieldConversionError("Transaction Id", _))

      acceptedEvent = LedgerSyncEvent.TransactionAccepted(
        optCompletionInfo = completionInfo,
        transactionMeta = TransactionMeta(
          ledgerEffectiveTime = lfTx.metadata.ledgerTime.toLf,
          workflowId = pendingRequestData.workflowId.map(_.unwrap),
          submissionTime = lfTx.metadata.submissionTime.toLf,
          // Set the submission seed to zeros one (None no longer accepted) because it is pointless for projected
          // transactions and it leaks the structure of the omitted parts of the transaction.
          submissionSeed = LedgerEvent.noOpSeed,
          optUsedPackages = None,
          optNodeSeeds = Some(lfTx.metadata.seeds.to(ImmArray)),
          optByKeyNodes = None, // optByKeyNodes is unused by the indexer
        ),
        transaction = LfCommittedTransaction(lfTx.unwrap),
        transactionId = lfTxId,
        recordTime = pendingRequestData.requestTime.toLf,
        divulgedContracts =
          pendingRequestData.transactionValidationResult.witnessedAndDivulged.map {
            case (divulgedCid, divulgedContract) =>
              DivulgedContract(divulgedCid, divulgedContract.contractInstance)
          }.toList,
        blindingInfo = None,
      )

      timestampedEvent = TimestampedEvent(
        acceptedEvent,
        pendingRequestData.requestCounter,
        Some(pendingRequestData.requestSequencerCounter),
      )
    } yield CommitAndStoreContractsAndPublishEvent(
      Some(commitSetF),
      contractsToBeStored,
      Some(timestampedEvent),
      Some(
        TransactionUpdate(
          pendingRequestData.transactionValidationResult.hostedInformeeStakeholders,
          pendingRequestData.requestTime,
          domainId,
          pendingRequestData.requestCounter,
        )
      ),
    )
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      event: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, TransactionResultMessage],
      pendingRequestData: PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      tracker: SingleDomainCausalTracker,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val Deliver(_, ts, _, _, _) = event.content
    val submitterMeta = pendingRequestData.transactionValidationResult.submitterMetadata
    val completionInfo = submitterMeta.flatMap(completionInfoFromSubmitterMetadata)

    def rejected(error: TransactionError) = {
      for {
        event <- EitherT.fromEither[Future](
          createRejectionEvent(RejectionArgs(pendingRequestData, error))
        )
      } yield CommitAndStoreContractsAndPublishEvent(None, Set(), event, None)
    }

    def getCommitSetAndContractsToBeStoredAndEvent()
        : EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
      import scala.util.Either.MergeableEither
      (
        MergeableEither[MediatorResult](result).merge.verdict,
        pendingRequestData.modelConformanceResult,
      ) match {
        case (Verdict.Approve, Right(modelConformance)) =>
          getCommitSetAndContractsToBeStoredAndEventApproveConform(
            pendingRequestData,
            completionInfo,
            modelConformance,
          )

        case (_, Left(modelConformanceError)) =>
          rejected(
            LocalReject.MalformedRejects.ModelConformance.Reject(modelConformanceError.toString)
          )

        case (reasons: Verdict.RejectReasons, _) =>
          // TODO(M40): Implement checks against malicious rejections and scrutinize the reasons such that an alarm is raised if necessary
          rejected(reasons.keyEvent)
        case (reject: Verdict.MediatorReject, _) =>
          rejected(reject)
        case (Verdict.Timeout, _) =>
          rejected(Verdict.MediatorReject.Timeout.Reject())
      }
    }

    for {
      domainParameters <- EitherT.right[TransactionProcessorError](
        crypto.ips
          .awaitSnapshot(pendingRequestData.requestTime)
          .flatMap(_.findDynamicDomainParametersOrDefault())
      )
      maxDecisionTime = domainParameters.decisionTimeFor(pendingRequestData.requestTime)
      _ <-
        if (ts <= maxDecisionTime) EitherT.pure[Future, TransactionProcessorError](())
        else
          EitherT.right[TransactionProcessorError](
            Future.failed(new IllegalArgumentException("Timeout message after decision time"))
          )
      res <- getCommitSetAndContractsToBeStoredAndEvent()
    } yield res
  }

  override def postProcessResult(verdict: Verdict, pendingSubmission: Nothing)(implicit
      traceContext: TraceContext
  ): Unit = ()

  private[this] def extractUsedAndCreatedContracts(
      rootViewTrees: Seq[TransactionViewTree],
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[UsedAndCreated] = {

    def idWithSerializable(created: CreatedContract): (LfContractId, SerializableContract) =
      created.contract.contractId -> created.contract

    def visit(rootView: TransactionView)(f: TransactionView => Unit): Unit = {
      def go(rootView: TransactionView): Unit = {
        f(rootView)
        rootView.subviews.foreach { wrappedSubview =>
          go(wrappedSubview.tryUnwrap)
        }
      }

      go(rootView)
    }

    // prefetch parties
    val prefetchParties = mutable.Set.empty[LfPartyId]
    rootViewTrees.foreach { rootViewTree =>
      visit(rootViewTree.view) { subview =>
        val viewParticipantData = subview.viewParticipantData.tryUnwrap
        prefetchParties ++= subview.viewCommonData.tryUnwrap.informees.map(_.party)
        viewParticipantData.resolvedKeys.foreach {
          case (_, FreeKey(maintainers)) => prefetchParties ++= maintainers
          case _ => ()
        }
        viewParticipantData.coreInputs.values.foreach { x =>
          prefetchParties ++= x.maintainers
          prefetchParties ++= x.stakeholders
        }
      }
    }
    val hostsPartyPrefetchF = prefetchParties.toList
      .traverse(partyId =>
        topologySnapshot.hostedOn(partyId, participantId).map {
          case Some(relationship) if relationship.permission.isActive => partyId -> true
          case _ => partyId -> false
        }
      )
      .map(_.toMap)

    hostsPartyPrefetchF.map { hostsPartyPrefetch =>
      def hostsParty(parties: Set[LfPartyId]): Boolean =
        parties.exists(party =>
          hostsPartyPrefetch
            .get(party)
            .fold {
              logger.error(s"Prefetch of parties is wrong and missed to load data for party $party")
              false
            }(x => x)
        )
      val divulgedInputsB = Map.newBuilder[LfContractId, SerializableContract]
      val createdContractsOfHostedStakeholdersB =
        Map.newBuilder[LfContractId, (Option[SerializableContract], Set[LfPartyId])]
      val contractsForActivenessCheckUnlessRelativeB = Map.newBuilder[LfContractId, Set[LfPartyId]]
      val witnessedB = Map.newBuilder[LfContractId, SerializableContract]
      val consumedInputsOfHostedStakeholdersB =
        Map.newBuilder[LfContractId, WithContractHash[Set[LfPartyId]]]
      val perRootViewInputKeysB =
        List.newBuilder[(TransactionViewTree, Map[LfGlobalKey, Option[LfContractId]])]
      val transientSameViewOrEarlier = mutable.Set.empty[LfContractId]
      val inputKeysOfHostedMaintainers = mutable.Map.empty[LfGlobalKey, ContractKeyJournal.Status]

      // Stores the change in the number of active contracts for a given key.
      // We process the nodes in the transaction out of execution order,
      // so during processing the numbers may lie outside of {-1,0,+1}.
      // At the end, however, by the assumption of internal key consistency of the submitted transaction
      // we should end up with 0 or -1 for assigned input keys and 0 or +1 for unassigned ones.
      // TODO(M40) This assumption holds only for honest submitters
      //
      // Keys in this map must be locked during phase 3 even if their status does not change (i.e., maps to 0)
      // because we cannot guarantee that the transaction is committed atomically (with dishonest submitter and confirmers).
      val keyUpdatesOfHostedMaintainers = mutable.Map.empty[LfGlobalKey, Int]

      def updateKeyCount(key: LfGlobalKey, delta: Int): Unit = {
        if (delta == 0) {
          keyUpdatesOfHostedMaintainers.getOrElseUpdate(key, 0).discard[Int]
        } else {
          val previous = keyUpdatesOfHostedMaintainers.getOrElse(key, 0)
          // We don't have to check for overflow here
          // because by the assumption on internal key consistency,
          // the overflows will cancel out in the end.
          keyUpdatesOfHostedMaintainers += key -> (previous + delta)
        }
      }

      def keyMustBeFree(key: LfGlobalKey): Unit = {
        val _ = inputKeysOfHostedMaintainers.getOrElseUpdate(key, ContractKeyJournal.Unassigned)
      }

      rootViewTrees.foreach { rootViewTree =>
        val resolvedKeysInView = mutable.Map.empty[LfGlobalKey, Option[LfContractId]]

        visit(rootViewTree.view) { subview =>
          val viewParticipantData = subview.viewParticipantData.tryUnwrap
          val informees = subview.viewCommonData.tryUnwrap.informees.map(_.party)

          viewParticipantData.resolvedKeys.foreach { case (key, resolved) =>
            val _ = resolvedKeysInView.getOrElseUpdate(key, resolved.resolution)
            resolved match {
              case FreeKey(maintainers) =>
                if (hostsParty(maintainers)) {
                  keyMustBeFree(key)
                }
              case AssignedKey(_) =>
              // AssignedKeys are part of the coreInputs and thus will be dealt with below.
            }
          }
          viewParticipantData.coreInputs.values.foreach { inputContractWithMetadata =>
            val contract = inputContractWithMetadata.contract
            val stakeholders = inputContractWithMetadata.contract.metadata.stakeholders

            val informeeStakeholders = stakeholders.intersect(informees)

            if (hostsParty(informeeStakeholders)) {
              val contractId = contract.contractId
              contractsForActivenessCheckUnlessRelativeB += (contractId -> informeeStakeholders)
              // We do not need to include in consumedInputsOfHostedStakeholders the contracts created in the core
              // because they are not inputs even if they are consumed.
              if (inputContractWithMetadata.consumed) {
                // Input contracts consumed under rollback node are not necessarily consumed in the transaction.
                if (!viewParticipantData.rollbackContext.inRollback) {
                  consumedInputsOfHostedStakeholdersB += contractId -> WithContractHash
                    .fromContract(contract, stakeholders)
                }

                // It suffices to do the check underneath these if conditions
                // because the maintainers are stakeholders and informees.
                if (hostsParty(inputContractWithMetadata.maintainers)) {
                  val key = inputContractWithMetadata.contractKey.getOrElse(
                    throw new RuntimeException(
                      "If there is no key, then there cannot be a hosted maintainer."
                    )
                  )
                  // In UCK mode (inputKeysOfHostedMaintainers only used in UCK mode), key must still be marked as
                  // assigned even if the contract was consumed under a rollback node. (In non-UCK mode the semantics
                  // are more nuanced per https://github.com/digital-asset/daml/pull/9546).
                  val _ =
                    inputKeysOfHostedMaintainers.getOrElseUpdate(key, ContractKeyJournal.Assigned)
                  // Contract key assignments below rollbacks do not change at the level of the transaction.
                  if (!viewParticipantData.rollbackContext.inRollback) {
                    updateKeyCount(
                      key,
                      delta = -1,
                    ) // But under rollback we would not update the key count
                  }
                }
              }
            } else if (hostsParty(stakeholders.diff(informees))) {
              // TODO(M40) report view participant data as malformed
              ErrorUtil.requireArgument(
                !inputContractWithMetadata.consumed,
                s"Participant hosts non-informee stakeholder(s) of consumed ${contract.contractId}; stakeholders: $stakeholders, informees: $informees",
              )
              // If the participant hosts a non-informee stakeholder of a used contract,
              // it shouldn't check activeness, so we don't add it to checkActivenessOrRelative
              // If another view adds the contract nevertheless to it, it will not matter since the participant
              // will not send a confirmation for this view.
            } else {
              divulgedInputsB += (contract.contractId -> contract)
            }
          }

          def isCreatedContractRolledBack(createdContract: CreatedContract): Boolean =
            viewParticipantData.rollbackContext.inRollback || createdContract.rolledBack

          // Since the informees of a Create node are the stakeholders of the created contract,
          // the participant either witnesses all creations in a view's core or hosts a party of all created contracts.
          import cats.implicits._
          if (hostsParty(informees)) {
            createdContractsOfHostedStakeholdersB ++= viewParticipantData.createdCore.map(
              createdContract =>
                idWithSerializable(createdContract).map(sc =>
                  // None out serialized contracts that are rolled back, so we don't actually create those
                  (
                    if (isCreatedContractRolledBack(createdContract)) None else Some(sc),
                    createdContract.contract.metadata.stakeholders,
                  )
                )
            )

            // Only track transient contracts outside of rollback scopes.
            if (!viewParticipantData.rollbackContext.inRollback) {
              val transientCore =
                viewParticipantData.createdCore
                  .filter(x => x.consumedInCore && !x.rolledBack)
                  .map(_.contract.contractId)
              transientSameViewOrEarlier ++= transientCore
              // The participant might host only an actor and not a stakeholder of the contract that is archived in the core.
              // We nevertheless add all of them here because we will intersect this set with `createdContractsOfHostedStakeholdersB` later.
              // This ensures that we only cover contracts of which the participant hosts a stakeholder.
              transientSameViewOrEarlier ++= viewParticipantData.archivedFromSubviews
            }

            // Update the key allocation count for created contracts.
            // Also deals with their archivals for transient contracts
            // if the archival happens in the current view's core or one of its parent view's cores.
            //
            // If the archival happens in a proper subview or a later subview of the current view,
            // then this view will list the contract among its core inputs and the archival will be dealt with then.
            viewParticipantData.createdCore.foreach { createdContract =>
              createdContract.contract.metadata.maybeKeyWithMaintainers.foreach {
                keyAndMaintainer =>
                  val LfGlobalKeyWithMaintainers(key, maintainers) = keyAndMaintainer
                  if (hostsParty(maintainers)) {
                    keyMustBeFree(key)

                    if (isCreatedContractRolledBack(createdContract)) {
                      // Created contracts under rollback nodes don't update the key count.
                      updateKeyCount(key, delta = 0)
                    } else if (
                      transientSameViewOrEarlier.contains(createdContract.contract.contractId)
                    ) {
                      // If the contract is archived by the core of the current view or a parent view,
                      // then it's transient and doesn't modify the allocation count.
                      //
                      // `transientSameViewOrEarlier` may contain contracts archived in earlier root views or
                      // from subviews of the current root view that precede the current subview.
                      // So it is a superset of the contracts we're looking for.
                      // However, this does not affect the condition here by the assumption of internal consistency,
                      // because these archivals from preceding non-parent views must refer to different contract IDs
                      // as the contract ID of the created node is fresh.
                      // TODO(M40) Internal consistency can be assumed only for an honest submitter
                      updateKeyCount(key, delta = 0)
                    } else {
                      updateKeyCount(key, delta = 1)
                    }
                  }
              }
            }
          } else if (!viewParticipantData.rollbackContext.inRollback) {
            // Contracts created, but rolled back are not witnessed.
            val _ = witnessedB ++= viewParticipantData.createdCore
              .filter { case CreatedContract(_, _, rolledBack) => !rolledBack }
              .map(idWithSerializable)
          }
        }
        perRootViewInputKeysB += rootViewTree -> resolvedKeysInView.toMap
      }

      val createdResultStakeholders = createdContractsOfHostedStakeholdersB.result()
      val maybeCreatedResult = createdResultStakeholders.fmap(tuple =>
        tuple._1
      ) // includes contracts created under rollback nodes
      val checkActivenessOrRelative = contractsForActivenessCheckUnlessRelativeB.result()

      // Remove the contracts created in the same transaction from the contracts to be checked for activeness
      val checkActivenessAndOrderFor = checkActivenessOrRelative -- maybeCreatedResult.keySet
      val checkActivenessTxInputs = checkActivenessAndOrderFor.keySet

      val consumedInputsOfHostedStakeholders = consumedInputsOfHostedStakeholdersB.result()

      val informeeStakeholdersCreatedContracts =
        createdResultStakeholders.values
          .flatMap((x: (Option[SerializableContract], Set[LfPartyId])) => x._2)
          .toSet

      val informeeStakeholdersUsedContracts = checkActivenessOrRelative.values.flatten.toSet

      //TODO(i6222): Consider tracking causal dependencies from contract keys
      val informeeStakeholders =
        informeeStakeholdersUsedContracts ++ informeeStakeholdersCreatedContracts

      // Among the consumed relative contracts, the activeness check on the participant cares only about those
      // for which the participant hosts a stakeholder, i.e., the participant must also see the creation.
      // If the contract is created in a view (including subviews) and archived in the core,
      // then it does not show up as a consumed input of another view, so we explicitly add those.
      val allConsumed = consumedInputsOfHostedStakeholders.keySet.union(transientSameViewOrEarlier)
      val transientResult =
        maybeCreatedResult.collect {
          case (cid, Some(contract)) if allConsumed.contains(cid) =>
            cid -> WithContractHash.fromContract(contract, contract.metadata.stakeholders)
        }

      // Only perform activeness checks for keys on domains with unique contract key semantics
      val (updatedKeys, freeKeys) = if (staticDomainParameters.uniqueContractKeys) {
        val updatedKeys = keyUpdatesOfHostedMaintainers.map { case (key, delta) =>
          import ContractKeyJournal._
          val newStatus = (checked(inputKeysOfHostedMaintainers(key)), delta) match {
            case (status, 0) => status
            case (Assigned, -1) => Unassigned
            case (Unassigned, 1) => Assigned
            case (status, _) =>
              throw new IllegalArgumentException(
                s"Request changes allocation count of $status key $key by $delta."
              )
          }
          key -> newStatus
        }.toMap
        val freeKeys = inputKeysOfHostedMaintainers.collect {
          case (key, ContractKeyJournal.Unassigned) => key
        }.toSet
        (updatedKeys, freeKeys)
      } else (Map.empty[LfGlobalKey, ContractKeyJournal.Status], Set.empty[LfGlobalKey])

      val usedAndCreated = UsedAndCreated(
        witnessedAndDivulged = divulgedInputsB.result() ++ witnessedB.result(),
        checkActivenessTxInputs = checkActivenessTxInputs,
        consumedInputsOfHostedStakeholders =
          consumedInputsOfHostedStakeholders -- maybeCreatedResult.keySet,
        maybeCreated = maybeCreatedResult,
        transient = transientResult,
        rootViewsWithContractKeys = NonEmptyUtil.fromUnsafe(perRootViewInputKeysB.result()),
        uckFreeKeysOfHostedMaintainers = freeKeys,
        uckUpdatedKeysOfHostedMaintainers = updatedKeys,
        hostedInformeeStakeholders = informeeStakeholders.filter(s => hostsParty(Set(s))),
      )
      usedAndCreated
    }
  }

  override def embedRequestError(
      err: ProtocolProcessor.RequestProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)

  override def embedResultError(
      err: ProtocolProcessor.ResultProcessingError
  ): TransactionProcessorError =
    GenericStepsError(err)
}

object TransactionProcessingSteps {

  case class SubmissionParam(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: WellFormedTransaction[WithoutSuffixes],
  )

  case class EnrichedTransaction(
      policies: NonEmpty[Set[ConfirmationPolicy]],
      rootViewsWithUsedAndCreated: UsedAndCreated,
      workflowId: Option[WorkflowId],
      submitterMetadata: Option[SubmitterMetadata],
  ) {

    def transactionId: TransactionId =
      rootViewsWithUsedAndCreated.rootViewsWithContractKeys.head1._1.transactionId
    def ledgerTime: CantonTimestamp =
      rootViewsWithUsedAndCreated.rootViewsWithContractKeys.head1._1.ledgerTime
  }

  case class ParallelChecksResult(
      consistencyResult: Either[List[ReferenceToFutureContractError], Unit],
      conformanceResult: Either[ModelConformanceChecker.Error, ModelConformanceChecker.Result],
      timeValidationResult: Either[TimeCheckFailure, Unit],
  )

  case class PendingDataAndResponseArgs(
      enrichedTransaction: Option[EnrichedTransaction],
      requestTimestamp: CantonTimestamp,
      malformedPayloads: Seq[MalformedPayload],
      rc: RequestCounter,
      sc: SequencerCounter,
      snapshot: DomainSnapshotSyncCryptoApi,
  )

  case class RejectionArgs(pendingTransaction: PendingTransaction, error: TransactionError)

  /** @throws java.lang.IllegalArgumentException if `receivedViewTrees` contains views with different transaction root hashes
    */
  def tryCommonData(receivedViewTrees: NonEmpty[Seq[TransactionViewTree]]): CommonData = {
    val distinctCommonData = receivedViewTrees
      .map(v => CommonData(v.transactionId, v.ledgerTime, v.submissionTime, v.confirmationPolicy))
      .distinct
    if (distinctCommonData.lengthCompare(1) == 0) distinctCommonData.head1
    else
      throw new IllegalArgumentException(
        s"Found several different transaction IDs, LETs or confirmation policies: $distinctCommonData"
      )
  }

  case class CommonData(
      transactionId: TransactionId,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      confirmationPolicy: ConfirmationPolicy,
  )

  private val RemappedCodesForConformance = Map[ErrorCode, Status.Code](
    LocalReject.ConsistencyRejections.DuplicateKey -> Status.Code.ABORTED
  )

}
