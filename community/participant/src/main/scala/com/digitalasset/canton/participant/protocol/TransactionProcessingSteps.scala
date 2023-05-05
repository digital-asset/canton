// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.error.definitions.LedgerApiErrors
import com.daml.lf.data.ImmArray
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.participant.metrics.TransactionProcessingMetrics
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.{
  MalformedPayload,
  NoMediatorError,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps.*
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  ContractAuthenticationFailed,
  DomainWithoutMediatorError,
  SequencerRequest,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.*
import com.digitalasset.canton.participant.protocol.conflictdetection.ActivenessResult
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory.*
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker.{
  SubmissionAlreadyInFlight,
  TimeoutTooLow,
  UnknownDomain,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  SerializableContractOfId,
  UnknownPackageError,
}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.protocol.validation.ContractConsistencyChecker.ReferenceToFutureContractError
import com.digitalasset.canton.participant.protocol.validation.TimeValidator.TimeCheckFailure
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.DbStorage.PassiveInstanceException
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, IterableUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  DiscardOps,
  LedgerSubmissionId,
  LfKeyResolver,
  LfPartyId,
  RequestCounter,
  SequencerCounter,
  WorkflowId,
  checked,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
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
    serializableContractAuthenticator: SerializableContractAuthenticator,
    authenticationValidator: AuthenticationValidator,
    authorizationValidator: AuthorizationValidator,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends ProcessingSteps[
      SubmissionParam,
      TransactionSubmitted,
      TransactionViewType,
      TransactionResultMessage,
      TransactionSubmissionError,
    ]
    with NamedLogging {
  private def protocolVersion = staticDomainParameters.protocolVersion

  override type SubmissionSendError = TransactionProcessor.SubmissionErrors.SequencerRequest.Error
  override type PendingSubmissions = Unit
  override type PendingSubmissionId = Unit
  override type PendingSubmissionData = Nothing

  override type SubmissionResultArgs = Unit

  override type PendingDataAndResponseArgs = TransactionProcessingSteps.PendingDataAndResponseArgs

  override type RejectionArgs = TransactionProcessingSteps.RejectionArgs

  override type RequestError = TransactionProcessorError
  override type ResultError = TransactionProcessorError

  override type RequestType = ProcessingSteps.RequestType.Transaction
  override val requestType = ProcessingSteps.RequestType.Transaction

  override def pendingSubmissions(state: SyncDomainEphemeralState): Unit = ()

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
    val SubmissionParam(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      disclosedContracts,
    ) = param

    val tracked = new TrackedTransactionSubmission(
      submitterInfo,
      transactionMeta,
      keyResolver,
      wfTransaction,
      mediatorId,
      recentSnapshot,
      ephemeralState.contractLookup,
      ephemeralState.observedTimestampLookup,
      disclosedContracts,
    )

    EitherT.rightT[FutureUnlessShutdown, TransactionSubmissionError](tracked)
  }

  override def embedNoMediatorError(error: NoMediatorError): TransactionSubmissionError =
    DomainWithoutMediatorError.Error(error.topologySnapshotTimestamp, domainId)

  override def decisionTimeFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransactionProcessorError, CantonTimestamp] =
    parameters.decisionTimeFor(requestTs).leftMap(DomainParametersError(parameters.domainId, _))

  override def participantResponseDeadlineFor(
      parameters: DynamicDomainParametersWithValidity,
      requestTs: CantonTimestamp,
  ): Either[TransactionProcessorError, CantonTimestamp] = parameters
    .participantResponseDeadlineFor(requestTs)
    .leftMap(DomainParametersError(parameters.domainId, _))

  private class TrackedTransactionSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      mediatorId: MediatorId,
      recentSnapshot: DomainSnapshotSyncCryptoApi,
      contractLookup: ContractLookup,
      watermarkLookup: WatermarkLookup[CantonTimestamp],
      disclosedContracts: Map[LfContractId, SerializableContract],
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
        protocolVersion,
      )
      UnsequencedSubmission(timestampForUpdate(), tracking)
    }

    override def submissionId: Option[LedgerSubmissionId] = submitterInfo.submissionId

    override def maxSequencingTimeO: OptionT[Future, CantonTimestamp] = OptionT.liftF(
      recentSnapshot.ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion).map {
        domainParameters =>
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
              case Left(Right(LfTransaction.DuplicateContractKey(key))) =>
                causeWithTemplate(
                  "Domain with unique contract keys semantics",
                  ContractKeyDuplicateError(key),
                ).asLeft

              case Left(Left(LfTransaction.InconsistentContractKey(key))) =>
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
                    MalformedLfTransaction(
                      s"No confirmation policy applicable (snapshot at ${recentSnapshot.ipsSnapshot.timestamp})"
                    ),
                  )
                )
            )
        )

        _submitters <- submitterInfo.actAs
          .parTraverse(rawSubmitter =>
            EitherT
              .fromEither[Future](LfPartyId.fromString(rawSubmitter))
              .leftMap[TransactionSubmissionTrackingData.RejectionCause](msg =>
                causeWithTemplate(msg, MalformedSubmitter(rawSubmitter))
              )
          )

        lookupContractsWithDisclosed: SerializableContractOfId =
          (contractId: LfContractId) =>
            disclosedContracts
              .get(contractId)
              .map(contract =>
                EitherT.rightT[Future, TransactionTreeFactory.ContractLookupError](
                  (contract.rawContractInstance, contract.ledgerCreateTime, contract.contractSalt)
                )
              )
              .getOrElse(
                TransactionTreeFactory
                  .contractInstanceLookup(contractLookup)(implicitly, implicitly)(
                    contractId
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
              transactionMeta.workflowId.map(WorkflowId(_)),
              keyResolver,
              mediatorId,
              recentSnapshot,
              lookupContractsWithDisclosed,
              None,
              protocolVersion,
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
        val batchSize = batch.toProtoVersioned.serializedSize
        metrics.protocolMessages.confirmationRequestSize.update(batchSize)(MetricsContext.Empty)

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
          protocolVersion,
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
        protocolVersion,
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
      case TimeoutTooLow(_submission, lowerBound) =>
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
      val trackingData =
        TransactionSubmissionTrackingData(completionInfo, rejectionCause, protocolVersion)
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
      ): Either[DefaultDeserializationError, LightTransactionViewTree] =
        LightTransactionViewTree
          .fromByteString(pureCrypto)(bytes)
          .leftMap(err => DefaultDeserializationError(err.message))

      type DecryptionError = EncryptedViewMessageDecryptionError[TransactionViewType]

      def decryptTree(
          vt: TransactionViewMessage,
          optRandomness: Option[SecureRandomness],
      ): EitherT[Future, DecryptionError, LightTransactionViewTree] =
        EncryptedViewMessage.decryptFor(
          snapshot,
          vt,
          participantId,
          protocolVersion,
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
          m + (evt.protocolMessage.viewHash -> new SupervisedPromise[SecureRandomness](
            "secure-randomness",
            futureSupervisor,
          ))
        }

      def extractRandomnessFromView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Unit = {
        if (
          // TODO(#12382): support group addressing for informees
          transactionViewEnvelope.recipients.leafRecipients.contains(MemberRecipient(participantId))
        ) {
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
          subviewHashAndIndex: (ViewHash, ViewPosition.MerklePathElement)
      ): Either[DecryptionError, Unit] = {
        val (subviewHash, index) = subviewHashAndIndex
        val info = HkdfInfo.subview(index)
        for {
          subviewRandomness <-
            pureCrypto
              .computeHkdf(
                randomness.unwrap,
                randomness.unwrap.size,
                info,
              )
              .leftMap(error =>
                EncryptedViewMessageDecryptionError.HkdfExpansionError(error, viewMessage)
              )
        } yield {
          randomnessMap.get(subviewHash) match {
            case Some(promise) =>
              promise.tryComplete(Success(subviewRandomness)).discard
            case None =>
              // TODO(M40): make sure to not approve the request
              SyncServiceAlarm
                .Warn(
                  s"View ${viewMessage.viewHash} lists a subview with hash $subviewHash, but I haven't received any views for this hash"
                )
                .report()
          }
          ()
        }
      }

      def decryptViewWithRandomness(
          viewMessage: TransactionViewMessage,
          randomness: SecureRandomness,
      ): EitherT[Future, DecryptionError, (DecryptedView, Option[Signature])] =
        for {
          ltvt <- decryptTree(viewMessage, Some(randomness))
          _ <- EitherT.fromEither[Future](
            ltvt.subviewHashes
              .zip(TransactionSubviews.indices(protocolVersion, ltvt.subviewHashes.length))
              .traverse(
                deriveRandomnessForSubviews(viewMessage, randomness)
              )
          )
        } yield (ltvt, viewMessage.submitterParticipantSignature)

      def decryptView(
          transactionViewEnvelope: OpenEnvelope[TransactionViewMessage]
      ): Future[Either[DecryptionError, (WithRecipients[DecryptedView], Option[Signature])]] = {
        extractRandomnessFromView(transactionViewEnvelope)
        for {
          randomness <- randomnessMap(transactionViewEnvelope.protocolMessage.viewHash).future
          lightViewTreeE <- decryptViewWithRandomness(
            transactionViewEnvelope.protocolMessage,
            randomness,
          ).value
        } yield lightViewTreeE.map { case (view, signature) =>
          (WithRecipients(view, transactionViewEnvelope.recipients), signature)
        }
      }

      val result = for {
        decryptionResult <- batch.toNEF.parTraverse(decryptView)
      } yield DecryptedViews(decryptionResult)
      EitherT.right(result)
    }

  override def computeActivenessSetAndPendingContracts(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      decryptedViewsWithSignatures: NonEmpty[
        Seq[(WithRecipients[DecryptedView], Option[Signature])]
      ],
      malformedPayloads: Seq[MalformedPayload],
      snapshot: DomainSnapshotSyncCryptoApi,
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CheckActivenessAndWritePendingContracts] = {

    val lightViewTrees = decryptedViewsWithSignatures.map { case (view, _) => view.unwrap }

    // The transaction ID is the root hash and all `decryptedViews` have the same root hash
    // so we can take any.
    val transactionId = lightViewTrees.head1.transactionId

    val policy = {
      val candidates = lightViewTrees.map(_.confirmationPolicy).toSet
      // The decryptedViews should all have the same root hash and therefore the same confirmation policy.
      // Bail out, if this is not the case.
      ErrorUtil.requireArgument(
        candidates.sizeCompare(1) == 0,
        s"Decrypted views have different confirmation policies. Bailing out... $candidates",
      )
      candidates.head1
    }
    val workflowIdO =
      IterableUtil.assertAtMostOne(lightViewTrees.forgetNE.mapFilter(_.workflowIdO), "workflow")
    val submitterMetaO =
      IterableUtil.assertAtMostOne(
        lightViewTrees.forgetNE.mapFilter(_.tree.submitterMetadata.unwrap.toOption),
        "submitterMetadata",
      )

    def tryFindViewSignature(viewTree: TransactionViewTree): Option[Signature] =
      decryptedViewsWithSignatures.find { case (view, _) =>
        view.unwrap.viewHash == viewTree.viewHash
      } match {
        case Some((_, signatureO)) => signatureO
        case None =>
          throw new RuntimeException(
            s"View tree ${viewTree.viewHash} must be present in the list of decrypted views."
          )
      }

    // TODO(M40): don't die on a malformed light transaction list. Moreover, pick out the views that are valid
    val rootViewTrees = LightTransactionViewTree
      .toToplevelFullViewTrees(protocolVersion, crypto.pureCrypto)(lightViewTrees)
      .valueOr(e =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Invalid (root) sequence of lightweight transaction trees: $e"
          )
        )
      )
      .map { viewTree => (viewTree, tryFindViewSignature(viewTree)) }

    // TODO(M40): check that all non-root lightweight trees can be decrypted with the expected (derived) randomness
    //   Also, check that all the view's informees received the derived randomness

    val usedAndCreatedF = ExtractUsedAndCreated(
      participantId,
      staticDomainParameters,
      rootViewTrees,
      snapshot.ipsSnapshot,
      loggerFactory,
    )

    val fut = usedAndCreatedF.map { usedAndCreated =>
      val enrichedTransaction =
        EnrichedTransaction(policy, usedAndCreated, workflowIdO, submitterMetaO)

      // TODO(M40): Check that the creations don't overlap with archivals
      val activenessSet = usedAndCreated.activenessSet

      val pendingContracts =
        usedAndCreated.contracts.created.values.map(WithTransactionId(_, transactionId)).toList

      CheckActivenessAndWritePendingContracts(
        activenessSet,
        pendingContracts,
        PendingDataAndResponseArgs(
          enrichedTransaction,
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

  def authenticateInputContracts(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    authenticateInputContractsInternal(
      pendingDataAndResponseArgs.enrichedTransaction.rootViewsWithUsedAndCreated.contracts.used
    )

  override def constructPendingDataAndResponse(
      pendingDataAndResponseArgs: PendingDataAndResponseArgs,
      transferLookup: TransferLookup,
      contractLookup: ContractLookup,
      activenessResultFuture: FutureUnlessShutdown[ActivenessResult],
      pendingCursor: Future[Unit],
      mediatorId: MediatorId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransactionProcessorError,
    StorePendingDataAndSendResponseAndCreateTimeout,
  ] = {
    import cats.Order.*

    val PendingDataAndResponseArgs(
      enrichedTransaction,
      requestTimestamp,
      malformedPayloads,
      rc,
      sc,
      snapshot,
    ) = pendingDataAndResponseArgs

    val ipsSnapshot = snapshot.ipsSnapshot
    val requestId = RequestId(requestTimestamp)

    def doParallelChecks(enrichedTransaction: EnrichedTransaction): Future[ParallelChecksResult] = {
      val ledgerTime = enrichedTransaction.ledgerTime
      for {
        _ <- pendingCursor

        rootViewsWithUsedAndCreated = enrichedTransaction.rootViewsWithUsedAndCreated
        rootViews = rootViewsWithUsedAndCreated.rootViewsWithSignatures.map { case (viewTree, _) =>
          viewTree
        }

        authenticationResult <-
          authenticationValidator.verifyViewSignatures(
            requestId,
            rootViewsWithUsedAndCreated.rootViewsWithSignatures,
            snapshot,
          )

        consistencyResultE = ContractConsistencyChecker
          .assertInputContractsInPast(
            rootViewsWithUsedAndCreated.contracts.used.toList,
            ledgerTime,
          )

        domainParameters <- ipsSnapshot.findDynamicDomainParametersOrDefault(protocolVersion)

        // `tryCommonData` should never throw here because all views have the same root hash
        // which already commits to the ParticipantMetadata and CommonMetadata
        commonData = checked(tryCommonData(rootViews))
        amSubmitter = enrichedTransaction.submitterMetadataO.exists(
          _.submitterParticipant == participantId
        )
        timeValidationE = TimeValidator.checkTimestamps(
          commonData,
          requestTimestamp,
          domainParameters.ledgerTimeRecordTimeTolerance,
          amSubmitter,
          logger,
        )

        authorizationResult <- authorizationValidator.checkAuthorization(
          requestId,
          rootViews,
          ipsSnapshot,
        )

        conformanceResultE <- modelConformanceChecker
          .check(
            rootViews,
            rootViewsWithUsedAndCreated.keys.keyResolverFor(_),
            pendingDataAndResponseArgs.rc,
            ipsSnapshot,
            commonData,
          )
          .value

      } yield ParallelChecksResult(
        authenticationResult,
        consistencyResultE,
        authorizationResult,
        conformanceResultE,
        timeValidationE,
      )
    }

    def awaitActivenessResult: FutureUnlessShutdown[ActivenessResult] = activenessResultFuture.map {
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

      enrichedTransaction.rootViewsWithUsedAndCreated.rootViewsWithSignatures
        .map { case (view, _) => view }
        .forgetNE
        .flatMap(_.tryFlattenToParticipantViews)
        .foreach { view =>
          val viewParticipantData = view.viewParticipantData
          val createdCore = viewParticipantData.createdCore.map(_.contract.contractId).toSet
          /* Since `viewParticipantData.coreInputs` contains all input contracts (archivals and usage only),
           * it suffices to check for `coreInputs` here.
           * We don't check for `viewParticipantData.createdInSubviewArchivedInCore` in this view
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

          // TODO(#9636) This logic does not make sense: the resolved keys of a view are relative to the contract/key state
          //  at the beginning of the view, but `activenessResult.keys` reports conflicts at the beginning of the transaction.
          val keyResult = activenessResult.keys
          // We don't look at keys modified by `viewParticipantData.createdInSubviewArchivedInCore`
          // because it is enough to consider them in the subview where the contract is created.
          val keysOfCoreInputs =
            viewParticipantData.coreInputs.to(LazyList).mapFilter { case (_cid, inputContract) =>
              inputContract.contract.metadata.maybeKeyWithMaintainers
            }
          val freeResolvedKeysWithMaintainers =
            viewParticipantData.resolvedKeys.to(LazyList).mapFilter {
              case (key, FreeKey(maintainers)) =>
                Some(LfGlobalKeyWithMaintainers(key, maintainers))
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
            inactiveContracts = inactive,
            alreadyLockedContracts = alreadyLocked,
            existingContracts = existing,
            duplicateKeys = duplicateKeys,
            inconsistentKeys = inconsistentKeys,
            lockedKeys = lockedKeys,
          )

          viewResults += (view.unwrap.viewHash -> ViewValidationResult(view, viewActivenessResult))
        }

      validation.TransactionValidationResult(
        transactionId = enrichedTransaction.transactionId,
        confirmationPolicy = enrichedTransaction.policy,
        submitterMetadataO = enrichedTransaction.submitterMetadataO,
        workflowIdO = enrichedTransaction.workflowIdO,
        contractConsistencyResultE = parallelChecksResult.consistencyResultE,
        authenticationResult = parallelChecksResult.authenticationResult,
        authorizationResult = parallelChecksResult.authorizationResult,
        modelConformanceResultE = parallelChecksResult.conformanceResultE,
        consumedInputsOfHostedParties =
          enrichedTransaction.rootViewsWithUsedAndCreated.contracts.consumedInputsOfHostedStakeholders,
        witnessedAndDivulged =
          enrichedTransaction.rootViewsWithUsedAndCreated.contracts.witnessedAndDivulged,
        createdContracts = enrichedTransaction.rootViewsWithUsedAndCreated.contracts.created,
        transient = enrichedTransaction.rootViewsWithUsedAndCreated.contracts.transient,
        keyUpdates =
          enrichedTransaction.rootViewsWithUsedAndCreated.keys.uckUpdatedKeysOfHostedMaintainers,
        successfulActivenessCheck = activenessResult.isSuccessful,
        viewValidationResults = viewResults.result(),
        timeValidationResultE = parallelChecksResult.timeValidationResultE,
        hostedInformeeStakeholders =
          enrichedTransaction.rootViewsWithUsedAndCreated.hostedInformeeStakeholders,
      )
    }

    val mediatorRecipient = Recipients.cc(mediatorId)

    val result =
      for {
        parallelChecksResult <- FutureUnlessShutdown.outcomeF(doParallelChecks(enrichedTransaction))
        activenessResult <- awaitActivenessResult
        _ = crashOnUnknownKeys(activenessResult)
        transactionValidationResult = computeValidationResult(
          enrichedTransaction,
          parallelChecksResult,
          activenessResult,
        )
        responses <- FutureUnlessShutdown.outcomeF(
          confirmationResponseFactory.createConfirmationResponses(
            requestId,
            malformedPayloads,
            transactionValidationResult,
            ipsSnapshot,
          )
        )
      } yield {

        val pendingTransaction =
          createPendingTransaction(requestId, transactionValidationResult, rc, sc, mediatorId)
        StorePendingDataAndSendResponseAndCreateTimeout(
          pendingTransaction,
          responses.map(_ -> mediatorRecipient),
          RejectionArgs(
            pendingTransaction,
            LocalReject.TimeRejects.LocalTimeout.Reject(protocolVersion),
          ),
        )
      }
    EitherT.right(result)
  }

  override def constructResponsesForMalformedPayloads(
      requestId: RequestId,
      malformedPayloads: Seq[MalformedPayload],
  )(implicit
      traceContext: TraceContext
  ): Seq[MediatorResponse] =
    confirmationResponseFactory.createConfirmationResponsesForMalformedPayloads(
      requestId,
      malformedPayloads,
      Seq.empty,
    )

  /** A key is reported as unknown if the transaction tries to reassign or unassign it,
    * but the key cannot be found in the [[com.digitalasset.canton.participant.store.ContractKeyJournal]].
    * That is normal if the exercised contract has already been archived and pruned,
    * so we expect to see a failed activeness check on the currently assigned contract.
    * If not, this indicates either a malicious submitter or an inconsistency between the
    * [[com.digitalasset.canton.participant.store.ContractKeyJournal]] and the
    * [[com.digitalasset.canton.participant.store.ActiveContractStore]].
    */
  // TODO(M40) Internal consistency checks should give the answer whether this is maliciousness or a corrupted store.
  private def crashOnUnknownKeys(
      result: ActivenessResult
  )(implicit traceContext: TraceContext): Unit = {
    if (result.contracts.isSuccessful && result.keys.unknown.nonEmpty)
      ErrorUtil.internalError(
        new IllegalStateException(
          show"Unknown keys are to be reassigned. Either the persisted ledger state corrupted or this is a malformed transaction. Unknown keys: ${result.keys.unknown}"
        )
      )
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
    submitterMetadataO.flatMap(completionInfoFromSubmitterMetadataO).map { completionInfo =>
      val rejection = LedgerSyncEvent.CommandRejected.FinalReason(
        TransactionProcessor.SubmissionErrors.InactiveMediatorError
          .Error(mediatorId, ts)
          .rpcStatus()
      )

      TimestampedEvent(
        LedgerSyncEvent.CommandRejected(
          ts.toLf,
          completionInfo,
          rejection,
          requestType,
        ),
        rc.asLocalOffset,
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
      _,
    ) =
      pendingTransaction
    val submitterMetaO = transactionValidationResult.submitterMetadataO
    val submitterParticipantSubmitterInfoO =
      submitterMetaO.flatMap(completionInfoFromSubmitterMetadataO)

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
        case reason => LedgerSyncEvent.CommandRejected.FinalReason(reason.rpcStatus())
      }

    val tseO = submitterParticipantSubmitterInfoO.map(info =>
      TimestampedEvent(
        LedgerSyncEvent.CommandRejected(requestTime.toLf, info, rejection, requestType),
        requestCounter.asLocalOffset,
        Some(requestSequencerCounter),
      )
    )
    Right(tseO)
  }

  @VisibleForTesting
  private[protocol] def authenticateInputContractsInternal(
      inputContracts: Map[LfContractId, SerializableContract]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, Unit] =
    if (protocolVersion < ProtocolVersion.v4)
      EitherT.rightT(())
    else
      EitherT.fromEither(
        inputContracts.toList
          .traverse_ { case (contractId, contract) =>
            serializableContractAuthenticator
              .authenticate(contract)
              .leftMap(message =>
                ContractAuthenticationFailed.Error(contractId, message).reported()
              )
          }
      )

  private def completionInfoFromSubmitterMetadataO(
      meta: SubmitterMetadata
  ): Option[CompletionInfo] =
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
      mediatorId: MediatorId,
  )(implicit traceContext: TraceContext): PendingTransaction = {
    val TransactionValidationResult(
      transactionId,
      confirmationPolicies,
      submitterMetaO,
      workflowIdO,
      contractConsistencyE,
      authenticationResult,
      authorizationResult,
      modelConformanceResultE,
      consumedInputsOfHostedParties,
      witnessedAndDivulged,
      createdContracts,
      transient,
      keyUpdates,
      successfulActivenessCheck,
      viewValidationResults,
      timeValidationE,
      hostedInformeeStakeholders,
    ) = transactionValidationResult

    ErrorUtil.requireArgument(
      contractConsistencyE.isRight,
      s"Cannot handle contract-inconsistent transaction $transactionId: $contractConsistencyE",
    )

    // TODO(M40): Do not discard the view validation results
    validation.PendingTransaction(
      transactionId,
      modelConformanceResultE,
      workflowIdO,
      id.unwrap,
      rc,
      sc,
      transactionValidationResult,
      mediatorId,
    )
  }

  private def getCommitSetAndContractsToBeStoredAndEventApproveConform(
      pendingRequestData: RequestType#PendingRequestData,
      completionInfo: Option[CompletionInfo],
      modelConformance: ModelConformanceChecker.Result,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val commitSetF = Future {
      pendingRequestData.transactionValidationResult.commitSet(pendingRequestData.requestId)
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

      contractMetadata =
        // TODO(#11047): Forward driver contract metadata also for divulged contracts
        pendingRequestData.transactionValidationResult.createdContracts.view.collect {
          case (contractId, SerializableContract(_, _, _, _, Some(salt))) =>
            contractId -> DriverContractMetadata(salt).toLfBytes(protocolVersion)
        }.toMap

      acceptedEvent = LedgerSyncEvent.TransactionAccepted(
        optCompletionInfo = completionInfo,
        transactionMeta = TransactionMeta(
          ledgerEffectiveTime = lfTx.metadata.ledgerTime.toLf,
          workflowId = pendingRequestData.workflowIdO.map(_.unwrap),
          submissionTime = lfTx.metadata.submissionTime.toLf,
          // Set the submission seed to zeros one (None no longer accepted) because it is pointless for projected
          // transactions and it leaks the structure of the omitted parts of the transaction.
          submissionSeed = LedgerSyncEvent.noOpSeed,
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
        contractMetadata = contractMetadata,
      )

      timestampedEvent = TimestampedEvent(
        acceptedEvent,
        pendingRequestData.requestCounter.asLocalOffset,
        Some(pendingRequestData.requestSequencerCounter),
      )
    } yield CommitAndStoreContractsAndPublishEvent(
      Some(commitSetF),
      contractsToBeStored,
      Some(timestampedEvent),
    )
  }

  override def getCommitSetAndContractsToBeStoredAndEvent(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      resultE: Either[MalformedMediatorRequestResult, TransactionResultMessage],
      pendingRequestData: RequestType#PendingRequestData,
      pendingSubmissionMap: PendingSubmissions,
      hashOps: HashOps,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
    val content = eventE.fold(_.content, _.content)
    val Deliver(_, ts, _, _, _) = content
    val submitterMetaO = pendingRequestData.transactionValidationResult.submitterMetadataO
    val completionInfoO = submitterMetaO.flatMap(completionInfoFromSubmitterMetadataO)

    def rejected(error: TransactionError) = {
      for {
        event <- EitherT.fromEither[Future](
          createRejectionEvent(RejectionArgs(pendingRequestData, error))
        )
      } yield CommitAndStoreContractsAndPublishEvent(None, Set(), event)
    }

    def getCommitSetAndContractsToBeStoredAndEvent()
        : EitherT[Future, TransactionProcessorError, CommitAndStoreContractsAndPublishEvent] = {
      import scala.util.Either.MergeableEither
      (
        MergeableEither[MediatorResult](resultE).merge.verdict,
        pendingRequestData.modelConformanceResultE,
      ) match {
        case (_: Verdict.Approve, Right(modelConformance)) =>
          getCommitSetAndContractsToBeStoredAndEventApproveConform(
            pendingRequestData,
            completionInfoO,
            modelConformance,
          )

        case (_: Verdict.Approve, Left(modelConformanceError))
            if ModelConformanceChecker
              .isSubviewsCheckEnabled(
                loggerFactory.name
              ) && modelConformanceError.aSubviewIsValid =>
          ErrorUtil.invalidState(
            s"The Mediator approved a request where a root view fails the model conformance check while a subview passes it"
          )

        case (_, Left(modelConformanceError)) =>
          rejected(
            LocalReject.MalformedRejects.ModelConformance.Reject(modelConformanceError.toString)(
              LocalVerdict.protocolVersionRepresentativeFor(protocolVersion)
            )
          )

        case (reasons: Verdict.ParticipantReject, _) =>
          // TODO(M40): Implement checks against malicious rejections and scrutinize the reasons such that an alarm is raised if necessary
          rejected(reasons.keyEvent)
        case (reject: Verdict.MediatorReject, _) =>
          rejected(reject)
      }
    }

    for {
      topologySnapshot <- EitherT.right[TransactionProcessorError](
        crypto.ips.awaitSnapshot(pendingRequestData.requestTime)
      )
      maxDecisionTime <- ProcessingSteps
        .getDecisionTime(topologySnapshot, pendingRequestData.requestTime)
        .leftMap(DomainParametersError(domainId, _))
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

  final case class SubmissionParam(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: WellFormedTransaction[WithoutSuffixes],
      disclosedContracts: Map[LfContractId, SerializableContract],
  )

  final case class EnrichedTransaction(
      policy: ConfirmationPolicy,
      rootViewsWithUsedAndCreated: UsedAndCreated,
      workflowIdO: Option[WorkflowId],
      submitterMetadataO: Option[SubmitterMetadata],
  ) {

    def transactionId: TransactionId = {
      val (tvt, _) = rootViewsWithUsedAndCreated.rootViewsWithSignatures.head1
      tvt.transactionId
    }

    def ledgerTime: CantonTimestamp = {
      val (tvt, _) = rootViewsWithUsedAndCreated.rootViewsWithSignatures.head1
      tvt.ledgerTime
    }
  }

  final case class ParallelChecksResult(
      authenticationResult: Map[ViewHash, String],
      consistencyResultE: Either[List[ReferenceToFutureContractError], Unit],
      authorizationResult: Map[ViewHash, String],
      conformanceResultE: Either[
        ModelConformanceChecker.ErrorWithSubviewsCheck,
        ModelConformanceChecker.Result,
      ],
      timeValidationResultE: Either[TimeCheckFailure, Unit],
  )

  final case class PendingDataAndResponseArgs(
      enrichedTransaction: EnrichedTransaction,
      requestTimestamp: CantonTimestamp,
      malformedPayloads: Seq[MalformedPayload],
      rc: RequestCounter,
      sc: SequencerCounter,
      snapshot: DomainSnapshotSyncCryptoApi,
  )

  final case class RejectionArgs(pendingTransaction: PendingTransaction, error: TransactionError)

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

  final case class CommonData(
      transactionId: TransactionId,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      confirmationPolicy: ConfirmationPolicy,
  )

}
