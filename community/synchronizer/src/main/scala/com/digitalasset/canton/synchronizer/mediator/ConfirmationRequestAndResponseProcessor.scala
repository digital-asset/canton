// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.semigroup.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{
  SigningKeyUsage,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{CantonTimestamp, ViewConfirmationParameters, ViewType}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.mediator.store.MediatorState
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Scalable service to validate the received MediatorConfirmationRequests and
  * ConfirmationResponses, derive a verdict, and send ConfirmationResultMessages to informee
  * participants.
  */
private[mediator] class ConfirmationRequestAndResponseProcessor(
    private val mediatorId: MediatorId,
    verdictSender: VerdictSender,
    crypto: SynchronizerCryptoClient,
    timeTracker: SynchronizerTimeTracker,
    val mediatorState: MediatorState,
    asynchronousProcessing: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with Spanning
    with FlagCloseable
    with HasCloseContext
    with MediatorEventHandler {

  private val psid = crypto.psid

  val processingQueue: ProcessingQueue[RequestId] =
    if (asynchronousProcessing) new ShardedSequentialProcessingQueue
    else new SynchronousProcessingQueue

  override def observeTimestampWithoutEvent(sequencingTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): HandlerResult =
    handleTimeouts(sequencingTimestamp)

  override def handleMediatorEvent(
      event: MediatorEvent
  )(implicit traceContext: TraceContext): HandlerResult =
    if (asynchronousProcessing) handleMediatorEventAsynchronous(event)
    else handleMediatorEventSynchronous(event)

  private def handleMediatorEventAsynchronous(
      event: MediatorEvent
  )(implicit traceContext: TraceContext): HandlerResult =
    for {
      asyncTimeoutHandling <- handleTimeouts(event.sequencingTimestamp)
      asyncEventHandling <- doHandleMediatorEvent(event)
    } yield asyncEventHandling |+| asyncTimeoutHandling

  private def handleMediatorEventSynchronous(
      event: MediatorEvent
  )(implicit traceContext: TraceContext): HandlerResult = {
    // to process synchronously, we inline the async results before continuing on to the next step
    val future = for {
      asyncTimeoutHandling <- handleTimeouts(event.sequencingTimestamp)
      _ <- asyncTimeoutHandling.unwrap
      asyncEventHandling <- doHandleMediatorEvent(event)
      _ <- asyncEventHandling.unwrap
    } yield ()
    HandlerResult.synchronous(future)
  }

  private def doHandleMediatorEvent(
      event: MediatorEvent
  )(implicit traceContext: TraceContext): HandlerResult = {
    val requestTimestamp = event.requestId.unwrap
    if (requestTimestamp > event.sequencingTimestamp) {
      val error = MediatorError.MalformedMessage.Reject(
        s"Received a mediator message for request $requestTimestamp with earlier sequencing time ${event.sequencingTimestamp}. Discarding the message."
      )
      error.report()
      HandlerResult.done
    } else {
      val synchronousResult: HandlerResult = for {
        snapshot <- crypto.ips.awaitSnapshot(requestTimestamp)

        synchronizerParameters <- snapshot
          .findDynamicSynchronizerParameters()
          .flatMap(_.toFutureUS(new IllegalStateException(_)))

        participantResponseDeadline <- FutureUnlessShutdown.outcomeF(
          synchronizerParameters.participantResponseDeadlineForF(requestTimestamp)
        )
        decisionTime <- synchronizerParameters.decisionTimeForF(requestTimestamp)
        asyncProcessing <- (event match {
          case MediatorEvent.Request(
                counter,
                _,
                requestEnvelope,
                rootHashMessages,
                batchAlsoContainsTopologyTransaction,
              ) =>
            mediatorState.registerTimeoutForRequest(
              event.requestId,
              participantResponseDeadline,
            )
            HandlerResult.asynchronous(
              processingQueue.enqueueForProcessing(event.requestId)(
                processRequest(
                  event.requestId,
                  counter,
                  participantResponseDeadline,
                  decisionTime,
                  requestEnvelope,
                  rootHashMessages,
                  batchAlsoContainsTopologyTransaction,
                )
              )
            )
          case MediatorEvent.Response(
                counter,
                responseTimestamp,
                responses,
                topologyTimestamp,
                recipients,
              ) =>
            HandlerResult.asynchronous(
              processingQueue.enqueueForProcessing(responses.message.requestId)(
                processResponses(
                  responseTimestamp,
                  counter,
                  participantResponseDeadline,
                  decisionTime,
                  responses,
                  topologyTimestamp,
                  recipients,
                )
              )
            )
        })

      } yield asyncProcessing
      synchronousResult
    }
  }

  private[mediator] def handleTimeouts(
      timestamp: CantonTimestamp
  ): HandlerResult =
    // Determine the timed-out requests in the synchronous processing stage, ...
    mediatorState
      .pendingTimedoutRequest(timestamp) match {
      case Nil => HandlerResult.done
      case nonEmptyTimeouts =>
        // ... but perform the actual timeout handling in the asynchronous processing stage.
        // This allows us to wait for the individual requests' previous processing to finish without blocking
        // the synchronous processing stage of subsequent events.
        HandlerResult.asynchronous(nonEmptyTimeouts.map(handleTimeout(_, timestamp)).sequence_)
    }

  @VisibleForTesting
  private[mediator] def handleTimeout(
      requestId: RequestId,
      timestamp: CantonTimestamp,
  ): FutureUnlessShutdown[Unit] = {
    def pendingRequestNotFound: FutureUnlessShutdown[Unit] = {
      noTracingLogger.debug(
        s"Pending aggregation for request [$requestId] not found. This implies the request has been finalized since the timeout was scheduled."
      )
      FutureUnlessShutdown.unit
    }

    processingQueue.enqueueForProcessing(requestId)(
      mediatorState.getPending(requestId).fold(pendingRequestNotFound) { responseAggregation =>
        // the event causing the timeout is likely unrelated to the transaction we're actually timing out,
        // so use the original request trace context
        implicit val traceContext: TraceContext = responseAggregation.requestTraceContext

        logger.info(
          s"Phase 6: Request ${requestId.unwrap}: Timeout in state ${responseAggregation.state} at $timestamp"
        )

        val timedOut = responseAggregation.timeout()
        MonadUtil.whenM(mediatorState.replace(responseAggregation, timedOut))(
          sendResultIfDone(timedOut, responseAggregation.decisionTime)
        )
      }
    )
  }

  /** Stores the incoming request in the MediatorStore. Sends a result message if no responses need
    * to be received or if the request is malformed, including if it declares a different mediator.
    */
  @VisibleForTesting
  private[mediator] def processRequest(
      requestId: RequestId,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      requestEnvelope: OpenEnvelope[MediatorConfirmationRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      batchAlsoContainsTopologyTransaction: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    withSpan("ConfirmationRequestAndResponseProcessor.processRequest") {
      val request = requestEnvelope.protocolMessage
      implicit traceContext =>
        span =>
          span.setAttribute("request_id", requestId.toString)
          span.setAttribute("counter", counter.toString)

          for {
            snapshot <- crypto.awaitSnapshot(requestId.unwrap)

            unitOrVerdictO <- validateRequest(
              requestId,
              requestEnvelope,
              rootHashMessages,
              snapshot,
              batchAlsoContainsTopologyTransaction,
            )

            // Take appropriate actions based on unitOrVerdictO
            asyncResult <- unitOrVerdictO match {
              // Request is well-formed, but not yet finalized
              case Right(()) =>
                val participantResponseDeadlineTick =
                  timeTracker.requestTick(participantResponseDeadline)
                for {
                  aggregation <- ResponseAggregation.fromRequest(
                    requestId,
                    request,
                    participantResponseDeadline,
                    decisionTime,
                    snapshot.ipsSnapshot,
                    Some(participantResponseDeadlineTick),
                  )
                  _ <- aggregation.asFinalized(
                    crypto.staticSynchronizerParameters.protocolVersion
                  ) match {
                    case None =>
                      // in case the aggregation is not finalized, we need to update the mediator state on the synchronous path
                      mediatorState.registerPendingRequest(aggregation)
                      FutureUnlessShutdown.unit
                    case Some(finalizedResponse) =>
                      // if the request is finalized, we don't update the in-memory state, but instead we can write the result
                      // on the asynchronous path
                      mediatorState.add(finalizedResponse)
                  }
                } yield {
                  logger.info(
                    show"Phase 2: Registered request=${requestId.unwrap} from submittingParticipant=${request.submittingParticipant} with ${request.informeesAndConfirmationParamsByViewPosition.size} view(s)."
                  )
                  logger.debug(
                    show"Phase 2: Initial state for request=${requestId.unwrap}: ${aggregation.showMergedState}"
                  )
                }

              // Request is finalized, approve / reject immediately
              case Left(Some(rejection)) =>
                val verdict = rejection.toVerdict(psid.protocolVersion)
                logger.debug(show"$requestId: finalizing immediately with verdict $verdict...")
                for {
                  _ <-
                    verdictSender.sendReject(
                      requestId,
                      Some(request),
                      rootHashMessages,
                      verdict,
                      decisionTime,
                    )
                  _ <- mediatorState.add(
                    FinalizedResponse(requestId, request, requestId.unwrap, verdict)(
                      traceContext
                    )
                  )
                } yield ()

              // Discard request
              case Left(None) =>
                logger.debug(show"$requestId: discarding request...")
                FutureUnlessShutdown.unit
            }
          } yield asyncResult
    }

  /** Validate a mediator confirmation request
    *
    * Yields `Left(Some(verdict))`, if `request` can already be finalized. Yields `Left(None)`, if
    * `request` should be discarded
    */
  private def validateRequest(
      requestId: RequestId,
      requestEnvelope: OpenEnvelope[MediatorConfirmationRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      batchAlsoContainsTopologyTransaction: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[Option[MediatorVerdict.MediatorReject], Unit]] = {
    val topologySnapshot = snapshot.ipsSnapshot
    val request = requestEnvelope.protocolMessage
    (for {
      // Bail out, if this mediator or group is passive, except if the mediator itself is passive in an active group.
      isActive <- EitherT
        .right[Option[MediatorVerdict.MediatorReject]](
          topologySnapshot.isMediatorActive(mediatorId)
        )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        isActive, {
          logger.info(
            show"Ignoring mediator confirmation request $requestId because I'm not active or mediator group is not active."
          )
          Option.empty[MediatorVerdict.MediatorReject]
        },
      )

      submissionTimeTopologySnapshotO <- EitherT.right(
        getSubmissionTopologySnapshot(requestId, rootHashMessages)
      )

      // Validate signature of submitting participant
      _ <- checkAndReportMaliciousParticipant(
        snapshot,
        submissionTimeTopologySnapshotO,
      )(
        _.verifySignature(
          request.rootHash.unwrap,
          request.submittingParticipant,
          request.submittingParticipantSignature,
          SigningKeyUsage.ProtocolOnly,
        ).leftMap(err =>
          // The error is logged at the appropriate level by checkAndReportMaliciousParticipant
          MediatorError.MalformedMessage.Reject(
            show"Received a mediator confirmation request with id $requestId from ${request.submittingParticipant} with an invalid signature. Rejecting request.\nDetailed error: $err"
          )
        )
      ).leftMap(Option.apply)

      // Validate activeness of informee participants
      _ <- checkAndReportMaliciousParticipant(snapshot, submissionTimeTopologySnapshotO)(
        _.ipsSnapshot
          .allHaveActiveParticipants(request.allInformees)
          .leftMap(informeesNoParticipant =>
            // The error is logged at the appropriate level by checkAndReportMaliciousParticipant
            MediatorError.InvalidMessage.Reject(
              show"Received a mediator confirmation request with id $requestId with some informees not being hosted by an active participant: $informeesNoParticipant. Rejecting request..."
            )
          )
      ).leftMap(Option.apply)

      // Validate declared mediator and the group being active
      validMediator <- checkDeclaredMediator(
        requestId,
        requestEnvelope,
        snapshot,
        submissionTimeTopologySnapshotO,
      )

      // Validate root hash messages
      _ <- checkRootHashMessages(
        validMediator,
        requestId,
        request,
        rootHashMessages,
        snapshot,
        submissionTimeTopologySnapshotO,
      )
        .leftMap(Option.apply)

      // Validate minimum threshold
      _ <- EitherT
        .fromEither[FutureUnlessShutdown](validateMinimumThreshold(requestId, request))
        .leftMap(Option.apply)

      // Reject, if the authorized confirming parties cannot attain the threshold
      _ <- validateAuthorizedConfirmingParties(
        requestId,
        request,
        snapshot,
        submissionTimeTopologySnapshotO,
      )
        .leftMap(Option.apply)

      // Reject, if the batch also contains a topology transaction
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          !batchAlsoContainsTopologyTransaction, {
            val rejection = MediatorError.MalformedMessage
              .Reject(
                s"Received a mediator confirmation request with id $requestId also containing a topology transaction."
              )
              .reported()
            MediatorVerdict.MediatorReject(rejection)
          },
        )
        .leftMap(Option.apply)
    } yield ()).value
  }

  private def checkDeclaredMediator(
      requestId: RequestId,
      requestEnvelope: OpenEnvelope[MediatorConfirmationRequest],
      topologySnapshot: SynchronizerSnapshotSyncCryptoApi,
      submissionTimeTopologySnapshotO: Option[SynchronizerSnapshotSyncCryptoApi],
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, Option[
    MediatorVerdict.MediatorReject
  ], MediatorGroupRecipient] = {

    val request = requestEnvelope.protocolMessage

    def rejectWrongMediator(hint: => String): MediatorError.MalformedMessage.Reject =
      MediatorError.MalformedMessage
        .Reject(
          show"Rejecting mediator confirmation request with $requestId, mediator ${request.mediator}, topology at ${topologySnapshot.ipsSnapshot.timestamp} due to $hint"
        )

    def getMediatorGroup(
        snapshot: SynchronizerSnapshotSyncCryptoApi
    ): EitherT[FutureUnlessShutdown, MediatorError.MalformedMessage.Reject, MediatorGroup] =
      for {
        mediatorGroupO <- EitherT.right(
          snapshot.ipsSnapshot.mediatorGroup(request.mediator.group)(loggingContext)
        )
        mediatorGroup <- EitherT.fromOption[FutureUnlessShutdown](
          mediatorGroupO,
          rejectWrongMediator(show"unknown mediator group"),
        )
      } yield mediatorGroup

    val submissionTimeIndependentChecks = for {
      mediatorGroup <- getMediatorGroup(topologySnapshot)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        mediatorGroup.active.contains(mediatorId) || mediatorGroup.passive.contains(
          mediatorId
        ),
        rejectWrongMediator(show"this mediator not being part of the mediator group"),
      )
      expectedRecipients = Recipients.cc(request.mediator)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        requestEnvelope.recipients == expectedRecipients,
        rejectWrongMediator(
          show"wrong recipients (expected $expectedRecipients, actual ${requestEnvelope.recipients})"
        ),
      )
    } yield ()

    for {
      _ <- submissionTimeIndependentChecks.leftMap { error =>
        // log the submission-time independent checks explicitly.
        Option(MediatorVerdict.MediatorReject(error.reported()))
      }
      _ <- checkAndReportMaliciousParticipant(topologySnapshot, submissionTimeTopologySnapshotO)(
        snapshot =>
          getMediatorGroup(snapshot).flatMap { mediatorGroup =>
            EitherTUtil.condUnitET[FutureUnlessShutdown](
              mediatorGroup.isActive,
              // The error is logged at the appropriate level by checkAndReportMaliciousParticipant
              rejectWrongMediator(show"inactive mediator group"),
            )
          }
      )(loggingContext.traceContext).leftMap(Option.apply)
    } yield request.mediator

  }

  private def checkRootHashMessages(
      validMediator: MediatorGroupRecipient,
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      sequencingSnapshot: SynchronizerSnapshotSyncCryptoApi,
      submissionTimeSnapshotO: Option[SynchronizerSnapshotSyncCryptoApi],
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[FutureUnlessShutdown, MediatorVerdict.MediatorReject, Unit] = {

    // since `checkDeclaredMediator` already validated against the mediatorId we can safely use validMediator = request.mediator
    val (wrongRecipients, correctRecipients) = RootHashMessageRecipients.wrongAndCorrectRecipients(
      rootHashMessages.map(_.recipients),
      validMediator,
    )

    val rootHashMessagesRecipients = correctRecipients
      .flatMap(recipients => recipients.collect { case m @ MemberRecipient(_: ParticipantId) => m })

    def repeatedMembers(recipients: Seq[Recipient]): Seq[Recipient] = {
      val repeatedRecipientsB = Seq.newBuilder[Recipient]
      val seen = new mutable.HashSet[Recipient]()
      recipients.foreach { recipient =>
        val fresh = seen.add(recipient)
        if (!fresh) repeatedRecipientsB += recipient
      }
      repeatedRecipientsB.result()
    }

    def wrongRootHashes(expectedRootHash: RootHash): Seq[RootHash] =
      rootHashMessages.mapFilter { envelope =>
        val rootHash = envelope.protocolMessage.rootHash
        if (rootHash == expectedRootHash) None else Some(rootHash)
      }.distinct

    def distinctPayloads: Seq[SerializedRootHashMessagePayload] =
      rootHashMessages.map(_.protocolMessage.payload).distinct

    def wrongViewType(expectedViewType: ViewType): Seq[ViewType] =
      rootHashMessages.map(_.protocolMessage.viewType).filterNot(_ == expectedViewType).distinct

    def reportedMediatorReject(reason: String): MediatorVerdict.MediatorReject =
      MediatorVerdict.MediatorReject(malformedMessage(reason).reported())

    def malformedMessage(reason: String): MediatorError.MalformedMessage.Reject = {
      val message =
        s"Received a mediator confirmation request with id $requestId with invalid root hash messages. Rejecting... Reason: $reason"
      MediatorError.MalformedMessage.Reject(message)
    }

    for {
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          wrongRecipients.isEmpty,
          reportedMediatorReject(
            show"Root hash messages with wrong recipients tree: $wrongRecipients"
          ),
        )
      repeated = repeatedMembers(rootHashMessagesRecipients)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        repeated.isEmpty,
        reportedMediatorReject(
          show"Several root hash messages for recipients: $repeated"
        ),
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        distinctPayloads.sizeIs <= 1,
        reportedMediatorReject(
          show"Different payloads in root hash messages. Sizes: ${distinctPayloads.map(_.bytes.size).mkShow()}."
        ),
      )

      wrongHashes = wrongRootHashes(request.rootHash)
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          wrongHashes.isEmpty,
          reportedMediatorReject(show"Wrong root hashes: $wrongHashes"),
        )

      wrongViewTypes = wrongViewType(request.viewType)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        wrongViewTypes.isEmpty,
        reportedMediatorReject(
          show"View types in root hash messages differ from expected view type ${request.viewType}: $wrongViewTypes"
        ),
      )
      _ <- checkAndReportMaliciousParticipant(sequencingSnapshot, submissionTimeSnapshotO)(
        snapshot =>
          RootHashMessageRecipients
            .wrongMembers(
              rootHashMessagesRecipients,
              request,
              snapshot.ipsSnapshot,
            )(ec, loggingContext)
            // The error is logged at the appropriate level by checkAndReportMaliciousParticipant
            .leftMap(malformedMessage)
      )(loggingContext.traceContext)

    } yield ()
  }

  private def validateMinimumThreshold(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
  )(implicit loggingContext: ErrorLoggingContext): Either[MediatorVerdict.MediatorReject, Unit] =
    request.informeesAndConfirmationParamsByViewPosition.toSeq
      .traverse_ { case (viewPosition, ViewConfirmationParameters(_, quorums)) =>
        val minimumThreshold = NonNegativeInt.one
        Either.cond(
          quorums.exists(quorum => quorum.threshold >= minimumThreshold),
          (),
          MediatorVerdict.MediatorReject(
            MediatorError.MalformedMessage
              .Reject(
                s"Received a mediator confirmation request with id $requestId for transaction view at $viewPosition, where no quorum of the list satisfies the minimum threshold. Rejecting request..."
              )
              .reported()
          ),
        )
      }

  private def validateAuthorizedConfirmingParties(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      sequencingTimeSnapshot: SynchronizerSnapshotSyncCryptoApi,
      submissionTimeTopologyTimestampO: Option[SynchronizerSnapshotSyncCryptoApi],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MediatorVerdict.MediatorReject, Unit] =
    checkAndReportMaliciousParticipant(sequencingTimeSnapshot, submissionTimeTopologyTimestampO) {
      snapshot =>
        request.informeesAndConfirmationParamsByViewPosition.toList
          .parTraverse_ { case (viewPosition, viewConfirmationParameters) =>
            // sorting parties to get deterministic error messages
            val declaredConfirmingParties =
              viewConfirmationParameters.confirmers.toSeq.sortBy(pId => pId)

            for {
              hostedConfirmingParties <- EitherT.right[MediatorError](
                snapshot.ipsSnapshot
                  .isHostedByAtLeastOneParticipantF(
                    declaredConfirmingParties.toSet,
                    (_, attr) => attr.canConfirm,
                  )
              )

              (authorized, unauthorized) = declaredConfirmingParties.partition(
                hostedConfirmingParties.contains
              )

              confirmed = viewConfirmationParameters.quorums.forall { quorum =>
                // For the authorized informees that belong to each quorum, verify if their combined weight is enough
                // to meet the quorum's threshold.
                quorum.confirmers
                  .filter { case (partyId, _) => authorized.contains(partyId) }
                  .values
                  .map(_.unwrap)
                  .sum >= quorum.threshold.unwrap
              }

              _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][MediatorError](
                confirmed, {
                  val insufficientPermissionHint =
                    if (unauthorized.nonEmpty)
                      show"\nParties without participant having permission to confirm: $unauthorized"
                    else ""

                  val authorizedPartiesHint =
                    if (authorized.nonEmpty) show"\nAuthorized parties: $authorized" else ""

                  val rejection = MediatorError.MalformedMessage
                    .Reject(
                      s"Received a mediator confirmation request with id $requestId with insufficient authorized confirming parties for transaction view at $viewPosition. " +
                        s"Rejecting request." +
                        insufficientPermissionHint +
                        authorizedPartiesHint
                    )
                  // The error is logged at the appropriate level by checkAndReportMaliciousParticipant
                  rejection
                },
              )
            } yield ()
          }
    }

  def processResponses(
      ts: CantonTimestamp,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      signedResponses: SignedProtocolMessage[ConfirmationResponses],
      topologyTimestamp: Option[CantonTimestamp],
      recipients: Recipients,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    withSpan("ConfirmationRequestAndResponseProcessor.processResponse") {
      implicit traceContext => span =>
        span.setAttribute("timestamp", ts.toString)
        span.setAttribute("counter", counter.toString)

        val responses = signedResponses.message
        logger.info(
          show"Phase 5: Received ${responses.responses.size} response(s) for request=${responses.requestId.unwrap}."
        )
        logger.debug(show"Phase 5: Responses for request=${responses.requestId.unwrap}: $responses")
        (for {
          snapshot <- OptionT.liftF(crypto.awaitSnapshot(responses.requestId.unwrap))
          _ <- signedResponses
            .verifySignature(snapshot, responses.sender)
            .leftMap(err =>
              MediatorError.MalformedMessage
                .Reject(
                  s"$psid (timestamp: $ts): invalid signature from ${responses.sender} with $err"
                )
                .report()
            )
            .toOption
          _ <-
            if (signedResponses.psid == psid)
              OptionT.some[FutureUnlessShutdown](())
            else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${responses.requestId}, sender ${responses.sender}: Discarding confirmation response for wrong synchronizer ${signedResponses.psid}"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }

          _ <-
            if (ts <= participantResponseDeadline) OptionT.some[FutureUnlessShutdown](())
            else {
              logger.warn(
                s"Response $ts is too late as request ${responses.requestId} has already exceeded the participant response deadline [$participantResponseDeadline]"
              )
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          _ <- {
            // To ensure that a mediator group address is resolved in the same way as for the request
            // we require that the topology timestamp on the response submission request is set to the
            // request's sequencing time. The sequencer communicates this timestamp to the client
            // via the timestamp of signing key.
            if (topologyTimestamp.contains(responses.requestId.unwrap))
              OptionT.some[FutureUnlessShutdown](())
            else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${responses.requestId}, sender ${responses.sender}: Discarding confirmation response because the topology timestamp is not set to the request id [$topologyTimestamp]"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          }

          responseAggregation <- mediatorState.fetch(responses.requestId).orElse {
            // This can happen after a fail-over or as part of an attack.
            val cause =
              s"Received a confirmation response at $ts by ${responses.sender} with an unknown request id ${responses.requestId}. Discarding response..."
            val error = MediatorError.InvalidMessage.Reject(cause)
            error.log()

            OptionT.none[FutureUnlessShutdown, ResponseAggregator]
          }

          _ <- {
            if (
              // Check that this message was sent to all mediators in the group.
              // Ignore other recipients of the response so that this check does not rely any recipients restrictions
              // that are enforced in the sequencer.
              recipients.allRecipients.contains(responseAggregation.request.mediator)
            ) {
              OptionT.some[FutureUnlessShutdown](())
            } else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${responses.requestId}, sender ${responses.sender}: Discarding confirmation response with wrong recipients $recipients, expected ${responseAggregation.request.mediator}"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          }
          nextResponseAggregation <- OptionT(
            responseAggregation.validateAndProgress(ts, responses, snapshot.ipsSnapshot)
          )
          _unit <- OptionT(
            mediatorState
              .replace(responseAggregation, nextResponseAggregation)
              .map(Option.when(_)(()))
          )
          _ <- OptionT.some[FutureUnlessShutdown](
            // we can send the result asynchronously, as there is no need to reply in
            // order and there is no need to guarantee delivery of verdicts
            doNotAwait(
              responses.requestId,
              sendResultIfDone(nextResponseAggregation, decisionTime),
            )
          )
        } yield ()).value.map(_ => ())
    }

  /** This method is here to allow overriding the async send & determinism in tests
    */
  protected def doNotAwait(requestId: RequestId, f: => FutureUnlessShutdown[Any])(implicit
      tc: TraceContext
  ): Future[Unit] = {
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      synchronizeWithClosing("send-result-if-done")(f),
      s"send-result-if-done failed for request $requestId",
      level = Level.WARN,
    )
    Future.unit
  }

  private def sendResultIfDone(
      responseAggregation: ResponseAggregation[?],
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    responseAggregation.asFinalized(psid.protocolVersion) match {
      case Some(finalizedResponse) =>
        logger.info(
          s"Phase 6: Finalized request=${finalizedResponse.requestId} with verdict ${finalizedResponse.verdict}"
        )

        // We've reached a verdict. Cancel any outstanding request for a tick of the participant response deadline.
        responseAggregation.participantResponseDeadlineTick.foreach(_.cancel())

        finalizedResponse.verdict match {
          case Verdict.Approve() => mediatorState.metrics.approvedRequests.mark()
          case _: Verdict.MediatorReject | _: Verdict.ParticipantReject => ()
        }

        verdictSender.sendResult(
          finalizedResponse.requestId,
          finalizedResponse.request,
          finalizedResponse.verdict,
          decisionTime,
        )
      case None =>
        /* no op */
        FutureUnlessShutdown.unit
    }

  // Retrieve the topology snapshot at submission time. Return `None` in case of error.
  private def getSubmissionTopologySnapshot(
      requestId: RequestId,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
  )(implicit
      traceContext: TraceContext,
      errorLoggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[Option[SynchronizerSnapshotSyncCryptoApi]] = {
    val submissionTopologyTimestamps = rootHashMessages
      .map(_.protocolMessage.submissionTopologyTimestamp)
      .distinct

    submissionTopologyTimestamps match {
      case Seq(submissionTopologyTimestamp) =>
        val sequencingTimestamp = requestId.unwrap
        SubmissionTopologyHelper.getSubmissionTopologySnapshot(
          timeouts,
          sequencingTimestamp,
          submissionTopologyTimestamp,
          crypto,
        )

      case Seq() =>
        // This can only happen if there are no root hash messages.
        // This will be detected during the wrong members check and logged as a warning, so we can log at info level.
        logger.info(
          s"No declared submission topology timestamp found. Inconsistencies will be logged as warnings."
        )
        FutureUnlessShutdown.pure(None)

      case _ =>
        // Log at warning level because this is not detected by another check
        logger.warn(
          s"Found ${submissionTopologyTimestamps.size} different declared submission topology timestamps. Inconsistencies will be logged as warnings."
        )
        FutureUnlessShutdown.pure(None)
    }
  }

  /** Performs a validation based on the sequencing snapshot. In case of failure, checks whether the
    * validation would have succeeded at the submission topology timestamp.
    *
    * If yes, log the rejection at INFO, because this was merely a race between the request and a
    * topology change.
    *
    * If not, log the rejection as WARN because the participant is acting maliciously.
    *
    * The request will be rejected regardless, but the rejection will be logged at the appropriate
    * log level.
    */
  private def checkAndReportMaliciousParticipant[Result](
      sequencingSnapshot: SynchronizerSnapshotSyncCryptoApi,
      submissionTimeSnapshotO: Option[SynchronizerSnapshotSyncCryptoApi],
  )(
      check: SynchronizerSnapshotSyncCryptoApi => EitherT[
        FutureUnlessShutdown,
        MediatorError,
        Result,
      ]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, MediatorVerdict.MediatorReject, Result] =
    check(sequencingSnapshot).leftFlatMap { reject =>
      val errorLoggedAtAdjustedLevel = for {
        validAtSubmissionTopologyTimestamp <-
          submissionTimeSnapshotO.existsM(
            check(_).isRight
          )
      } yield {
        if (validAtSubmissionTopologyTimestamp) {
          // Log the error at INFO level, since it was just a race between the request and a topology change.
          logger.info(
            s"""$reject
               | This error is due to a change of topology state between the declared topology timestamp used
               | for submission and the sequencing time of the request.""".stripMargin
          )
        } else {
          // otherwise log the error at the original log level.
          reject.log()
        }
        MediatorVerdict.MediatorReject(reject)
      }

      EitherT.left(errorLoggedAtAdjustedLevel)
    }

}
