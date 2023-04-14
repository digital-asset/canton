// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, ViewType}
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, RootHash, v0}
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Scalable service to check the received Stakeholder Trees and Confirmation Responses, derive a verdict and post
  * result messages to stakeholders.
  */
private[mediator] class ConfirmationResponseProcessor(
    domainId: DomainId,
    private val mediatorId: MediatorId,
    verdictSender: VerdictSender,
    crypto: DomainSyncCryptoClient,
    timeTracker: DomainTimeTracker,
    val mediatorState: MediatorState,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with Spanning
    with FlagCloseable
    with HasCloseContext {

  /** Handle events for a single request-id.
    * Callers should ensure all events are for the same request and ordered by sequencer time.
    */
  def handleRequestEvents(
      requestId: RequestId,
      events: Seq[Traced[MediatorEvent]],
      callerTraceContext: TraceContext,
  ): HandlerResult = {

    val requestTs = requestId.unwrap

    val future = for {
      // FIXME(M40): do not block if requestId is far in the future
      snapshot <- crypto.ips.awaitSnapshot(requestId.unwrap)(callerTraceContext)

      domainParameters <- snapshot
        .findDynamicDomainParameters()(callerTraceContext)
        .flatMap(_.toFuture(new IllegalStateException(_)))

      participantResponseDeadline <- domainParameters.participantResponseDeadlineForF(requestTs)
      decisionTime <- domainParameters.decisionTimeForF(requestTs)

      _ <- MonadUtil.sequentialTraverse_(events) {
        _.withTraceContext { implicit traceContext =>
          {
            case MediatorEvent.Request(counter, _, request, rootHashMessages) =>
              processRequest(
                requestId,
                counter,
                participantResponseDeadline,
                decisionTime,
                request,
                rootHashMessages,
              )
            case MediatorEvent.Response(counter, timestamp, response) =>
              processResponse(
                timestamp,
                counter,
                participantResponseDeadline,
                decisionTime,
                response,
              )
            case MediatorEvent.Timeout(_counter, timestamp, requestId) =>
              handleTimeout(requestId, timestamp, decisionTime)
          }
        }
      }
    } yield ()
    HandlerResult.synchronous(FutureUnlessShutdown.outcomeF(future))
  }

  @VisibleForTesting
  private[mediator] def handleTimeout(
      requestId: RequestId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def pendingRequestNotFound: Future[Unit] = {
      logger.debug(
        s"Pending aggregation for request [$requestId] not found. This implies the request has been finalized since the timeout was scheduled."
      )
      Future.unit
    }

    mediatorState.getPending(requestId).fold(pendingRequestNotFound) { responseAggregation =>
      // the event causing the timeout is likely unrelated to the transaction we're actually timing out,
      // so replace the implicit trace context with the original request trace
      implicit val traceContext = responseAggregation.requestTraceContext

      logger
        .debug(s"Request ${requestId}: Timeout in state ${responseAggregation.state} at $timestamp")

      val timeout = responseAggregation.timeout(version = timestamp)
      mediatorState
        .replace(responseAggregation, timeout)
        .semiflatMap { _ =>
          sendResultIfDone(timeout, decisionTime)
        }
        .getOrElse(())
    }
  }

  /** Stores the incoming request in the MediatorStore.
    * Sends a result message if no responses need to be received or if the request is malformed,
    * including if it declares a different mediator.
    */
  @VisibleForTesting
  private[mediator] def processRequest(
      requestId: RequestId,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    withSpan("ConfirmationResponseProcessor.processRequest") { implicit traceContext => span =>
      span.setAttribute("request_id", requestId.toString)
      span.setAttribute("counter", counter.toString)

      for {
        topologySnapshot <- crypto.ips.awaitSnapshot(requestId.unwrap)

        unitOrVerdictO <- validateRequest(
          requestId,
          request,
          rootHashMessages,
          topologySnapshot,
        )

        // Take appropriate actions based on unitOrVerdictO
        _ <- unitOrVerdictO match {
          // Request is well-formed, but not yet finalized
          case Right(()) =>
            val aggregationF = ResponseAggregation.fromRequest(
              requestId,
              request,
              protocolVersion,
              topologySnapshot,
            )(loggerFactory)

            for {
              aggregation <- aggregationF
              _ <- mediatorState.add(aggregation)
            } yield {
              timeTracker.requestTick(participantResponseDeadline)

              logger.debug(
                show"$requestId: registered informee message. Initial state: ${aggregation.state.showMerged}"
              )
            }

          // Request is finalized, approve / reject immediately
          case Left(Some(verdict)) =>
            logger.debug(
              show"$requestId: finalizing immediately with verdict $verdict..."
            )

            val aggregation = ResponseAggregation.fromVerdict(
              requestId,
              request,
              verdict,
              protocolVersion,
            )(loggerFactory)

            for {
              _ <- verdict match {
                case mediatorReject: MediatorReject =>
                  sendMalformedRejection(
                    requestId,
                    Some(request),
                    rootHashMessages,
                    mediatorReject,
                    decisionTime,
                  )

                case _: Verdict =>
                  verdictSender.sendResult(requestId, request, verdict, decisionTime)
              }

              _ <- mediatorState.add(aggregation)
            } yield ()

          // Discard request
          case Left(None) =>
            logger.debug(show"$requestId: discarding request...")

            Future.successful(None)
        }
      } yield ()
    }
  }

  /** Validate a mediator request
    *
    * Yields `Left(Some(verdict))`, if `request` can already be finalized.
    * Yields `Left(None)`, if `request` should be discarded
    */
  private def validateRequest(
      requestId: RequestId,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): Future[Either[Option[Verdict], Unit]] = (for {

    // Bail out, if this mediator is passive
    isActive <- EitherT.right(topologySnapshot.isMediatorActive(mediatorId))
    _ <- EitherTUtil.condUnitET[Future][Option[Verdict]](
      isActive, {
        logger.info(show"Ignoring mediator request $requestId because I'm not active.")
        None
      },
    )

    // Validate activeness of informee participants
    _ <- topologySnapshot
      .allHaveActiveParticipants(request.allInformees)
      .leftMap { informeesNoParticipant =>
        val reject = MediatorError.InvalidMessage.Reject.create(
          show"Received a mediator request with id $requestId with some informees not being hosted by an active participant: $informeesNoParticipant. Rejecting request...",
          protocolVersion,
          v0.MediatorRejection.Code.InformeesNotHostedOnActiveParticipant,
        )
        reject.log()
        Some(reject)
      }

    // Validate root hash messages
    _ <- checkRootHashMessages(requestId, request, rootHashMessages, topologySnapshot)
      .leftMap[Option[Verdict]](Some(_))

    // Validate declared mediator id
    _ <- EitherTUtil.condUnitET[Future][Option[Verdict]](
      request.mediatorId == mediatorId,
      Some(
        MediatorError.MalformedMessage
          .Reject(
            show"Received a mediator request with id $requestId with an incorrect mediator id ${request.mediatorId}. Rejecting request...",
            v0.MediatorRejection.Code.WrongDeclaredMediator,
            protocolVersion,
          )
          .reported()
      ),
    )

    // Validate minimum threshold
    _ <- EitherT
      .fromEither[Future](validateMinimumThreshold(requestId, request, protocolVersion))
      .leftMap[Option[Verdict]](Some(_))

    // Reject, if the authorized confirming parties cannot attain the threshold
    _ <-
      validateAuthorizedConfirmingParties(
        requestId,
        request,
        topologySnapshot,
        protocolVersion,
      )
        .leftMap[Option[Verdict]](Some(_))

  } yield ()).value

  private def checkRootHashMessages(
      requestId: RequestId,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      topologySnapshot: TopologySnapshot,
  )(implicit loggingContext: ErrorLoggingContext): EitherT[Future, MediatorReject, Unit] = {
    val (wrongRecipients, oneMemberRecipients) = rootHashMessages.flatMap { rhm =>
      rhm.recipients.trees.toList.map {
        case tree @ RecipientsTree(group, Seq()) =>
          Either.cond(group.size == 2 && group.contains(mediatorId), group, tree)
        case badTree => Left(badTree)
      }
    }.separate
    val members = oneMemberRecipients.mapFilter(recipients => (recipients - mediatorId).headOption)

    def repeatedMembers(members: Seq[Member]): Seq[Member] = {
      val repeatedMembersB = Seq.newBuilder[Member]
      val seen = new mutable.HashSet[Member]()
      members.foreach { member =>
        val fresh = seen.add(member)
        if (!fresh) repeatedMembersB += member
      }
      repeatedMembersB.result()
    }

    def wrongRootHashes(expectedRootHash: RootHash): Seq[RootHash] =
      rootHashMessages.mapFilter { envelope =>
        val rootHash = envelope.protocolMessage.rootHash
        if (rootHash == expectedRootHash) None else Some(rootHash)
      }.distinct

    final case class WrongMembers(
        missingInformeeParticipants: Set[Member],
        superfluousMembers: Set[Member],
    )

    def wrongMembers(): Future[WrongMembers] = {
      val allInformeeParticipantsF =
        request.allInformees.toList
          .parTraverse(topologySnapshot.activeParticipantsOf)
          .map(_.flatMap(_.keySet).toSet[Member])
      allInformeeParticipantsF.map { allInformeeParticipants =>
        val membersSet = members.toSet
        val missingInformeeParticipants = allInformeeParticipants diff membersSet
        val superfluousMembers = membersSet diff allInformeeParticipants
        WrongMembers(missingInformeeParticipants, superfluousMembers)
      }
    }

    def distinctPayloads: Seq[SerializedRootHashMessagePayload] =
      rootHashMessages.map(_.protocolMessage.payload).distinct

    def wrongViewType(expectedViewType: ViewType): Seq[ViewType] =
      rootHashMessages.map(_.protocolMessage.viewType).filterNot(_ == expectedViewType).distinct

    val unitOrRejectionReason = for {
      _ <- EitherTUtil
        .condUnitET[Future](
          wrongRecipients.isEmpty,
          show"Root hash messages with wrong recipients tree: $wrongRecipients",
        )
      repeated = repeatedMembers(members)
      _ <- EitherTUtil.condUnitET[Future](
        repeated.isEmpty,
        show"Several root hash messages for members: $repeated",
      )
      _ <- EitherTUtil.condUnitET[Future](
        distinctPayloads.sizeCompare(1) <= 0,
        show"Different payloads in root hash messages. Sizes: ${distinctPayloads.map(_.bytes.size).mkShow()}.",
      )
      _ <- request.rootHash match {
        case None =>
          EitherTUtil.condUnitET[Future](
            rootHashMessages.isEmpty,
            show"No root hash messages expected, but received for members: $members",
          )
        case Some(rootHash) =>
          val wrongHashes = wrongRootHashes(rootHash)
          val wrongViewTypes = wrongViewType(request.viewType)
          val wrongMembersF = wrongMembers()
          for {
            _ <- EitherTUtil
              .condUnitET[Future](wrongHashes.isEmpty, show"Wrong root hashes: $wrongHashes")
            wrongMems <- EitherT.liftF(wrongMembersF)
            _ <- EitherTUtil.condUnitET[Future](
              wrongViewTypes.isEmpty,
              show"View types in root hash messages differ from expected view type ${request.viewType}: $wrongViewTypes",
            )
            _ <- EitherTUtil.condUnitET[Future](
              wrongMems.missingInformeeParticipants.isEmpty,
              show"Missing root hash message for informee participants: ${wrongMems.missingInformeeParticipants}",
            )
            _ <- EitherTUtil.condUnitET[Future](
              wrongMems.superfluousMembers.isEmpty,
              show"Superfluous root hash message for members: ${wrongMems.superfluousMembers}",
            )
          } yield ()
      }
    } yield ()

    unitOrRejectionReason.leftMap((rejectionReason: String) =>
      MediatorError.MalformedMessage
        .Reject(
          s"Received a mediator request with id $requestId with invalid root hash messages. Rejecting... Reason: $rejectionReason",
          v0.MediatorRejection.Code.InvalidRootHashMessage,
          protocolVersion,
        )
        .reported()
    )
  }

  private def validateMinimumThreshold(
      requestId: RequestId,
      request: MediatorRequest,
      protocolVersion: ProtocolVersion,
  )(implicit loggingContext: ErrorLoggingContext): Either[MediatorReject, Unit] = {
    val minimumThreshold = request.confirmationPolicy.minimumThreshold

    request.informeesAndThresholdByView.toSeq
      .traverse_ { case (viewHash, (_, threshold)) =>
        EitherUtil.condUnitE(
          threshold >= minimumThreshold,
          MediatorError.MalformedMessage
            .Reject(
              s"Received a mediator request with id $requestId having threshold $threshold for transaction view $viewHash, which is below the confirmation policy's minimum threshold of $minimumThreshold. Rejecting request...",
              v0.MediatorRejection.Code.ViewThresholdBelowMinimumThreshold,
              protocolVersion,
            )
            .reported(),
        )
      }
  }

  private def validateAuthorizedConfirmingParties(
      requestId: RequestId,
      request: MediatorRequest,
      snapshot: TopologySnapshot,
      protocolVersion: ProtocolVersion,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, MediatorReject, Unit] = {
    val requiredTrustLevel = request.confirmationPolicy.requiredTrustLevel

    request.informeesAndThresholdByView.toList
      .parTraverse_ { case (viewHash, (informees, threshold)) =>
        // sorting parties to get deterministic error messages
        val declaredConfirmingParties =
          informees.collect { case p: ConfirmingParty => p }.toSeq.sortBy(_.party)

        for {
          partitionedConfirmingParties <- EitherT.right[MediatorReject](
            declaredConfirmingParties.parTraverse { p =>
              for {
                canConfirm <- snapshot.isHostedByAtLeastOneParticipantF(
                  p.party,
                  attr =>
                    attr.permission.canConfirm && attr.trustLevel.rank >= requiredTrustLevel.rank,
                )
              } yield Either.cond(canConfirm, p, p)
            }
          )

          (unauthorized, authorized) = partitionedConfirmingParties.separate

          _ <- EitherTUtil.condUnitET[Future][MediatorReject](
            authorized.map(_.weight).sum >= threshold.value, {
              val unauthorizedPartiesHint =
                if (unauthorized.nonEmpty)
                  if (requiredTrustLevel == TrustLevel.Vip)
                    show"\nParties without VIP participant: $unauthorized"
                  else
                    show"\nParties without participant having permission to confirm: $unauthorized"
                else ""
              val authorizedPartiesHint =
                if (authorized.nonEmpty) show"\nAuthorized parties: $authorized" else ""

              MediatorError.MalformedMessage
                .Reject(
                  s"Received a mediator request with id $requestId with insufficient authorized confirming parties for transaction view $viewHash. " +
                    s"Rejecting request. Threshold: $threshold." +
                    unauthorizedPartiesHint +
                    authorizedPartiesHint,
                  v0.MediatorRejection.Code.NotEnoughConfirmingParties,
                  protocolVersion,
                )
                .reported()
            },
          )

        } yield ()
      }
  }

  private[mediator] def sendMalformedRejection(
      requestId: RequestId,
      requestO: Option[MediatorRequest],
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      rejectionReason: MediatorReject,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    // For each view type among the root hash messages,
    // we send one rejection message to all participants, where each participant is in its own recipient group.
    // This ensures that participants do not learn from the list of recipients who else is involved in the transaction.
    // This can happen without a malicious submitter, e.g., if the topology has changed.
    val recipientsByViewType =
      rootHashMessages.groupBy(_.protocolMessage.viewType).mapFilter { rhms =>
        val recipients = rhms.flatMap(_.recipients.allRecipients).toSet
        val participantRecipients = recipients.collect[Member] { case p: ParticipantId => p }
        NonEmpty.from(participantRecipients.toSeq)
      }
    if (recipientsByViewType.nonEmpty) {
      for {
        snapshot <- crypto.awaitSnapshot(requestId.unwrap)
        envs <- recipientsByViewType.toSeq
          .parTraverse { case (viewType, flatRecipients) =>
            // This is currently a bit messy. We need to a TransactionResultMessage or TransferXResult whenever possible,
            // because that allows us to easily intercept and change the verdict in tests.
            // However, in some cases, the required information is not available, so we fall back to MalformedMediatorRequestResult.
            // TODO(i11326): Remove unnecessary fields from the result message types, so we can get rid of MalformedMediatorRequestResult and simplify this code.
            //  Afterwards, consider to unify this with the code in DefaultVerdictSender.
            val rejection = (viewType match {
              case ViewType.TransactionViewType =>
                requestO match {
                  case Some(request @ InformeeMessage(_)) =>
                    request.createMediatorResult(
                      requestId,
                      rejectionReason,
                      Set.empty,
                    )
                  // For other kinds of request, or if the request is unknown, we send a generic result
                  case _ =>
                    MalformedMediatorRequestResult(
                      requestId,
                      domainId,
                      viewType,
                      rejectionReason,
                      protocolVersion,
                    )
                }

              case ViewType.TransferInViewType =>
                TransferInResult.create(
                  requestId,
                  Set.empty,
                  TransferInDomainId(domainId),
                  rejectionReason,
                  protocolVersion,
                )
              case ViewType.TransferOutViewType =>
                TransferOutResult.create(
                  requestId,
                  Set.empty,
                  TransferOutDomainId(domainId),
                  rejectionReason,
                  protocolVersion,
                )
              case _: ViewType =>
                MalformedMediatorRequestResult(
                  requestId,
                  domainId,
                  viewType,
                  rejectionReason,
                  protocolVersion,
                )
            }): MediatorResult

            val recipients = Recipients.groups(flatRecipients.map(r => NonEmpty(Set, r)))

            SignedProtocolMessage
              .trySignAndCreate(rejection, snapshot, protocolVersion)
              .map(_ -> recipients)
          }
        batch = Batch.of(protocolVersion, envs *)
        _ <- verdictSender.sendResultBatch(requestId, batch, decisionTime)
      } yield ()
    } else Future.unit
  }

  def processResponse(
      ts: CantonTimestamp,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      signedResponse: SignedProtocolMessage[MediatorResponse],
  )(implicit traceContext: TraceContext): Future[Unit] =
    withSpan("ConfirmationResponseProcessor.processResponse") { implicit traceContext => span =>
      span.setAttribute("timestamp", ts.toString)
      span.setAttribute("counter", counter.toString)
      val response = signedResponse.message

      (for {
        snapshot <- OptionT.liftF(crypto.awaitSnapshot(response.requestId.unwrap))
        _ <- signedResponse
          .verifySignature(snapshot, response.sender)
          .leftMap(err =>
            MediatorError.MalformedMessage
              .Reject(
                s"$domainId (requestId: $ts): invalid signature from ${response.sender} with $err",
                protocolVersion,
              )
              .report()
          )
          .toOption
        _ <-
          if (signedResponse.domainId == domainId) OptionT.some[Future](())
          else {
            MediatorError.MalformedMessage
              .Reject(
                s"Request ${response.requestId}, sender ${response.sender}: Discarding mediator response for wrong domain ${signedResponse.domainId}",
                protocolVersion,
              )
              .report()
            OptionT.none[Future, Unit]
          }

        _ <-
          if (ts <= participantResponseDeadline) OptionT.some[Future](())
          else {
            logger.warn(
              s"Response ${ts} is too late as request ${response.requestId} has already exceeded the participant response deadline [$participantResponseDeadline]"
            )
            OptionT.none[Future, Unit]
          }

        responseAggregation <- mediatorState.fetch(response.requestId).orElse {
          // This can happen after a fail-over or as part of an attack.
          val cause =
            s"Received a mediator response at $ts by ${response.sender} with an unknown request id ${response.requestId}. Discarding response..."
          val error = MediatorError.InvalidMessage.Reject.create(cause, protocolVersion)
          error.log()

          OptionT.none
        }
        nextResponseAggregation <- responseAggregation.progress(ts, response, snapshot.ipsSnapshot)
        _unit <- mediatorState.replace(responseAggregation, nextResponseAggregation)
      } yield {
        // we can send the result asynchronously, as there is no need to reply in
        // order and there is no need to guarantee delivery of verdicts
        FutureUtil.doNotAwait(
          performUnlessClosingF("send-result-if-done")(
            sendResultIfDone(nextResponseAggregation, decisionTime)
          ).onShutdown(()),
          s"send-result-if-done failed for request ${response.requestId}",
          level = Level.WARN,
        )
      }).value.map(_ => ())
    }

  private def sendResultIfDone(
      responseAggregation: ResponseAggregation,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    responseAggregation match {
      case ResponseAggregation(requestId, request, _, Left(verdict)) =>
        verdictSender.sendResult(requestId, request, verdict, decisionTime)
      case ResponseAggregation(_, _, _, Right(_)) =>
        /* no op */
        Future.unit
    }
}
