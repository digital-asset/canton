// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.{EitherT, OptionT}
import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, RootHash, v0}
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

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
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with Spanning {

  /** Handle events for a single request-id.
    * Callers should ensure all events are for the same request and ordered by sequencer time.
    */
  def handleRequestEvents(
      requestId: RequestId,
      events: Seq[Traced[MediatorEvent]],
      callerTraceContext: TraceContext,
  ): HandlerResult = {
    val future = for {
      // FIXME(M40): do not block if requestId is far in the future
      snapshot <- crypto.ips.awaitSnapshot(requestId.unwrap)(callerTraceContext)
      domainParameters <- snapshot.findDynamicDomainParametersOrDefault(protocolVersion)(
        callerTraceContext
      )
      participantResponseDeadline =
        domainParameters.participantResponseDeadlineFor(requestId.unwrap)
      decisionTime = domainParameters.decisionTimeFor(requestId.unwrap)

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

      def immediatelyReject(reject: MediatorReject): Future[Unit] = {
        reject.logWithContext(Map("requestId" -> requestId.toString))
        sendMalformedRejection(requestId, rootHashMessages, reject, decisionTime)
      }

      def processNormally(): Future[Unit] = {
        val aggregation = ResponseAggregation(requestId, request, protocolVersion)(loggerFactory)
        logger.debug(
          show"$requestId: registered informee message. Initial state: ${aggregation.state.showMerged}"
        )
        for {
          _ <- sendResultIfDone(aggregation, decisionTime)
          _ = if (!aggregation.isFinalized) {
            timeTracker.requestTick(participantResponseDeadline)
          }
          _ <- mediatorState.add(aggregation)
        } yield ()
      }

      crypto.ips.awaitSnapshot(requestId.unwrap).flatMap { topologySnapshot =>
        topologySnapshot.isMediatorActive(mediatorId).flatMap {
          case true =>
            checkRootHashMessages(request, rootHashMessages, topologySnapshot).value.flatMap {
              case Left(rejectionReason) =>
                immediatelyReject(
                  MediatorError.InvalidMessage.Reject(
                    s"Rejected transaction due to invalid root hash messages: $rejectionReason",
                    v0.MediatorRejection.Code.InvalidRootHashMessage,
                  )(Verdict.protocolVersionRepresentativeFor(protocolVersion))
                )
              case Right(_) =>
                val declaredMediator = request.mediatorId
                if (declaredMediator == mediatorId) {
                  processNormally()
                } else {
                  // The mediator request was meant to be sent to a different mediator.
                  immediatelyReject(
                    MediatorError.MalformedMessage.Reject(
                      show"The declared mediator in the MediatorRequest ($declaredMediator) is not the mediator that received the request ($mediatorId).",
                      v0.MediatorRejection.Code.WrongDeclaredMediator,
                      protocolVersion,
                    )
                  )
                }
            }

          case false =>
            logger.info(show"Ignoring mediator request $requestId because I'm not active.")
            Future.unit
        }
      }
    }
  }

  private def checkRootHashMessages(
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      topologySnapshot: TopologySnapshot,
  ): EitherT[Future, String, Unit] = {
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

    case class WrongMembers(
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

    for {
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
        show"Different payloads in root hash messages: Sizes ${distinctPayloads.map(_.bytes.size)}",
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
  }

  private def sendMalformedRejection(
      requestId: RequestId,
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
        val recipients = rhms.flatMap(_.recipients.allRecipients).toSet - mediatorId
        NonEmpty.from(recipients.toSeq)
      }
    if (recipientsByViewType.nonEmpty) {
      for {
        snapshot <- crypto.awaitSnapshot(requestId.unwrap)
        envs <- recipientsByViewType.toSeq
          .parTraverse { case (viewType, recipients) =>
            val rejection =
              MalformedMediatorRequestResult(
                requestId,
                domainId,
                viewType,
                rejectionReason,
                protocolVersion,
              )
            SignedProtocolMessage
              .tryCreate(rejection, snapshot, protocolVersion)
              .map { signedRejection =>
                signedRejection -> Recipients.groups(recipients.map(r => NonEmpty(Set, r)))
              }
          }
        batch = Batch.of(protocolVersion, envs: _*)
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

      val bytes = response.getCryptographicEvidence
      val hash = crypto.pureCrypto.digest(response.hashPurpose, bytes)
      (for {
        snapshot <- OptionT.liftF(crypto.awaitSnapshot(response.requestId.unwrap))
        _ <- snapshot
          .verifySignature(hash, response.sender, signedResponse.signature)
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
          // We assume the informee message has already been persisted in mediatorStorage before any participant responds
          val cause =
            s"Received a mediator response at $ts by ${response.sender} with an unknown request id ${response.requestId}. Discarding response..."
          val alarm = MediatorError.MalformedMessage.Reject(cause, protocolVersion)
          alarm.report()

          OptionT.none
        }

        snapshot <- OptionT.liftF(crypto.ips.awaitSnapshot(response.requestId.unwrap))
        nextResponseAggregation <- responseAggregation.progress(ts, response, snapshot)
        _unit <- mediatorState.replace(responseAggregation, nextResponseAggregation)

        _ <- OptionT.liftF(sendResultIfDone(nextResponseAggregation, decisionTime))
      } yield ()).value.map(_ => ())
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
