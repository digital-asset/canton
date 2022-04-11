// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.syntax.alternative._
import cats.syntax.functor._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton._
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoError}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.domain.mediator.MediatorMessageId.VerdictMessageId
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{
  AlarmStreamer,
  DynamicDomainParameters,
  RequestId,
  RootHash,
}
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.client.{
  SendCallback,
  SendResult,
  SendType,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Scalable service to check the received Stakeholder Trees and Confirmation Responses, derive a verdict and post
  * result messages to stakeholders.
  */
class ConfirmationResponseProcessor(
    domain: DomainId,
    private val mediatorId: MediatorId,
    crypto: DomainSyncCryptoClient,
    sequencer: SequencerClient,
    timeTracker: DomainTimeTracker,
    val mediatorState: MediatorState,
    alarmer: AlarmStreamer,
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
  ): HandlerResult = {
    val future = MonadUtil.sequentialTraverse_(events) {
      _.withTraceContext { implicit traceContext =>
        {
          case MediatorEvent.Request(counter, _, request, rootHashMessages) =>
            processRequest(requestId, counter, request, rootHashMessages)
          case MediatorEvent.Response(counter, timestamp, response) =>
            processResponse(timestamp, counter, response)
          case MediatorEvent.Timeout(_counter, timestamp, requestId) =>
            handleTimeout(requestId, timestamp)
        }
      }
    }
    HandlerResult.synchronous(FutureUnlessShutdown.outcomeF(future))
  }

  @VisibleForTesting
  private[mediator] def handleTimeout(
      requestId: RequestId,
      timestamp: CantonTimestamp,
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
      mediatorState.replace(responseAggregation, timeout).value.flatMap {
        case Left(e) =>
          logger.error(e.toString)
          Future.unit
        case Right(()) =>
          for {
            domainParameters <- crypto.ips
              .awaitSnapshot(requestId.unwrap)
              .flatMap(_.findDynamicDomainParametersOrDefault())

            _ <- sendResultIfDone(timeout, domainParameters)
          } yield ()
      }
    }
  }

  /** Stores the incoming request in the MediatorStore.
    * Sends a result message if no responses need to be received or if the request is malformed,
    * including if it declares a different mediator.
    */
  //TODO (i749) duplication check
  @VisibleForTesting
  private[mediator] def processRequest(
      requestId: RequestId,
      counter: SequencerCounter,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    withSpan("ConfirmationResponseProcessor.processRequest") { implicit traceContext => span =>
      span.setAttribute("request_id", requestId.toString)
      span.setAttribute("counter", counter.toString)

      def immediatelyReject(
          reject: MediatorReject,
          domainParameters: DynamicDomainParameters,
      ): Future[Unit] = {
        reject.logWithContext(Map("requestId" -> requestId.toString))
        sendMalformedRejection(requestId, rootHashMessages, reject, domainParameters)
      }

      def processNormally(domainParameters: DynamicDomainParameters): Future[Unit] = {
        val aggregation = ResponseAggregation(requestId, request)(loggerFactory)
        logger.debug(
          show"$requestId: registered informee message. Initial state: ${aggregation.state.showMerged}"
        )
        for {
          _ <- sendResultIfDone(aggregation, domainParameters)
          _ = if (!aggregation.isFinalized) {
            val timeoutAt = aggregation.requestId.unwrap.add(
              domainParameters.participantResponseTimeout.unwrap
            )
            timeTracker.requestTick(timeoutAt)
          }
          _ <- mediatorState.add(aggregation)
        } yield ()
      }

      crypto.ips.awaitSnapshot(requestId.unwrap).flatMap { topologySnapshot =>
        topologySnapshot.isMediatorActive(mediatorId).flatMap {
          case true =>
            topologySnapshot.findDynamicDomainParametersOrDefault().flatMap { domainParameters =>
              checkRootHashMessages(request, rootHashMessages, topologySnapshot).value.flatMap {
                case Left(rejectionReason) =>
                  immediatelyReject(
                    MediatorReject.Topology.InvalidRootHashMessages.Reject(rejectionReason),
                    domainParameters,
                  )
                case Right(_) =>
                  val declaredMediator = request.mediatorId
                  if (declaredMediator == mediatorId) {
                    processNormally(domainParameters)
                  } else {
                    // The mediator request was meant to be sent to a different mediator.
                    immediatelyReject(
                      MediatorReject.MaliciousSubmitter.WrongDeclaredMediator.Reject(
                        s"Declared mediator $declaredMediator is not the processing mediator $mediatorId"
                      ),
                      domainParameters,
                    )
                  }
              }
            }

          case false =>
            // TODO(M40) Decide whether we want the mediator to nevertheless analyze the message for maliciousness
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
          .traverse(topologySnapshot.activeParticipantsOf)
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
      domainParameters: DynamicDomainParameters,
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
          .traverse { case (viewType, recipients) =>
            val rejection =
              MalformedMediatorRequestResult(requestId, domain, viewType, rejectionReason)
            SignedProtocolMessage
              .tryCreate(rejection, snapshot, crypto.pureCrypto)
              .map { signedRejection =>
                signedRejection -> Recipients.groups(recipients.map(r => NonEmpty(Set, r)))
              }
          }
        batch = Batch.of(envs: _*)
        _ <- sendResultBatch(requestId, batch, domainParameters)
      } yield ()
    } else Future.unit
  }

  def processResponse(
      ts: CantonTimestamp,
      counter: SequencerCounter,
      signedResponse: SignedProtocolMessage[MediatorResponse],
  )(implicit traceContext: TraceContext): Future[Unit] =
    withSpan("ConfirmationResponseProcessor.processResponse") { implicit traceContext => span =>
      span.setAttribute("timestamp", ts.toString)
      span.setAttribute("counter", counter.toString)
      val response = signedResponse.message

      val bytes = response.getCryptographicEvidence
      val hash = crypto.pureCrypto.digest(response.hashPurpose, bytes)
      (for {
        snapshot <- EitherT.right(crypto.awaitSnapshot(response.requestId.unwrap))
        domainParameters <- EitherT.right(
          snapshot.ipsSnapshot.findDynamicDomainParametersOrDefault()
        )
        _ <- snapshot
          .verifySignature(hash, response.sender, signedResponse.signature)
          .leftMap(err => {
            alarmer
              .alarm(
                s"$domain (requestId: $ts): invalid signature from ${response.sender} with $err"
              )
              .discard
          })
        _ <- EitherT.cond[Future](
          signedResponse.domainId == domain,
          (),
          logger.warn(
            s"Request ${response.requestId}, sender ${response.sender}: Discarding mediator response for wrong domain ${signedResponse.domainId}"
          ),
        )
        _ <- EitherT.cond[Future](
          ts <= domainParameters.participantResponseDeadlineFor(response.requestId.unwrap),
          (),
          logger.warn(
            s"Response ${ts} is too late as request ${response.requestId} has already exceeded the participant response deadline [${domainParameters.participantResponseTimeout}]"
          ),
        )
        responseAggregation <- mediatorState.fetch(response.requestId).leftMap { _ =>
          //we assume that informee message has already been persisted in mediatorStorage before any participant responds
          logger.warn(s"${response.requestId} no corresponding request")
        }
        snapshot <- EitherT.right(crypto.ips.awaitSnapshot(response.requestId.unwrap))
        nextResponseAggregation <- responseAggregation
          .progress(ts, response, snapshot)
          .leftMap(_.alarm(response.sender, alarmer))
        _unit <- mediatorState
          .replace(responseAggregation, nextResponseAggregation)
          .leftMap(e => logger.error(e.toString))
        _ <- EitherT.right[Unit](sendResultIfDone(nextResponseAggregation, domainParameters))
      } yield ()).value.map(_ => ())
    }

  private def sendResultIfDone(
      responseAggregation: ResponseAggregation,
      domainParameters: DynamicDomainParameters,
  )(implicit traceContext: TraceContext): Future[Unit] =
    responseAggregation match {
      case ResponseAggregation(requestId, request, _, Left(verdict)) =>
        sendResult(requestId, request, verdict, domainParameters)
      case ResponseAggregation(_, _, _, Right(_)) =>
        /* no op */
        Future.unit
    }

  private def sendResult(
      requestId: RequestId,
      request: MediatorRequest,
      verdict: Verdict,
      domainParameters: DynamicDomainParameters,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val resultET = for {
      batch <- createResults(requestId, request, verdict)
      _ <- EitherT.right[SyncCryptoError](
        sendResultBatch(requestId, batch, domainParameters)
      )
    } yield ()

    // we don't want to halt the mediator if an individual send fails or if we're unable to create a batch, so just log
    resultET
      .leftMap(err => logger.warn(s"Failed to send verdict for $requestId: $err"))
      .value
      .map(_.merge)
  }

  private def sendResultBatch(
      requestId: RequestId,
      batch: Batch[DefaultOpenEnvelope],
      domainParameters: DynamicDomainParameters,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val decisionTime = domainParameters.decisionTimeFor(requestId.unwrap)

    val callback: SendCallback = {
      case SendResult.Success(_) =>
        logger.debug(s"Sent result for request ${requestId.unwrap}")
      case SendResult.Error(error) =>
        val reason = error.reason
        reason match {
          case _: DeliverErrorReason.BatchRefused =>
            logger.warn(
              s"Sequencing result was refused for request ${requestId.unwrap}: ${reason.toString}"
            )
          case _ =>
            logger.error(
              s"Failed to sequence result for request ${requestId.unwrap}: ${reason.toString}"
            )
        }
      case _: SendResult.Timeout =>
        logger.warn("Sequencing result timed out")
    }

    // the result of send request will be logged within the returned future however any error is effectively
    // discarded. Any error logged by the eventual callback will most likely occur after the returned future has
    // completed.
    // we use decision-time for max-sequencing-time as recipients will simply ignore the message if received after
    // that point.
    val sendET = sequencer.sendAsync(
      batch,
      SendType.Other,
      Some(requestId.unwrap),
      callback = callback,
      maxSequencingTime = decisionTime,
      messageId = VerdictMessageId(requestId).toMessageId,
    )

    EitherTUtil
      .logOnError(sendET, s"Failed to send result to sequencer for request ${requestId.unwrap}")
      .value
      .void
  }

  private def informeesGroupedByParticipant(requestId: RequestId, informees: Set[LfPartyId])(
      implicit traceContext: TraceContext
  ): Future[(Map[ParticipantId, Set[LfPartyId]], Set[LfPartyId])] = {

    def participantInformeeMapping(
        topologySnapshot: TopologySnapshot
    ): Future[(Map[ParticipantId, Set[LfPartyId]], Set[LfPartyId])] = {
      val start = Map.empty[ParticipantId, Set[LfPartyId]]
      val prefetchF =
        informees.toList.traverse(partyId =>
          topologySnapshot.activeParticipantsOf(partyId).map(res => (partyId, res))
        )
      prefetchF.map { fetched =>
        fetched.foldLeft((start, Set.empty[LfPartyId]))({
          case (
                (participantsToInformees, informeesNoParticipant),
                (informee, activeParticipants),
              ) =>
            val activeParticipantsOfInformee = activeParticipants.keySet
            if (activeParticipantsOfInformee.isEmpty)
              (participantsToInformees, informeesNoParticipant + informee)
            else {
              val updatedMap = activeParticipantsOfInformee.foldLeft(participantsToInformees)({
                case (map, participant) =>
                  map + (participant -> (map.getOrElse(participant, Set.empty) + informee))
              })
              (updatedMap, informeesNoParticipant)
            }
        })
      }
    }

    crypto.ips.awaitSnapshot(requestId.unwrap).flatMap(participantInformeeMapping)
  }

  private[this] def createResults(requestId: RequestId, request: MediatorRequest, verdict: Verdict)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SyncCryptoError, Batch[DefaultOpenEnvelope]] =
    for {
      informeesMapAndAnyInformeeNoActiveParticipant <- EitherT.right(
        informeesGroupedByParticipant(requestId, request.allInformees)
      )
      (informeesMap, informeesNoParticipant) = informeesMapAndAnyInformeeNoActiveParticipant
      snapshot <- EitherT.right(crypto.awaitSnapshot(requestId.unwrap))
      verdictWithInformeeCheck = {
        if (informeesNoParticipant.nonEmpty)
          Verdict.MediatorReject.Topology.InformeesNotHostedOnActiveParticipants
            .Reject(show"$informeesNoParticipant")
        else verdict
      }
      envelopes <- informeesMap
        .map { case (participantId, informees) =>
          val result = request.createMediatorResult(requestId, verdictWithInformeeCheck, informees)
          SignedProtocolMessage
            .create(result, snapshot, crypto.pureCrypto)
            .map(signedResult => OpenEnvelope(signedResult, Recipients.cc(participantId)))
        }
        .toList
        .sequence
    } yield Batch(envelopes)
}
