// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.kernel.Monoid
import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.handlers.StripSignature
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HandlerResult,
  OrdinaryProtocolEvent,
  UnsignedProtocolEventHandler,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** The [[MediatorEventsProcessor]] divides incoming events into a series of stages where each stage should be executed
  * sequentially but the actions in the stage itself may execute in parallel.
  *
  * All mediator request related events that can be processed concurrently grouped by requestId.
  */
private[mediator] case class MediatorEventStage(
    requests: NonEmpty[Map[RequestId, NonEmpty[Seq[Traced[MediatorEvent]]]]]
) {
  def mergeNewEvents(newEvents: MediatorEventStage): MediatorEventStage = {
    val mergedRequests = newEvents.requests.foldLeft(requests) {
      case (oldRequests, (requestId, newRequests)) =>
        oldRequests.updated(
          requestId,
          requests.get(requestId).fold(newRequests)(old => old ++ newRequests),
        )
    }
    copy(
      requests = mergedRequests
    )
  }
}

private[mediator] object MediatorEventStage {

  def apply(
      events: NonEmpty[Seq[MediatorEvent]]
  )(implicit traceContext: TraceContext): MediatorEventStage = {
    val eventsByRequestId = events.map(Traced(_)).groupBy(_.value.requestId)
    MediatorEventStage(eventsByRequestId)
  }

}

/** Attempt to process a sequence of sequential events from the sequencer for the mediator in an optimal manner.
  * We could correctly process them sequentially however this is suboptimal.
  * We can parallelize their processing by respecting the following rules:
  *  - TODO(soren) Only the active mediator for the domain can send messages to sequencer, and this active state could change
  *    within the events we are processing. So this must be determined before processing any Mediator requests.
  *  - Mediator requests/responses with different request ids can be processed in parallel.
  *    For events referencing the same request-id we can provide these to the confirmation request processor as a group
  *    so it can make optimizations such as deferring persistence of a response state until the final message
  *    to avoid unnecessary database writes.
  *  - Identity transactions must be processed by the identity client before subsequent mediator request/responses as
  *    the confirmation response processor may require knowing the latest relevant topology state.
  *  - Pending mediator requests could timeout during the execution of this batch and should be handled with the timestamp
  *    of the event from the sequencer that caused them to timeout (it is tempting to just use the last timestamp to
  *    determine timeouts however we would like to ensure we use the closest timestamp to ensure a consistent version
  *    is applied across Mediators regardless of the batches of events they process). Unlikely however technically
  *    possible is that requests that are created while processing these events could also timeout due to sequencer
  *    time passing within this event range (think a low timeout value with a sequencer that is
  *    catching up so a long period could elapse even during a short range of events).
  *
  * Crashes can occur at any point during this processing (or even afterwards as it's the persistence in the sequencer
  * client that would move us to following events). Processing should be effectively idempotent to handle this.
  */
private[mediator] class MediatorEventsProcessor(
    state: MediatorState,
    crypto: DomainSyncCryptoClient,
    identityClientEventHandler: UnsignedProtocolEventHandler,
    handleMediatorEvents: (
        RequestId,
        Seq[Traced[MediatorEvent]],
        TraceContext,
    ) => HandlerResult,
    protocolVersion: ProtocolVersion,
    deduplicator: MediatorEventDeduplicator,
    readyCheck: MediatorReadyCheck,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def handle(events: Seq[OrdinaryProtocolEvent])(implicit
      traceContext: TraceContext
  ): HandlerResult =
    NonEmpty.from(events).fold(HandlerResult.done)(handle)

  private def handle(
      events: NonEmpty[Seq[OrdinaryProtocolEvent]]
  )(implicit traceContext: TraceContext): HandlerResult = {
    val identityF = StripSignature(identityClientEventHandler)(Traced(events))

    val envelopesByEvent = envelopesGroupedByEvent(events)

    for {
      deduplicatorResult <- FutureUnlessShutdown.outcomeF(
        deduplicator.rejectDuplicates(envelopesByEvent)
      )
      (uniqueEnvelopesByEvent, storeF) = deduplicatorResult

      determinedStages <- FutureUnlessShutdown.outcomeF(determineStages(uniqueEnvelopesByEvent))
      (hasIdentityUpdates, stages) = determinedStages
      _ <- MonadUtil.sequentialTraverseMonoid(stages)(executeStage(traceContext))

      resI <- identityF
    } yield {
      // reset the ready check if there was an identity update.
      if (hasIdentityUpdates)
        readyCheck.reset()
      resI.andThenF(_ => FutureUnlessShutdown.outcomeF(storeF))
    }
  }

  private def executeStage(traceContext: TraceContext)(stage: MediatorEventStage): HandlerResult = {
    for {
      result <- stage.requests.forgetNE.toSeq.parTraverse { case (requestId, events) =>
        handleMediatorEvents(requestId, events, traceContext)
      } map Monoid[AsyncResult].combineAll
    } yield result
  }

  /** Keep track of the stages we've accumulated alongside requests that are still pending. */
  private case class EventProcessingStages(
      pendingRequests: List[RequestId],
      stages: List[MediatorEventStage] = List.empty,
      hasIdentityUpdate: Boolean = false,
  ) {

    def withIdentityUpdate(
        foundIdentityUpdate: => Boolean
    ): MediatorEventsProcessor.this.EventProcessingStages =
      copy(hasIdentityUpdate = hasIdentityUpdate || foundIdentityUpdate)

    def addStage(stage: MediatorEventStage): EventProcessingStages =
      addStage(stage, pendingRequests)

    private def addStage(
        mediatorEvents: MediatorEventStage,
        pendingRequests: List[RequestId],
    ) = {
      // we add new requests to our pending requests collection in case they timeout during bounds of this batch of events
      val newRequests = mediatorEvents.requests.collect {
        case (requestId, events) if containsRequest(events) => requestId
      }
      // ideally we merge this into the prior stage if possible
      stages.lastOption match {
        case Some(priorEvents: MediatorEventStage) =>
          // merge these new mediator events into the last mediator stage rather than appending a new sequential stage
          val mergedEvents = priorEvents.mergeNewEvents(mediatorEvents)
          val priorStages = stages.dropRight(1) // remove this stage from what we've accumulated
          val newStages = priorStages :+ mergedEvents // add the new merged stage on the end
          copy(stages = newStages, pendingRequests = pendingRequests ++ newRequests)
        case _ =>
          // the last stage wasn't of mediator events so we cannot merge these new requests into it
          // instead just append a new stage to be processed
          copy(stages = stages :+ mediatorEvents, pendingRequests = pendingRequests ++ newRequests)
      }
    }

    private def containsRequest(events: Seq[Traced[MediatorEvent]]): Boolean =
      events.exists(_.value match {
        case _: MediatorEvent.Request => true
        case _ => false
      })

    private def hasRequestTimedOut(pendingRequestId: RequestId, timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[Boolean] =
      crypto.ips
        .awaitSnapshot(pendingRequestId.unwrap)
        .flatMap(_.findDynamicDomainParametersOrDefault(protocolVersion))
        .map { domainParameters =>
          val requestTimeout =
            pendingRequestId.unwrap.plus(domainParameters.participantResponseTimeout.unwrap)

          timestamp.isAfter(requestTimeout)
        }

    /** Has the given sequencer time caused any of our pending events to time out?
      * If so add them as MediatorEvents to be processed.
      */
    def addTimeouts(
        counter: SequencerCounter,
        timestamp: CantonTimestamp,
    )(implicit traceContext: TraceContext): Future[EventProcessingStages] =
      pendingRequests
        .parTraverse { requestID =>
          hasRequestTimedOut(requestID, timestamp).map(Either.cond(_, requestID, requestID))
        }
        .map(_.separate)
        .map { case (stillPendingRequests, timedOutRequests) =>
          NonEmpty.from(timedOutRequests).fold(this) { timedOutRequests =>
            val timeoutEvents = timedOutRequests
              .map(requestId => MediatorEvent.Timeout(counter, timestamp, requestId))

            addStage(MediatorEventStage(timeoutEvents), stillPendingRequests)
          }
        }

    def complete: (Boolean, List[MediatorEventStage]) = (hasIdentityUpdate, stages)
  }

  private def envelopesGroupedByEvent(
      events: NonEmpty[Seq[OrdinaryProtocolEvent]]
  ): NonEmpty[Seq[(OrdinaryProtocolEvent, Seq[OpenEnvelope[ProtocolMessage]])]] =
    events.map { event =>
      implicit val traceContext: TraceContext = event.traceContext
      event.signedEvent.content match {
        case deliver: Deliver[DefaultOpenEnvelope] =>
          val domainEnvelopes = ProtocolMessage.filterDomainsEnvelopes(
            deliver.batch,
            deliver.domainId,
            (wrongMessages: List[DefaultOpenEnvelope]) => {
              val wrongDomainIds = wrongMessages.map(_.protocolMessage.domainId)
              logger.error(s"Received messages with wrong domain ids: $wrongDomainIds")
            },
          )
          (event, domainEnvelopes)

        case _: DeliverError => (event, Seq.empty)
      }
    }

  private def determineStages(
      envelopesByEvent: Seq[(OrdinaryProtocolEvent, Seq[OpenEnvelope[ProtocolMessage]])]
  ): Future[(Boolean, List[MediatorEventStage])] = {
    NonEmpty
      .from(envelopesByEvent)
      .fold(Future.successful((false, List.empty[MediatorEventStage]))) { envelopesByEventNE =>
        // work out requests that will timeout during this range of events
        // (keep in mind that they may receive a result during this time, in which case the timeout will be ignored)
        val (lastEvent, _) = envelopesByEventNE.last1
        val unfinalized = state.pendingRequestIdsBefore(lastEvent.timestamp)

        val stagesF =
          envelopesByEventNE.foldLeft(Future.successful(EventProcessingStages(unfinalized))) {
            case (stages, (event, envelopes)) =>
              implicit val traceContext: TraceContext = event.traceContext
              for {
                stages <- extractMediatorEventsStage(
                  event.counter,
                  event.timestamp,
                  envelopes,
                ).toList
                  .foldLeft(
                    stages
                  ) { case (acc, stage) => acc.map(_.addStage(stage)) }

                stagesWithTimeouts <- stages.addTimeouts(event.counter, event.timestamp)
              } yield stagesWithTimeouts.withIdentityUpdate(
                envelopes
                  .mapFilter(ProtocolMessage.select[DomainTopologyTransactionMessage])
                  .nonEmpty
              )
          }
        stagesF.map(_.complete)
      }
  }

  private def extractMediatorEventsStage(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      envelopes: Seq[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): Option[MediatorEventStage] = {
    val requests = envelopes.mapFilter(ProtocolMessage.select[MediatorRequest])
    val responses =
      envelopes.mapFilter(ProtocolMessage.select[SignedProtocolMessage[MediatorResponse]])

    val events: Seq[MediatorEvent] = if (requests.nonEmpty && responses.nonEmpty) {
      logger.error("Received both mediator requests and mediator responses.")
      Seq.empty
    } else if (requests.nonEmpty) {
      requests match {
        case Seq(request) =>
          val rootHashMessages =
            envelopes.mapFilter(
              ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
            )
          Seq(
            MediatorEvent.Request(
              counter,
              timestamp,
              request.protocolMessage,
              rootHashMessages.toList,
            )
          )

        case _ =>
          logger.error("Received more than one mediator request.")
          Seq.empty
      }
    } else if (responses.nonEmpty) {
      responses.map(res => MediatorEvent.Response(counter, timestamp, res.protocolMessage))
    } else Seq.empty

    NonEmpty.from(events).map(MediatorEventStage(_))
  }
}

private[mediator] object MediatorEventsProcessor {
  def apply(
      state: MediatorState,
      crypto: DomainSyncCryptoClient,
      identityClientEventHandler: UnsignedProtocolEventHandler,
      confirmationResponseProcessor: ConfirmationResponseProcessor,
      mediatorEventDeduplicator: MediatorEventDeduplicator,
      protocolVersion: ProtocolVersion,
      readyCheck: MediatorReadyCheck,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): MediatorEventsProcessor = {
    new MediatorEventsProcessor(
      state,
      crypto,
      identityClientEventHandler,
      confirmationResponseProcessor.handleRequestEvents,
      protocolVersion,
      mediatorEventDeduplicator,
      readyCheck,
      loggerFactory,
    )
  }

}
