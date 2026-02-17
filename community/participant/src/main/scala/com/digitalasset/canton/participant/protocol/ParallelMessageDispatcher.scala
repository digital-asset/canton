// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monoid
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdownFactory}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.ConnectedSynchronizerMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.TicksAfter.{
  EventPublicationData,
  FutureEventPublication,
}
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{
  ParticipantTopologyProcessor,
  RequestProcessors,
  TickDecision,
  TickMoment,
  TicksAfter,
}
import com.digitalasset.canton.participant.protocol.ParallelMessageDispatcher.{
  NoEventPublication,
  Runner,
  tickDecisionAsynchronousMayTickRequestTracker,
  tickDecisionSynchronousMayTickTopologyProcessor,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionSynchronizerTracker
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HandlerResult,
  OrdinaryProtocolEvent,
  PossiblyIgnoredProtocolEvent,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
}
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, MonadUtil}
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Dispatches the incoming messages of the
  * [[com.digitalasset.canton.sequencing.client.SequencerClient]] to the different processors. It
  * also informs the
  * [[com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker]] about the
  * passing of time for messages that are not processed by the
  * [[com.digitalasset.canton.participant.protocol.ProtocolProcessor]].
  */
class ParallelMessageDispatcher(
    override protected val synchronizerId: PhysicalSynchronizerId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val topologyProcessor: ParticipantTopologyProcessor,
    override protected val trafficProcessor: TrafficControlProcessor,
    override protected val acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
    override protected val requestCounterAllocator: RequestCounterAllocator,
    override protected val recordOrderPublisher: RecordOrderPublisher,
    override protected val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
    override protected val inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTracker,
    processAsynchronously: ViewType => Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
    override val metrics: ConnectedSynchronizerMetrics,
    override protected val promiseFactory: PromiseUnlessShutdownFactory,
)(override implicit val ec: ExecutionContext, tracer: Tracer)
    extends MessageDispatcher
    with NamedLogging
    with Spanning {

  import MessageDispatcher.*

  override protected type ProcessingAsyncResult = AsyncResult[Unit]
  override protected def processingAsyncResultMonoid: Monoid[ProcessingAsyncResult] =
    Monoid[ProcessingAsyncResult]

  override protected def doProcess(
      kind: MessageKind
  ): ProcessingResult = {
    @inline def runSynchronously[A](run: () => FutureUnlessShutdown[A])(
        tickDecision: TickDecision,
        futureEventPublication: Option[FutureEventPublication],
    ): FutureUnlessShutdown[(AsyncResult[A], TicksAfter)] =
      run().map { result =>
        val asyncF = AsyncResult.pure(result)
        asyncF -> TicksAfter.fromTickDecision(tickDecision, asyncF, futureEventPublication)
      }

    @inline def runAsynchronously(run: () => HandlerResult): Runner =
      new Runner {
        override def apply(
            tickDecision: TickDecision,
            futureEventPublication: Option[FutureEventPublication],
        ): FutureUnlessShutdown[(AsyncResult[Unit], TicksAfter)] =
          run().map { result =>
            result -> TicksAfter.fromTickDecision(tickDecision, result, futureEventPublication)
          }
      }

    @inline def forceRunSynchronously(run: () => HandlerResult): Runner =
      new Runner {
        override def apply(
            tickDecision: TickDecision,
            futureEventPublication: Option[FutureEventPublication],
        ): FutureUnlessShutdown[(AsyncResult[Unit], TicksAfter)] = {
          val flattenedRun: () => FutureUnlessShutdown[Unit] =
            () => run().flatMap(_.unwrap)
          runSynchronously(flattenedRun)(tickDecision, futureEventPublication)
        }
      }

    // Explicitly enumerate all cases for type safety
    kind match {
      case TopologyTransaction(run) =>
        runAsynchronously(run)(tickDecisionSynchronousMayTickTopologyProcessor, NoEventPublication)
      case TrafficControlTransaction(run) =>
        // Traffic control messages are processed synchronously, so we can tick synchronously
        runSynchronously(run)(TickDecision.tickSynchronous, NoEventPublication)
      case AcsCommitment(run) =>
        runAsynchronously(run)(TickDecision.tickSynchronous, NoEventPublication)
      case MalformedMessage(run) =>
        runSynchronously(run)(TickDecision.tickSynchronous, NoEventPublication)
      case UnspecifiedMessageKind(run) =>
        runSynchronously(run)(TickDecision.tickSynchronous, NoEventPublication)
      case RequestKind(viewType, futureEventPublication, run) =>
        val runner =
          if (processAsynchronously(viewType)) runAsynchronously(run)
          else forceRunSynchronously(run)
        runner(
          tickDecisionAsynchronousMayTickRequestTracker,
          Some(FutureEventPublication(Some(futureEventPublication))),
        )
      case ResultKind(viewType, run) =>
        if (processAsynchronously(viewType))
          runAsynchronously(run)(tickDecisionAsynchronousMayTickRequestTracker, NoEventPublication)
        else
          forceRunSynchronously(run)(
            tickDecisionAsynchronousMayTickRequestTracker,
            NoEventPublication,
          )
      case DeliveryMessageKind(run) =>
        // TODO(#6914) Figure out the required synchronization to run this asynchronously.
        //  We must make sure that the observation of having been sequenced runs before the tick to the record order publisher.
        // Marton's note:
        //   with an external queue to ensure serial execution for these
        //   with generating an input async message kind for each of the event's in the batch which would be also put in to the single handling to wait for before ticking
        //   we could do this async
        runSynchronously(run)(TickDecision.tickSynchronous, NoEventPublication)
    }
  }

  override def handleAll(
      tracedEvents: Traced[Seq[WithOpeningErrors[PossiblyIgnoredProtocolEvent]]]
  ): HandlerResult =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      for {
        observeSequencingAndTicks <- observeSequencing(
          events.collect { case WithOpeningErrors(e: OrdinaryProtocolEvent, _) =>
            e.signedEvent.content
          }
        )
        (observeSequencing, ticks) = observeSequencingAndTicks
        process <- MonadUtil.sequentialTraverseMonoid(events)(handle(ticks, _))
      } yield Monoid.combine(observeSequencing, process)
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(
      ticksObserveSequencing: TicksAfter,
      eventE: WithOpeningErrors[PossiblyIgnoredProtocolEvent],
  ): HandlerResult = {
    implicit val traceContext: TraceContext = eventE.event.traceContext

    withSpan("MessageDispatcher.handle") { implicit traceContext => _ =>
      val processingResult: ProcessingResult = eventE.event match {
        case OrdinarySequencedEvent(sequencerCounter, signedEvent) =>
          val signedEventE = eventE.map(_ => signedEvent)
          processOrdinary(sequencerCounter, signedEventE)

        case _: IgnoredSequencedEvent[?] =>
          pureProcessingResult
      }
      processingResult.map { case (asyncResult, ticksAfter) =>
        val ticksF = tickTrackers(
          sc = eventE.event.counter,
          ts = eventE.event.timestamp,
          ticksObserveSequencing.combine(ticksAfter),
        )
        asyncResult.flatMapFUS { (_: Unit) =>
          ticksF
        }
      }
    }(traceContext, tracer)
  }

  private def processOrdinary(
      sequencerCounter: SequencerCounter,
      signedEventE: WithOpeningErrors[SignedContent[SequencedEvent[DefaultOpenEnvelope]]],
  )(implicit traceContext: TraceContext): ProcessingResult =
    signedEventE.event.content match {
      case deliver @ Deliver(_pts, ts, _, _, _, _, _) if TimeProof.isTimeProofDeliver(deliver) =>
        logTimeProof(sequencerCounter, ts)
        FutureUnlessShutdown
          .lift(
            recordOrderPublisher.scheduleEmptyAcsChangePublication(sequencerCounter, ts)
          )
          .flatMap(_ => pureProcessingResult)

      case Deliver(_pts, ts, _, msgId, _, _, _) =>
        // TODO(#13883) Validate the topology timestamp
        if (signedEventE.hasNoErrors) {
          logEvent(sequencerCounter, ts, msgId, signedEventE.event)
        } else {
          logFaultyEvent(sequencerCounter, ts, msgId, signedEventE.map(_.content))
        }
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val deliverE =
          signedEventE.asInstanceOf[WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]]
        processBatch(sequencerCounter, deliverE)
          .transform {
            case success @ Success(_) => success

            case Failure(ex) =>
              logger.error("Synchronous event processing failed.", ex)
              // Do not tick anything as the subscription will close anyway
              // and there is no guarantee that the exception will happen again during a replay.
              Failure(ex)
          }

      case error @ DeliverError(_pts, ts, _, msgId, status, _) =>
        logDeliveryError(sequencerCounter, ts, msgId, status)
        observeDeliverError(error)
    }

  private def tickTrackers(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      ticksAfter: TicksAfter,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def topologyProcessorTickAsyncF(): HandlerResult =
      ticksAfter.tickTopologyProcessorAfter.flatMap((_: Unit) =>
        topologyProcessor(sc, SequencedTime(ts), None, Traced(List.empty))
      )

    def recordOrderPublisherTickF(): FutureUnlessShutdown[Unit] =
      ticksAfter.tickRecordOrderPublisherAfter.flatMap { futureEventPublication =>
        futureEventPublication.futureEventO match {
          case None =>
            val event = sequencerIndexMovedEvent(ts)
            recordOrderPublisher.tick(event, sc, None).onShutdown {
              logger.debug("Skipping record order publisher tick due to shutdown.")
            }
          case Some(futureEvent) =>
            val tickF =
              futureEvent.subflatMap { case EventPublicationData(event, rc) =>
                recordOrderPublisher.tick(event, sc, Some(rc))
              }
            // We cannot await the ticking of the record order publisher here because the ticking happens only
            // when the request is decided, i.e., after a mediator verdict arrives or whan a timeout happens.
            // This decision therefore is triggered by a later sequenced event. If we were to await this here,
            // message processing could deadlock as we allow only a bounded number of concurrently processed
            // sequenced events.
            //
            // The memory constraints are enforced by the same mechanism as for the in-flight requests:
            // The decision time is bounded and from this we can derive a bound on the number of in-flight
            // requests via the bound on the rate of requests.
            FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
              tickF,
              s"ticking record order publisher for ts=$ts / sc=$sc",
            )
        }
        // Incorporate backpressure signal from the indexer via the record order publisher into the protocol processing,
        // where it takes effect via the throttling of asynchronous processing.
        recordOrderPublisher.backpressure()
      }

    def requestTrackerTick(): FutureUnlessShutdown[Unit] =
      ticksAfter.tickRequestTrackerAfter.map { (_: Unit) =>
        requestTracker.tick(sc, ts)
      }

    for {
      // Signal to the topology processor that all messages up to timestamp `ts` have arrived
      // Publish the tick only afterwards as this may trigger an ACS commitment computation which accesses the topology state.
      topologyAsync <- topologyProcessorTickAsyncF()
      _ <- requestTrackerTick()
      // waiting for the topologyAsync as well before we tick the ROP for crash recovery
      _ <- topologyAsync.unwrap
      _ <- recordOrderPublisherTickF()
    } yield ()
  }
}

object ParallelMessageDispatcher {
  private sealed trait Runner {
    def apply(
        tickDecision: TickDecision,
        futureEventPublication: Option[FutureEventPublication],
    ): FutureUnlessShutdown[(AsyncResult[Unit], TicksAfter)]
  }

  private val NoEventPublication: Option[FutureEventPublication] = None

  private val tickDecisionSynchronousMayTickTopologyProcessor = TickDecision(
    tickTopologyProcessor = TickMoment.TickMayHappenDuringProcessing,
    tickRequestTracker = TickMoment.AfterSynchronous,
  )

  private val tickDecisionAsynchronousMayTickRequestTracker = TickDecision(
    tickTopologyProcessor = TickMoment.AfterSynchronous,
    tickRequestTracker = TickMoment.TickMayHappenDuringProcessing,
  )
}
