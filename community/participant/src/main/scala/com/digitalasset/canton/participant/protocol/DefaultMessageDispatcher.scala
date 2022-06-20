// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.Monoid
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.MessageDispatcher.RequestProcessors
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError, SignedContent}
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HandlerResult,
  OrdinaryProtocolEvent,
  PossiblyIgnoredProtocolEvent,
}
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.TimeProof
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.Thereafter.syntax._
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class DefaultMessageDispatcher(
    override protected val domainId: DomainId,
    override protected val participantId: ParticipantId,
    override protected val requestTracker: RequestTracker,
    override protected val requestProcessors: RequestProcessors,
    override protected val tracker: SingleDomainCausalTracker,
    override protected val topologyProcessor: (
        SequencerCounter,
        CantonTimestamp,
        Traced[List[DefaultOpenEnvelope]],
    ) => HandlerResult,
    override protected val acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
    override protected val requestCounterAllocator: RequestCounterAllocator,
    override protected val recordOrderPublisher: RecordOrderPublisher,
    override protected val badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
    override protected val repairProcessor: RepairProcessor,
    override protected val inFlightSubmissionTracker: InFlightSubmissionTracker,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext, tracer: Tracer)
    extends MessageDispatcher
    with Spanning
    with NamedLogging {

  import MessageDispatcher._

  override protected type ProcessingResult = Unit

  override implicit val processingResultMonoid: Monoid[ProcessingResult] = {
    import cats.instances.unit._
    Monoid[Unit]
  }

  private def runAsyncResult(
      run: FutureUnlessShutdown[AsyncResult]
  ): FutureUnlessShutdown[ProcessingResult] =
    run.flatMap(_.unwrap)

  override protected def doProcess[A](
      kind: MessageKind[A],
      run: => FutureUnlessShutdown[A],
  ): FutureUnlessShutdown[ProcessingResult] = {
    import MessageDispatcher._
    // Explicitly enumerate all cases for type safety
    kind match {
      case TopologyTransaction => runAsyncResult(run)
      case AcsCommitment => run
      case CausalityMessageKind => run
      case MalformedMessage => run
      case UnspecifiedMessageKind => run
      case MalformedMediatorRequestMessage => run
      case RequestKind(_) => run
      case ResultKind(_) => run
      case DeliveryMessageKind => run
    }
  }

  override def handleAll(tracedEvents: Traced[Seq[PossiblyIgnoredProtocolEvent]]): HandlerResult =
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      for {
        _observeSequencing <- observeSequencing(
          events.collect { case e: OrdinaryProtocolEvent => e.signedEvent.content }
        )
        result <- MonadUtil.sequentialTraverseMonoid(events)(handle)
      } yield result
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def handle(event: PossiblyIgnoredProtocolEvent): HandlerResult = {
    implicit val traceContext: TraceContext = event.traceContext

    def tickTrackers(
        sc: SequencerCounter,
        ts: CantonTimestamp,
        triggerAcsChangePublication: Boolean,
    ): FutureUnlessShutdown[Unit] =
      for {
        // Signal to the identity processor that all messages up to timestamp `ts` have arrived
        // Publish the empty ACS change only afterwards as this may trigger an ACS commitment computation which accesses the topology state.
        _unit <- runAsyncResult(topologyProcessor(sc, ts, Traced(List.empty)))
      } yield {
        // Make sure that the tick is not lost
        requestTracker.tick(sc, ts)
        if (triggerAcsChangePublication)
          recordOrderPublisher.scheduleEmptyAcsChangePublication(sc, ts)
        recordOrderPublisher.tick(sc, ts)
      }

    withSpan(s"MessageDispatcher.handle") { implicit traceContext => _ =>
      val future = event match {
        case ordinaryEvent @ OrdinarySequencedEvent(signedEvent) =>
          signedEvent.content match {
            case Deliver(sc, ts, _, _, _) if TimeProof.isTimeProofEvent(ordinaryEvent) =>
              tickTrackers(sc, ts, triggerAcsChangePublication = true)
            case Deliver(sc, ts, _, _, _) =>
              processBatch(signedEvent.asInstanceOf[SignedContent[Deliver[DefaultOpenEnvelope]]])
                .thereafter { result =>
                  result.failed.foreach(ex => logger.error("event processing failed.", ex))
                  // Make sure that the tick is not lost unless we're shutting down
                  if (result != Success(UnlessShutdown.AbortedDueToShutdown))
                    requestTracker.tick(sc, ts)
                }
            case error @ DeliverError(sc, ts, _, _, _) =>
              logger.debug(s"Received a deliver error at ${sc} / ${ts}")
              for {
                _unit <- observeDeliverError(error)
                _unit <- tickTrackers(sc, ts, triggerAcsChangePublication = false)
              } yield ()
          }
        case SequencedEventStore.IgnoredSequencedEvent(ts, sc, _) =>
          tickTrackers(sc, ts, triggerAcsChangePublication = false)
      }
      HandlerResult.synchronous(future)
    }(traceContext, tracer)
  }

  @VisibleForTesting
  override def flush(): Future[Unit] = Future.unit
}
