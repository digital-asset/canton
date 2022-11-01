// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports.replay

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.data.EitherT
import com.codahale.metrics.{ConsoleReporter, MetricFilter}
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  SequencerClientRecorder,
  SerializedEventHandler,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.util.{AkkaUtil, ErrorUtil, OptionUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter}
import io.functionmeta.functionFullName

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.DurationConverters.*
import scala.util.chaining.*

/** Replays previously recorded sends against the configured sequencer and using a real sequencer client transport.
  * Records the latencies/rates to complete the send itself, and latencies/rates for an event that was caused by the send to be witnessed.
  * These metrics are currently printed to stdout.
  * Sequencers are able to drop sends so to know when all sends have likely been sequenced we simply wait for a period
  * where no events are received for a configurable duration. This isn't perfect as technically a sequencer could stall,
  * however the inflight gauge will report a number greater than 0 indicating that these sends have gone missing.
  * Clients are responsible for interacting with the transport to initiate a replay and wait for observed events to
  * be idle. A reference can be obtained to this transport component by waiting on the future provided in [[ReplayAction.SequencerSends]].
  * This testing transport is very stateful and the metrics will only make sense for a single replay,
  * however currently multiple or even concurrent calls are not prevented (just don't).
  */
class ReplayingSendsSequencerClientTransport(
    protocolVersion: ProtocolVersion,
    recordedPath: Path,
    replaySendsConfig: ReplayAction.SequencerSends,
    member: Member,
    underlyingTransport: SequencerClientTransport,
    metrics: SequencerClientMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends SequencerClientTransport
    with NamedLogging
    with NoTracing
    with FlagCloseableAsync {

  case class SendReplayReport(successful: Int = 0, overloaded: Int = 0, errors: Int = 0) {
    def update(result: Either[SendAsyncClientError, Unit]): SendReplayReport = result match {
      case Left(SendAsyncClientError.RequestRefused(_: SendAsyncError.Overloaded)) =>
        copy(overloaded = overloaded + 1)
      case Left(_) => copy(errors = errors + 1)
      case Right(_) => copy(successful = successful + 1)
    }

    lazy val total: Int = successful + overloaded + errors

    override def toString: String = {
      val durationSecsText = sendDuration.map(_.getSeconds).map(secs => s"${secs}s").getOrElse("?")
      s"Sent $total send requests in $durationSecsText ($successful successful, $overloaded overloaded, $errors errors)"
    }
  }

  case class EventsReceivedReport(
      elapsedDuration: FiniteDuration,
      totalEventsReceived: Int,
      finishedAtCounter: SequencerCounter,
  ) {
    override def toString: String =
      s"Received $totalEventsReceived events within ${elapsedDuration.toSeconds}s"
  }

  private val pendingSends = TrieMap[MessageId, CantonTimestamp]()
  private val firstSend = new AtomicReference[Option[CantonTimestamp]](None)
  private val lastSend = new AtomicReference[Option[CantonTimestamp]](None)
  private val lastReceivedEvent = new AtomicReference[Option[CantonTimestamp]](None)

  private val submissionRequests: List[SubmissionRequest] = withNewTraceContext {
    implicit traceContext =>
      logger.debug("Loading recorded submission requests")
      ErrorUtil.withThrowableLogging {
        SequencerClientRecorder.loadSubmissions(recordedPath, logger)
      }
  }

  // Signals to the tests that this transport is ready to interact with
  replaySendsConfig.publishTransport(this)

  private def sendDuration: Option[java.time.Duration] =
    OptionUtil
      .zipWith(firstSend.get().map(_.toInstant), lastSend.get().map(_.toInstant))(
        java.time.Duration.between
      )

  private def replaySubmit(
      submission: SubmissionRequest
  ): Future[Either[SendAsyncClientError, Unit]] = {
    val startedAt = CantonTimestamp.now()
    // we'll correlate received events by looking at their message-id and calculate the
    // latency of the send by comparing now to the time the event eventually arrives
    pendingSends.put(submission.messageId, startedAt).discard

    // Picking a correct max sequencing time could be technically difficult,
    // so instead we pick the biggest point in time that should ensure the sequencer always
    // attempts to sequence valid sends
    def extendMaxSequencingTime(submission: SubmissionRequest): SubmissionRequest =
      submission.copy(maxSequencingTime = CantonTimestamp.MaxValue)

    def handleSendResult(
        result: Either[SendAsyncClientError, Unit]
    ): Either[SendAsyncClientError, Unit] =
      withEmptyMetricsContext { implicit metricsContext =>
        result.tap {
          case Left(SendAsyncClientError.RequestRefused(_: SendAsyncError.Overloaded)) =>
            logger.warn(
              s"Sequencer is overloaded and rejected our send. Please tune the sequencer to handle more concurrent requests."
            )
            metrics.submissions.overloaded.inc()

          case Left(error) =>
            // log, increase error counter, then ignore
            logger.warn(s"Send request failed: $error")

          case Right(_) =>
            // we've successfully sent the send request
            metrics.submissions.inFlight.inc()
            val sentAt = CantonTimestamp.now()
            metrics.submissions.sends
              .update(java.time.Duration.between(startedAt.toInstant, sentAt.toInstant))
        }
      }

    def updateTimestamps[A](item: A): A = {
      val now = CantonTimestamp.now()
      // only set the first send timestamp if none have been resent
      firstSend.compareAndSet(None, Some(now))
      lastSend.set(Some(now))
      item
    }

    TraceContext.withNewTraceContext(traceContext =>
      underlyingTransport
        .sendAsync(
          extendMaxSequencingTime(submission),
          replaySendsConfig.sendTimeout.toScala,
          protocolVersion,
        )(traceContext)
        .value
        .map(handleSendResult)
        .map(updateTimestamps)
    )
  }

  def replay(sendParallelism: Int): Future[SendReplayReport] = withNewTraceContext {
    implicit traceContext =>
      logger.info(s"Replaying ${submissionRequests.size} sends")

      val submissionReplay = Source(submissionRequests)
        .mapAsyncUnordered(sendParallelism)(replaySubmit)
        .toMat(Sink.fold(SendReplayReport())(_.update(_)))(Keep.right)

      AkkaUtil.runSupervised(logger.error("Failed to run submission replay", _), submissionReplay)
  }

  /** How long did the replay take from first send sent to last event received. */
  def replayDuration: Option[java.time.Duration] = {
    for {
      firstSend <- firstSend.get()
      lastReceived <- lastReceivedEvent.get()
    } yield java.time.Duration.between(firstSend.toInstant, lastReceived.toInstant)
  }

  def waitForIdle(
      duration: FiniteDuration,
      startFromCounter: SequencerCounter = SequencerCounter.Genesis,
  ): Future[EventsReceivedReport] = {
    val monitor = new SimpleIdlenessMonitor(startFromCounter, duration, timeouts, loggerFactory)

    monitor.idleF transform { result =>
      monitor.close()

      result
    }
  }

  /** Dump the submission related metrics into a string for periodic reporting during the replay test */
  def metricReport: String =
    withResource(new ByteArrayOutputStream()) { os =>
      withResource(new PrintStream(os)) { ps =>
        withResource(
          ConsoleReporter
            .forRegistry(metrics.registry)
            .filter(MetricFilter.startsWith(metrics.submissions.prefix.toString()))
            .outputTo(ps)
            .build()
        ) { reporter =>
          reporter.report()
          ps.flush()
          os.toString()
        }
      }
    }

  /** Monitor that when created subscribes the underlying transports and waits for Deliver or DeliverError events
    * to stop being observed for the given [[idlenessDuration]] (suggesting that there are no more events being
    * produced for the member).
    */
  private class SimpleIdlenessMonitor(
      readFrom: SequencerCounter,
      idlenessDuration: FiniteDuration,
      override protected val timeouts: ProcessingTimeout,
      protected val loggerFactory: NamedLoggerFactory,
  ) extends FlagCloseableAsync
      with NamedLogging {
    private case class State(
        startedAt: CantonTimestamp,
        lastEventAt: CantonTimestamp,
        eventCounter: Int,
        lastCounter: SequencerCounter,
    )

    private val lastDeliverRef: AtomicReference[Option[State]] = new AtomicReference(None)
    private val idleP = Promise[EventsReceivedReport]()

    private def scheduleCheck(): Unit = {
      performUnlessClosing(functionFullName) {
        val elapsed = lastDeliverRef
          .get()
          .map(_.lastEventAt.toInstant)
          .map(java.time.Duration.between(_, Instant.now()))
          .getOrElse(java.time.Duration.ZERO)
        val nextCheckDuration = idlenessDuration.toJava.minus(elapsed)

        val _ = materializer.scheduleOnce(nextCheckDuration.toScala, () => checkIfIdle())
      }.onShutdown(())
    }

    scheduleCheck() // kick off checks

    private def updateLastDeliver(counter: SequencerCounter): Unit = {
      val _ = lastDeliverRef.updateAndGet {
        case None =>
          Some(
            State(
              startedAt = CantonTimestamp.now(),
              lastEventAt = CantonTimestamp.now(),
              eventCounter = 1,
              lastCounter = counter,
            )
          )
        case Some(state @ State(_, _, eventCounter, _)) =>
          Some(
            state.copy(
              lastEventAt = CantonTimestamp.now(),
              lastCounter = counter,
              eventCounter = eventCounter + 1,
            )
          )
      }
    }

    private def checkIfIdle(): Unit = {
      val isIdle = lastDeliverRef.get() exists {
        case State(_startedAt, lastEventAt, eventCounter, lastCounter) =>
          val elapsed =
            java.time.Duration.between(lastEventAt.toInstant, CantonTimestamp.now().toInstant)
          val isIdle = elapsed.compareTo(idlenessDuration.toJava) >= 0

          if (isIdle) {
            idleP
              .trySuccess(
                EventsReceivedReport(
                  elapsed.toScala,
                  totalEventsReceived = eventCounter,
                  finishedAtCounter = lastCounter,
                )
              )
              .discard
          }

          isIdle
      }

      if (!isIdle) scheduleCheck() // schedule the next check
    }

    private def updateMetrics(event: SequencedEvent[ClosedEnvelope]): Unit =
      withEmptyMetricsContext { implicit metricsContext =>
        val messageIdO: Option[MessageId] = event match {
          case Deliver(_, _, _, messageId, _) => messageId
          case DeliverError(_, _, _, messageId, _) => Some(messageId)
          case _ => None
        }

        messageIdO.flatMap(pendingSends.remove) foreach { sentAt =>
          val latency = java.time.Duration.between(sentAt.toInstant, Instant.now())
          metrics.submissions.inFlight.dec()
          metrics.submissions.sequencingTime.update(latency)
          lastReceivedEvent.set(Some(CantonTimestamp.now()))
        }
      }

    private def handle(event: OrdinarySerializedEvent): Future[Either[NotUsed, Unit]] = {
      val content = event.signedEvent.content

      updateMetrics(content)
      updateLastDeliver(content.counter)

      Future(Right(()))
    }

    val idleF: Future[EventsReceivedReport] = idleP.future

    private val subscription =
      underlyingTransport.subscribe(SubscriptionRequest(member, readFrom), handle)

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        SyncCloseable("idleness-subscription", subscription.close())
      )
  }

  /** We're replaying sends so shouldn't allow the app to send any new ones */
  override def sendAsync(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] = EitherT.rightT(())

  /** We're replaying sends so shouldn't allow the app to send any new ones */
  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    EitherT.rightT(())

  override def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] = EitherT.rightT(())

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] = new SequencerSubscription[E] {
    override protected def loggerFactory: NamedLoggerFactory =
      ReplayingSendsSequencerClientTransport.this.loggerFactory

    override protected def timeouts: ProcessingTimeout =
      ReplayingSendsSequencerClientTransport.this.timeouts

    override private[canton] def complete(reason: SubscriptionCloseReason[E])(implicit
        traceContext: TraceContext
    ): Unit = closeReasonPromise.trySuccess(reason).discard[Boolean]
  }

  override def subscribeUnauthenticated[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] = subscribe(request, handler)

  override def acknowledge(request: AcknowledgeRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.unit

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    SubscriptionErrorRetryPolicy.never

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    EitherT.rightT(HandshakeResponse.Success(protocolVersion))

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    SyncCloseable("underlying-transport", underlyingTransport.close())
  )

}
