// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports.replay

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.signer.SyncCryptoSigner.SigningTimestampOverrides
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.{MetricValue, SequencerClientMetrics}
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.pool.{SequencerConnection, SequencerConnectionPool}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  SequencedEventHandler,
  SequencedSerializedEvent,
  SequencerClientRecorder,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, OptionUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.sdk.metrics.data.MetricData
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.DurationConverters.*
import scala.util.chaining.*

/** Replays previously recorded sends against the configured sequencer and using a real sequencer
  * client transport. Records the latencies/rates to complete the send itself, and latencies/rates
  * for an event that was caused by the send to be witnessed. These metrics are currently printed to
  * stdout. Sequencers are able to drop sends so to know when all sends have likely been sequenced
  * we simply wait for a period where no events are received for a configurable duration. This isn't
  * perfect as technically a sequencer could stall, however the inflight gauge will report a number
  * greater than 0 indicating that these sends have gone missing. Clients are responsible for
  * interacting with the transport to initiate a replay and wait for observed events to be idle. A
  * reference can be obtained to this transport component by waiting on the future provided in
  * [[ReplayAction.SequencerSends]]. This testing transport is very stateful and the metrics will
  * only make sense for a single replay, however currently multiple or even concurrent calls are not
  * prevented (just don't).
  */
trait ReplayClient extends FlagCloseableAsync {
  import ReplayClient.*

  /** @param sendRatePerSecond
    *   None means "as fast as possible".
    */
  def replay(
      sendParallelism: Int,
      sendRatePerSecond: Option[Int] = None,
      cycles: Int = 1,
  ): Future[SendReplayReport]

  def waitForIdle(
      duration: FiniteDuration,
      startFromTimestamp: Option[CantonTimestamp] = None, // start from the beginning
  ): Future[EventsReceivedReport]

  /** Dump the submission related metrics into a string for periodic reporting during the replay
    * test
    */
  def metricReport(snapshot: Seq[MetricData]): String
}

object ReplayClient {
  final case class SendReplayReport(
      successful: Int = 0,
      overloaded: Int = 0,
      errors: Int = 0,
      shutdowns: Int = 0,
  )(
      sendDuration: => Option[java.time.Duration]
  ) {
    def update(
        result: UnlessShutdown[Either[SendAsyncClientError, Unit]]
    ): SendReplayReport =
      result match {
        case Outcome(Left(SendAsyncClientError.RequestRefused(error))) if error.isOverload =>
          copy(overloaded = overloaded + 1)
        case Outcome(Left(_)) => copy(errors = errors + 1)
        case Outcome(Right(_)) => copy(successful = successful + 1)
        case AbortedDueToShutdown => copy(shutdowns = shutdowns + 1)
      }

    def copy(
        successful: Int = this.successful,
        overloaded: Int = this.overloaded,
        errors: Int = this.errors,
        shutdowns: Int = this.shutdowns,
    ): SendReplayReport = SendReplayReport(successful, overloaded, errors, shutdowns)(sendDuration)

    lazy val total: Int = successful + overloaded + errors

    override def toString: String = {
      val durationSecsText = sendDuration.map(_.getSeconds).map(secs => s"${secs}s").getOrElse("?")
      s"Sent $total send requests in $durationSecsText ($successful successful, $overloaded overloaded, $errors problems)"
    }
  }

  final case class EventsReceivedReport(
      elapsedDuration: FiniteDuration,
      totalEventsReceived: Int,
      finishedAtTimestamp: Option[CantonTimestamp],
  ) {
    override def toString: String =
      s"Received $totalEventsReceived events within ${elapsedDuration.toSeconds}s, finished at sequencing timestamp $finishedAtTimestamp"
  }

}

class ReplayClientImpl(
    protocolVersion: ProtocolVersion,
    recordedPath: Path,
    replaySendsConfig: ReplayAction.SequencerSends,
    member: Member,
    connectionPool: SequencerConnectionPool,
    requestSigner: RequestSigner,
    syncCryptoApi: SyncCryptoApi,
    clock: Clock,
    metrics: SequencerClientMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends ReplayClient
    with NamedLogging
    with NoTracing {
  import ReplayClient.*

  private val pendingSends = TrieMap[MessageId, CantonTimestamp]()
  private val firstSend = new AtomicReference[Option[CantonTimestamp]](None)
  private val lastSend = new AtomicReference[Option[CantonTimestamp]](None)

  private lazy val submissionRequests: List[SubmissionRequest] =
    withNewTraceContext("load_submissions") { implicit traceContext =>
      logger.debug("Loading recorded submission requests")
      ErrorUtil.withThrowableLogging {
        SequencerClientRecorder.loadSubmissions(recordedPath, logger)
      }
    }

  // Signals to the tests that this transport is ready to interact with
  replaySendsConfig.publishReplayClient(this)

  private def sendDuration: Option[java.time.Duration] =
    OptionUtil
      .zipWith(firstSend.get().map(_.toInstant), lastSend.get().map(_.toInstant))(
        java.time.Duration.between
      )

  private def getConnection(requester: String): Either[String, SequencerConnection] =
    connectionPool
      .getConnections(
        requester,
        PositiveInt.one,
        exclusions = Set.empty,
      )
      .headOption
      .toRight("No connection available")

  private def replaySubmit(
      submission: SubmissionRequest,
      snapshot: SyncCryptoApi,
  ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] = {
    val startedAt = CantonTimestamp.now()
    // we'll correlate received events by looking at their message-id and calculate the
    // latency of the send by comparing now to the time the event eventually arrives
    pendingSends.put(submission.messageId, startedAt).discard

    // Picking a correct max sequencing time could be technically difficult,
    // so instead we pick max value, which ensures the sequencer always
    // attempts to sequence valid sends
    def extendMaxSequencingTime(submission: SubmissionRequest): SubmissionRequest =
      submission.updateMaxSequencingTime(maxSequencingTime = CantonTimestamp.MaxValue)

    def handleSendResult(
        result: Either[SendAsyncClientError, Unit]
    ): Either[SendAsyncClientError, Unit] =
      withEmptyMetricsContext { implicit metricsContext =>
        result.tap {
          case Left(SendAsyncClientError.RequestRefused(error)) if error.isOverload =>
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

    TraceContext.withNewTraceContext("replay_submit") { implicit traceContext =>
      val withExtendedMst = extendMaxSequencingTime(submission)
      val sendET = for {
        // We need a new signature because we've modified the max sequencing time.
        signedRequest <- requestSigner
          .signRequest(
            withExtendedMst,
            HashPurpose.SubmissionRequestSignature,
            snapshot,
            Some(
              SigningTimestampOverrides(
                // It's safe to re-sign with a new timestamp because the max sequencing time is set to `MaxValue`,
                // which causes a fallback to the long-term key when signing.
                approximateTimestamp = clock.now,
                validityPeriodEnd = Some(withExtendedMst.maxSequencingTime),
              )
            ),
          )
          .leftMap(error => SendAsyncClientError.RequestFailed(error))
        connection <- EitherT.fromEither[FutureUnlessShutdown](
          getConnection("replay-client-send").leftMap(SendAsyncClientError.RequestFailed.apply)
        )
        _ <- connection.sendAsync(
          signedRequest,
          replaySendsConfig.sendTimeout.toScala,
        )
      } yield ()

      sendET.value
        .map(handleSendResult)
        .map(updateTimestamps)
    }
  }

  override def replay(
      sendParallelism: Int,
      maybeSendRatePerSecond: Option[Int],
      cycles: Int,
  ): Future[SendReplayReport] =
    withNewTraceContext("replay") { implicit traceContext =>
      logger.info(s"Replaying ${submissionRequests.size} sends")

      val baseSource =
        Source
          .cycle(() => submissionRequests.iterator)
          .take(submissionRequests.size.toLong * cycles)

      val intermediateSource =
        maybeSendRatePerSecond match {
          case None =>
            baseSource
          case Some(sendRate) =>
            baseSource.throttle(sendRate, per = 1.second)
        }

      val submissionReplay =
        intermediateSource
          .mapAsyncUnordered(sendParallelism)(sr => replaySubmit(sr, syncCryptoApi).unwrap)
          .toMat(Sink.fold(SendReplayReport()(sendDuration))(_.update(_)))(Keep.right)

      PekkoUtil.runSupervised(
        submissionReplay,
        errorLogMessagePrefix = "Failed to run submission replay",
      )
    }

  override def waitForIdle(
      duration: FiniteDuration,
      startFromTimestamp: Option[CantonTimestamp] = None, // start from the beginning
  ): Future[EventsReceivedReport] = {
    val monitor = new SimpleIdlenessMonitor(startFromTimestamp, duration, timeouts, loggerFactory)

    monitor.idleF transform { result =>
      monitor.close()

      result
    }
  }

  /** Dump the submission related metrics into a string for periodic reporting during the replay
    * test
    */
  override def metricReport(snapshot: Seq[MetricData]): String = {
    val metricName = metrics.submissions.prefix.toString()
    val out = snapshot.flatMap {
      case metric if metric.getName.startsWith(metricName) => MetricValue.fromMetricData(metric)
      case _ => Seq.empty
    }
    out.show
  }

  private def subscribe(
      request: SubscriptionRequest,
      handler: SequencedEventHandler[NotUsed],
  ): Either[String, AutoCloseable] =
    for {
      connection <- getConnection("replay-client-subscribe")
      subscription <- connection.subscribe(request, handler, timeouts.network.duration)
    } yield subscription

  /** Monitor that when created subscribes the underlying transports and waits for Deliver or
    * DeliverError events to stop being observed for the given [[idlenessDuration]] (suggesting that
    * there are no more events being produced for the member).
    */
  private class SimpleIdlenessMonitor(
      readFrom: Option[CantonTimestamp],
      idlenessDuration: FiniteDuration,
      override protected val timeouts: ProcessingTimeout,
      protected val loggerFactory: NamedLoggerFactory,
  ) extends FlagCloseableAsync
      with NamedLogging {
    private case class State(
        startedAt: CantonTimestamp,
        lastEventAt: Option[CantonTimestamp],
        eventCounter: Int,
        lastSequencingTimestamp: Option[CantonTimestamp],
    )

    private val stateRef: AtomicReference[State] = new AtomicReference(
      State(
        startedAt = CantonTimestamp.now(),
        lastEventAt = None,
        eventCounter = 0,
        lastSequencingTimestamp = None,
      )
    )
    private val idleP = Promise[EventsReceivedReport]()

    private def scheduleCheck(): Unit =
      synchronizeWithClosingSync(functionFullName) {
        val nextCheckDuration =
          idlenessDuration.toJava.minus(durationFromLastEventToNow(stateRef.get()))
        val _ = materializer.scheduleOnce(nextCheckDuration.toScala, () => checkIfIdle())
      }.onShutdown(())

    scheduleCheck() // kick off checks

    private def updateLastDeliver(
        sequencingTimestamp: CantonTimestamp
    ): Unit = {
      val _ = stateRef.updateAndGet { case state @ State(_, _, eventCounter, _) =>
        state.copy(
          lastEventAt = Some(CantonTimestamp.now()),
          eventCounter = eventCounter + 1,
          lastSequencingTimestamp = Some(sequencingTimestamp),
        )
      }
    }

    @SuppressWarnings(Array("com.digitalasset.canton.ConcurrentMapSize"))
    private def checkIfIdle(): Unit = {
      val stateSnapshot = stateRef.get()
      val lastEventTime = stateSnapshot.lastEventAt.getOrElse(stateSnapshot.startedAt).toInstant
      val elapsedDuration =
        java.time.Duration.between(stateSnapshot.startedAt.toInstant, lastEventTime)
      val isIdle = durationFromLastEventToNow(stateSnapshot).compareTo(idlenessDuration.toJava) >= 0

      if (isIdle) {
        if (pendingSends.sizeIs > 0) {
          idleP
            .tryFailure(
              new IllegalStateException(s"There are ${pendingSends.size} pending send requests")
            )
            .discard
        } else {
          idleP
            .trySuccess(
              EventsReceivedReport(
                elapsedDuration.toScala,
                totalEventsReceived = stateSnapshot.eventCounter,
                finishedAtTimestamp = stateSnapshot.lastSequencingTimestamp,
              )
            )
            .discard
        }
      } else {
        scheduleCheck() // schedule the next check
      }
    }

    private def durationFromLastEventToNow(stateSnapshot: State) = {
      val from = stateSnapshot.lastEventAt.getOrElse(stateSnapshot.startedAt)
      java.time.Duration.between(from.toInstant, Instant.now())
    }

    private def updateMetrics(event: SequencedEvent[ClosedEnvelope]): Unit =
      withEmptyMetricsContext { implicit metricsContext =>
        val messageIdO: Option[MessageId] = event match {
          case Deliver(_, _, _, messageId, _, _, _) => messageId
          case DeliverError(_, _, _, messageId, _, _) => Some(messageId)
          case _ => None
        }

        messageIdO.flatMap(pendingSends.remove) foreach { sentAt =>
          val latency = java.time.Duration.between(sentAt.toInstant, Instant.now())
          metrics.submissions.inFlight.dec()
          metrics.submissions.sequencingTime.update(latency)
        }
      }

    private def handle(
        event: SequencedSerializedEvent
    ): FutureUnlessShutdown[Either[NotUsed, Unit]] = {
      val content = event.signedEvent.content

      updateMetrics(content)
      updateLastDeliver(content.timestamp)

      FutureUnlessShutdown.pure(Either.unit)
    }

    val idleF: Future[EventsReceivedReport] = idleP.future

    private val subscription =
      subscribe(SubscriptionRequest(member, readFrom, protocolVersion), handle).valueOr(err =>
        ErrorUtil.invalidState(err)
      )

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        SyncCloseable("idleness-subscription", subscription.close())
      )
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
    SyncCloseable("connection-pool", connectionPool.close())
  )
}
