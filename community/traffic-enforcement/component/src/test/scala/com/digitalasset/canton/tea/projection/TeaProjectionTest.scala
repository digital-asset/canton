// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.tea.projection.TrafficDelta.{creditBalanceDelta, debitBalanceDelta}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext, Traced, W3CTraceContext}
import com.digitalasset.nonempty.NonEmpty
import com.typesafe.config.{Config, ConfigFactory}
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{StatusCode, Tracer}
import io.opentelemetry.sdk.trace.data.SpanData
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.language.implicitConversions

/** Shared behaviour for the TEA ingestion projection tests
  */
trait TeaProjectionTest extends BaseTest { this: AnyWordSpec =>

  // Test convenience: let plain numeric literals stand in for NonNegativeLong balances.
  private implicit def intToNonNegativeLong(value: Int): NonNegativeLong =
    NonNegativeLong.tryCreate(value.toLong)

  /** A storage backend providing the store under test together with a factory that rebuilds a
    * projection writing into that same store. Calling [[Backend.newProjection]] more than once
    * simulates restarting the node on top of the same storage.
    */
  protected trait Backend {
    def store: TeaTrafficStore
    def newProjection(): TeaProjectionFactory
    def metricsFactory: InMemoryMetricsFactory
    def metrics: TrafficEnforcementMetrics
  }

  protected val testTelemetry: TestTelemetrySetup = new TestTelemetrySetup()
  // Shadows the non-reporting tracer from BaseTest so the factories under test export their spans.
  override protected implicit lazy val tracer: Tracer = testTelemetry.tracer

  protected val clock = new SimClock(loggerFactory = loggerFactory)

  /** Whether the backend persists offsets across projection restarts. The DB backends keep the
    * offset in the pekko offset store, the in-memory backend does not (it replays from the start
    * and relies on store-level deduplication).
    */
  protected def offsetsArePersisted: Boolean

  /** Whether all deltas of a projection event are committed in a single transaction. The DB
    * backends persist them together with the offset, the in-memory backend applies them one by one.
    */
  protected def deltasAreAppliedAtomically: Boolean

  /** Build a fresh backend bound to the given actor system. */
  protected def createBackend()(implicit system: ActorSystem[?]): Backend

  // Stable projection id so that the offset is keyed consistently across restarts.
  protected val projectionId: ProjectionId = ProjectionId("debit-ingestion", "grpc-stream")

  // Keep the restart backoff small so crash-recovery tests stay fast.
  private def pekkoTestConfig: Config =
    ConfigFactory
      .parseString(
        """pekko.projection.restart-backoff {
          |  min-backoff = 200 ms
          |  max-backoff = 1 s
          |  random-factor = 0.0
          |}
          |""".stripMargin
      )

  /** Run a test body with a dedicated actor system and a fresh backend, tearing everything down
    * afterwards.
    */
  protected def withProjection[A](body: (ActorTestKit, Backend) => A): A = {
    val testKit = ActorTestKit(s"tea-projection-${UUID.randomUUID()}", pekkoTestConfig)
    try body(testKit, createBackend()(testKit.system))
    finally testKit.shutdownTestKit()
  }

  protected def projectionEvent(
      offset: Long,
      timestamp: CantonTimestamp,
      accountDelta: AccountDelta,
  ): ProjectionEvent =
    ProjectionEvent.Single(offset, timestamp, accountDelta)

  /** A projection event holding several deltas, as produced for a transaction with several top-ups.
    */
  protected def multiDeltaEvent(
      offset: Long,
      timestamp: CantonTimestamp,
      firstNodeIdAndDelta: (Int, AccountDelta),
      otherNodeIdAndDeltas: (Int, AccountDelta)*
  ): ProjectionEvent =
    ProjectionEvent.Multiple(
      offset,
      timestamp,
      NonEmpty(Map, firstNodeIdAndDelta, otherNodeIdAndDeltas*),
    )

  private def tracedSource(
      events: Seq[ProjectionEvent],
      traceContext: TraceContext,
  ): Source[Traced[ProjectionEvent], NotUsed] =
    Source(events.toList)
      .map(event => Traced(event)(traceContext))
      // Keep the stream open after the (finite) test events so the projection idles in a steady
      // state instead of completing and being restarted in a loop.
      .concat(Source.never[Traced[ProjectionEvent]])

  /** A source factory that resumes from the offset it is handed and records every offset it is
    * asked to resume from.
    */
  protected def recordingSourceFactory(
      events: Seq[ProjectionEvent],
      resumeOffsets: ConcurrentLinkedQueue[Option[Long]] =
        new ConcurrentLinkedQueue[Option[Long]](),
      traceContext: TraceContext = TraceContext.empty,
  ): Option[Long] => Source[Traced[ProjectionEvent], NotUsed] = { maybeOffset =>
    resumeOffsets.add(maybeOffset)
    val fromExclusive = maybeOffset.getOrElse(Long.MinValue)
    tracedSource(events.filter(_.offset > fromExclusive), traceContext)
  }

  /** The exporter isn't reset between tests, so each case gives its events its own trace context
    * and then only looks at the spans under that trace id.
    */
  private def spansOn(traceContext: TraceContext): List[SpanData] =
    testTelemetry.reportedSpans().filter(span => traceContext.traceId.contains(span.getTraceId))

  private def spanAttribute(span: SpanData, key: String): String =
    span.getAttributes.get(AttributeKey.stringKey(s"canton.$key"))

  private def balanceOf(backend: Backend, account: AccountId): Option[AccountState] =
    backend.store
      .getBalance(account)
      .value
      .futureValueUS
      .fold(
        err => fail(s"expected the lookup to succeed, but it failed: $err"),
        identity,
      )

  private def eventsOf(
      backend: Backend,
      account: AccountId,
      from: CantonTimestamp,
  ): Seq[DeltaEvent] =
    backend.store.getEvents(account, from).futureValueUS

  private def spawnProjection(
      testKit: ActorTestKit,
      backend: Backend,
      sourceFactory: Option[Long] => Source[Traced[ProjectionEvent], NotUsed],
  ): ActorRef[ProjectionBehavior.Command] =
    testKit.spawn(
      backend
        .newProjection()
        .projection(projectionId, EventSource.LedgerAPICompletions, sourceFactory),
      s"projection-${UUID.randomUUID()}",
    )

  private def stopProjection(
      testKit: ActorTestKit,
      ref: ActorRef[ProjectionBehavior.Command],
  ): Unit =
    testKit.stop(ref, 30.seconds)

  private val alice = AccountId.tryCreate("alice")
  private val bob = AccountId.tryCreate("bob")

  implicit class AccountDeltaOps(account: AccountId) {
    def credit(value: Long): AccountDelta = AccountDelta(account, creditBalanceDelta(value))
    def debit(value: Long): AccountDelta = AccountDelta(account, debitBalanceDelta(value))
  }

  private lazy val multiDeltaTraceContext: TraceContext =
    W3CTraceContext("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").toTraceContext

  def gaugeValue(name: MetricName, backend: Backend) =
    backend.metricsFactory.metrics
      .gauges(name)
      .getOrElse(MetricsContext.Empty, sys.error(s"Gauge $name not found"))
      .getValue

  def eventGaugeValue(backend: Backend) =
    gaugeValue(backend.metrics.projectionTimestamp.info.name, backend)

  def offsetGaugeValue(backend: Backend) =
    gaugeValue(backend.metrics.projectionOffset.info.name, backend)

  def teaProjection(): Unit = {
    "TeaProjection" should {

      "ingest events from the source into the store" in withProjection { (testKit, backend) =>
        val t1 = clock.now
        val t2 = t1.immediateSuccessor
        val eventSource = EventSource.LedgerAPICompletions
        val events = Seq(
          projectionEvent(1L, t1, alice.credit(10)),
          projectionEvent(2L, t2, alice.debit(3)),
        )

        val ref = spawnProjection(
          testKit,
          backend,
          recordingSourceFactory(events, traceContext = nonEmptyTraceContext1),
        )
        try {
          eventually() {
            balanceOf(backend, alice) shouldBe Some(AccountState(alice, 3, 10, t2))
          }
          eventsOf(backend, alice, t1) should contain theSameElementsAs Seq(
            DeltaEvent(creditBalanceDelta(10), t1, eventSource),
            DeltaEvent(debitBalanceDelta(3), t2, eventSource),
          )
          eventGaugeValue(backend) shouldBe t2.toEpochMilli
          offsetGaugeValue(backend) shouldBe 2L
          eventually() {
            val spans = spansOn(nonEmptyTraceContext1)
            spans should have size 2
            forEvery(spans) { span =>
              span.getName shouldBe TeaProjectionFactory.ApplyDeltaSpanName
              Some(span.getParentSpanId) shouldBe nonEmptyTraceContext1.spanId
              span.getStatus.getStatusCode should not be StatusCode.ERROR
              spanAttribute(span, TeaProjectionFactory.AccountIdAttribute) shouldBe alice.unwrap
              spanAttribute(
                span,
                TeaProjectionFactory.EventSourceAttribute,
              ) shouldBe EventSource.LedgerAPICompletions.toString
            }
            spans.map(
              spanAttribute(_, TeaProjectionFactory.OffsetAttribute)
            ) should contain theSameElementsAs Seq("1", "2")
            spans.map(
              spanAttribute(_, TeaProjectionFactory.DeltaAttribute)
            ) should contain theSameElementsAs Seq(
              creditBalanceDelta(10).toString,
              debitBalanceDelta(3).toString,
            )
          }
        } finally stopProjection(testKit, ref)
      }

      "deduplicate events delivered more than once" in withProjection { (testKit, backend) =>
        val t1 = clock.now
        val t2 = t1.immediateSuccessor
        val eventSource = EventSource.LedgerAPICompletions
        // Offset 1 is delivered twice; the store deduplicates on the derived event id, so the
        // duplicate must neither be counted in the balance nor stored a second time.
        val events = Seq(
          projectionEvent(1L, t1, alice.credit(10)),
          projectionEvent(1L, t1, alice.credit(10)),
          projectionEvent(2L, t2, alice.credit(5)),
        )

        val ref = spawnProjection(testKit, backend, recordingSourceFactory(events))
        try {
          eventually() {
            // 10 (offset 1, counted once) + 5 (offset 2); the duplicate +10 is ignored.
            balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 15, t2))
          }
          eventsOf(backend, alice, t1) should contain theSameElementsAs Seq(
            DeltaEvent(creditBalanceDelta(10), t1, eventSource),
            DeltaEvent(creditBalanceDelta(5), t2, eventSource),
          )
        } finally stopProjection(testKit, ref)
      }

      "apply every delta of an event holding several deltas" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val eventSource = EventSource.LedgerAPICompletions
          val events = Seq(
            // Same account twice: the position suffix keeps the two deltas from being deduplicated
            multiDeltaEvent(
              offset = 1L,
              t1,
              (1, alice.credit(10)),
              (2, alice.credit(10)),
              (3, bob.credit(7)),
            ),
            projectionEvent(2L, t2, alice.debit(3)),
          )

          val ref = spawnProjection(
            testKit,
            backend,
            recordingSourceFactory(events, traceContext = multiDeltaTraceContext),
          )
          try {
            eventually() {
              balanceOf(backend, alice) shouldBe Some(AccountState(alice, 3, 20, t2))
              balanceOf(backend, bob) shouldBe Some(AccountState.credits(bob, 7, t1))
            }
            eventsOf(backend, alice, t1) should contain theSameElementsAs Seq(
              DeltaEvent(creditBalanceDelta(10), t1, eventSource),
              DeltaEvent(creditBalanceDelta(10), t1, eventSource),
              DeltaEvent(debitBalanceDelta(3), t2, eventSource),
            )
            eventsOf(backend, bob, t1) shouldBe Seq(
              DeltaEvent(creditBalanceDelta(7), t1, eventSource)
            )
            offsetGaugeValue(backend) shouldBe 2L
            eventually() {
              // One span per delta, not per event
              spansOn(multiDeltaTraceContext).map(
                spanAttribute(_, TeaProjectionFactory.OffsetAttribute)
              ) should contain theSameElementsAs Seq("1", "1", "1", "2")
            }
          } finally stopProjection(testKit, ref)
      }

      "deduplicate an event holding several deltas when it is delivered again" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val multi = multiDeltaEvent(
            offset = 1L,
            t1,
            (1, alice.credit(10)),
            (2, bob.credit(7)),
          )
          val events = Seq(
            multi,
            multi,
            projectionEvent(2L, t2, alice.credit(5)),
          )

          val ref = spawnProjection(testKit, backend, recordingSourceFactory(events))
          try {
            eventually() {
              balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 15, t2))
            }
            balanceOf(backend, bob) shouldBe Some(AccountState.credits(bob, 7, t1))
            eventsOf(backend, alice, t1) should have size 2
            eventsOf(backend, bob, t1) should have size 1
          } finally stopProjection(testKit, ref)
      }

      "not commit any delta of an event when one of its deltas fails" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val maxCredit = NonNegativeLong.tryCreate(Long.MaxValue)
          // Offset 1 maxes out bob's credits, so bob's delta in offset 2 can never be applied
          val events = Seq(
            projectionEvent(1L, t1, bob.credit(Long.MaxValue)),
            multiDeltaEvent(
              offset = 2L,
              t2,
              (1, alice.credit(5)),
              (2, bob.credit(1)),
            ),
          )

          // Retries and restarts make the exact number of logged WARN entries non-deterministic.
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              val ref = spawnProjection(testKit, backend, recordingSourceFactory(events))
              try {
                // Alice's delta comes first. The DB backends roll it back together with bob's
                // failing delta, the in-memory backend keeps it (and deduplicates it on retry).
                val expectedAlice =
                  if (deltasAreAppliedAtomically) None
                  else Some(AccountState.credits(alice, 5, t2))
                eventually() {
                  balanceOf(backend, bob) shouldBe Some(AccountState.credits(bob, maxCredit, t1))
                  balanceOf(backend, alice) shouldBe expectedAlice
                }
                always() {
                  balanceOf(backend, alice) shouldBe expectedAlice
                  eventsOf(backend, alice, t1) should have size expectedAlice.toList.size.toLong
                  balanceOf(backend, bob) shouldBe Some(AccountState.credits(bob, maxCredit, t1))
                  eventsOf(backend, bob, t1) should have size 1
                }
              } finally stopProjection(testKit, ref)
            },
            entries => {
              entries should not be empty
              forEvery(entries) { entry =>
                entry.warningMessage should include("Error during envelope processing")
              }
            },
          )
      }

      "resume from the last processed offset after a restart" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val t3 = t2.immediateSuccessor
          val events = Seq(
            projectionEvent(1L, t1, alice.credit(10)),
            projectionEvent(2L, t2, alice.credit(20)),
            projectionEvent(3L, t3, alice.credit(30)),
          )

          // First run ingests everything.
          val firstRun = spawnProjection(testKit, backend, recordingSourceFactory(events))
          eventually() {
            balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 60, t3))
          }
          stopProjection(testKit, firstRun)

          // Second run: a fresh projection re-subscribes; we record the offset it resumes from.
          val resumeOffsets = new ConcurrentLinkedQueue[Option[Long]]()
          val secondRun =
            spawnProjection(testKit, backend, recordingSourceFactory(events, resumeOffsets))
          try {
            eventually() {
              resumeOffsets.asScala.toList should not be empty
            }
            val expectedResumeOffset = if (offsetsArePersisted) Some(3L) else None
            resumeOffsets.peek() shouldBe expectedResumeOffset

            // Whatever is replayed, deduplication keeps the balance and the event log correct.
            always() {
              balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 60, t3))
            }
            eventsOf(backend, alice, t1) should have size 3
          } finally stopProjection(testKit, secondRun)
      }

      "recover and finish pending work after a crash and restart" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val t3 = t2.immediateSuccessor
          val t4 = t3.immediateSuccessor
          val allEvents = Seq(
            projectionEvent(1L, t1, alice.credit(10)),
            projectionEvent(2L, t2, alice.credit(20)),
            projectionEvent(3L, t3, alice.credit(30)),
            projectionEvent(4L, t4, alice.credit(40)),
          )

          // The first instance only ingests the first two events before the node "crashes".
          val crashed = spawnProjection(testKit, backend, recordingSourceFactory(allEvents.take(2)))
          eventually() {
            balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 30, t2))
          }
          stopProjection(testKit, crashed)

          // After the restart the new instance sees the full stream and must finish the pending
          // work without re-applying the events that were already processed.
          val restarted = spawnProjection(testKit, backend, recordingSourceFactory(allEvents))
          try {
            eventually() {
              balanceOf(backend, alice) shouldBe Some(AccountState.credits(alice, 100, t4))
            }
            // Every event is applied exactly once.
            eventsOf(backend, alice, t1) should have size 4
          } finally stopProjection(testKit, restarted)
      }

      // Intended to verify the RestartWithBackoffSource rule in rewrite-appender.xml
      "log at info level" in {

        // Only run with DB tests
        if (offsetsArePersisted) {

          withProjection { (testKit, backend) =>
            val serviceName = "Ledger API offset dispatcher"
            val thrown = new AtomicInteger(0)

            val t1 = clock.now
            val events = Seq(
              projectionEvent(1L, t1, alice.credit(1))
            )

            val errorSource: Option[Long] => Source[Traced[ProjectionEvent], NotUsed] = o => {
              recordingSourceFactory(events)(o).map { event =>
                thrown.getAndIncrement() match {
                  case 0 =>
                    throw CommonErrors.ServiceNotRunning
                      .Reject(serviceName)(errorLoggingContext)
                      .asGrpcError
                  case _ =>
                    event
                }
              }
            }

            loggerFactory.assertLoggedWarningsAndErrorsSeq(
              {
                val ref = spawnProjection(testKit, backend, errorSource)
                try {
                  eventually()(thrown.get() shouldBe 2)
                } finally stopProjection(testKit, ref)
              },
              _ shouldBe empty,
            )
          }
        }
      }

      "report an unapplicable delta as TRAFFIC_UPDATE_OUT_OF_BOUND and stop advancing" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          // Offset 1 maxes out the credit total, so offset 2 keeps failing to apply on top of it.
          val events = Seq(
            projectionEvent(1L, t1, alice.credit(Long.MaxValue)),
            projectionEvent(2L, t2, alice.credit(1L)),
          )

          // Retries and restarts make the exact number of logged WARN entries non-deterministic.
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              val ref = spawnProjection(
                testKit,
                backend,
                recordingSourceFactory(events, traceContext = nonEmptyTraceContext2),
              )
              try {
                val maxCredit = NonNegativeLong.tryCreate(Long.MaxValue)
                eventually() {
                  balanceOf(backend, alice) shouldBe Some(
                    AccountState.credits(alice, maxCredit, t1)
                  )
                }
                // Offset 2 never commits: balance and event log stay where the first event left them.
                always() {
                  balanceOf(backend, alice) shouldBe Some(
                    AccountState.credits(alice, maxCredit, t1)
                  )
                  eventsOf(backend, alice, t1) should have size 1
                }
                // The span is only marked ERROR once Slick actually runs the deferred DB action, so poll for it.
                eventually() {
                  val (failed, succeeded) =
                    spansOn(nonEmptyTraceContext2).partition(
                      _.getStatus.getStatusCode == StatusCode.ERROR
                    )
                  failed should not be empty
                  forEvery(failed) { span =>
                    spanAttribute(span, TeaProjectionFactory.OffsetAttribute) shouldBe "2"
                  }
                  val recordedExceptions =
                    failed
                      .flatMap(_.getEvents.asScala)
                      .flatMap(event =>
                        Option(
                          event.getAttributes.get(AttributeKey.stringKey("exception.message"))
                        )
                      )
                  forAtLeast(1, recordedExceptions)(_ should include("TRAFFIC_UPDATE_OUT_OF_BOUND"))
                  succeeded.map(
                    spanAttribute(_, TeaProjectionFactory.OffsetAttribute)
                  ) should contain("1")
                }
              } finally stopProjection(testKit, ref)
            },
            entries => {
              entries should not be empty
              forEvery(entries) { entry =>
                entry.warningMessage should include("Error during envelope processing")
              }
              forAtLeast(1, entries) { entry =>
                val throwableMessage = entry.throwable.map(_.getMessage).getOrElse("")
                (entry.message + throwableMessage) should include("TRAFFIC_UPDATE_OUT_OF_BOUND")
              }
            },
          )
      }
    }
  }
}
