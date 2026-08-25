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
import com.digitalasset.canton.tracing.Traced
import com.typesafe.config.{Config, ConfigFactory}
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

  def additionalPekkoConfig: Config = ConfigFactory.empty()

  protected val clock = new SimClock(loggerFactory = loggerFactory)

  /** Whether the backend persists offsets across projection restarts. The DB backends keep the
    * offset in the pekko offset store, the in-memory backend does not (it replays from the start
    * and relies on store-level deduplication).
    */
  protected def offsetsArePersisted: Boolean

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
      .withFallback(additionalPekkoConfig)

  /** Run a test body with a dedicated actor system and a fresh backend, tearing everything down
    * afterwards.
    */
  protected def withProjection[A](body: (ActorTestKit, Backend) => A): A = {
    val testKit = ActorTestKit(s"tea-projection-${UUID.randomUUID()}", pekkoTestConfig)
    try body(testKit, createBackend()(testKit.system))
    finally testKit.shutdownTestKit()
  }

  protected def projectionEvent(
      account: AccountId,
      delta: TrafficDelta,
      offset: Long,
      timestamp: CantonTimestamp,
      eventSource: EventSource = EventSource.LedgerAPICompletions,
  ): ProjectionEvent =
    ProjectionEvent(
      account,
      OffsetDeltaEvent(DeltaEvent(delta, timestamp, eventSource), offset),
    )

  private def tracedSource(
      events: Seq[ProjectionEvent]
  ): Source[Traced[ProjectionEvent], NotUsed] =
    Source(events.toList)
      .map(event => Traced.empty(event))
      // Keep the stream open after the (finite) test events so the projection idles in a steady
      // state instead of completing and being restarted in a loop.
      .concat(Source.never[Traced[ProjectionEvent]])

  /** A source factory that resumes from the offset it is handed and records every offset it is
    * asked to resume from.
    */
  protected def recordingSourceFactory(
      events: Seq[ProjectionEvent],
      resumeOffsets: ConcurrentLinkedQueue[Option[Long]] = new ConcurrentLinkedQueue[Option[Long]](),
  ): Option[Long] => Source[Traced[ProjectionEvent], NotUsed] = { maybeOffset =>
    resumeOffsets.add(maybeOffset)
    val fromExclusive = maybeOffset.getOrElse(Long.MinValue)
    tracedSource(events.filter(_.event.offset > fromExclusive))
  }

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
      backend.newProjection().projection(projectionId, sourceFactory),
      s"projection-${UUID.randomUUID()}",
    )

  private def stopProjection(
      testKit: ActorTestKit,
      ref: ActorRef[ProjectionBehavior.Command],
  ): Unit =
    testKit.stop(ref, 30.seconds)

  private val alice = AccountId.tryCreate("alice")

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
          projectionEvent(alice, creditBalanceDelta(10), offset = 1L, t1),
          projectionEvent(alice, debitBalanceDelta(3), offset = 2L, t2),
        )

        val ref = spawnProjection(testKit, backend, recordingSourceFactory(events))
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
        } finally stopProjection(testKit, ref)
      }

      "deduplicate events delivered more than once" in withProjection { (testKit, backend) =>
        val t1 = clock.now
        val t2 = t1.immediateSuccessor
        val eventSource = EventSource.LedgerAPICompletions
        // Offset 1 is delivered twice; the store deduplicates on the derived event id, so the
        // duplicate must neither be counted in the balance nor stored a second time.
        val events = Seq(
          projectionEvent(alice, creditBalanceDelta(10), offset = 1L, t1),
          projectionEvent(alice, creditBalanceDelta(10), offset = 1L, t1),
          projectionEvent(alice, creditBalanceDelta(5), offset = 2L, t2),
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

      "resume from the last processed offset after a restart" in withProjection {
        (testKit, backend) =>
          val t1 = clock.now
          val t2 = t1.immediateSuccessor
          val t3 = t2.immediateSuccessor
          val events = Seq(
            projectionEvent(alice, creditBalanceDelta(10), offset = 1L, t1),
            projectionEvent(alice, creditBalanceDelta(20), offset = 2L, t2),
            projectionEvent(alice, creditBalanceDelta(30), offset = 3L, t3),
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
            projectionEvent(alice, creditBalanceDelta(10), offset = 1L, t1),
            projectionEvent(alice, creditBalanceDelta(20), offset = 2L, t2),
            projectionEvent(alice, creditBalanceDelta(30), offset = 3L, t3),
            projectionEvent(alice, creditBalanceDelta(40), offset = 4L, t4),
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
              projectionEvent(alice, TrafficDelta.creditBalanceDelta(1), offset = 1L, t1)
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
            projectionEvent(alice, creditBalanceDelta(Long.MaxValue), offset = 1L, t1),
            projectionEvent(alice, creditBalanceDelta(1L), offset = 2L, t2),
          )

          // Retries and restarts make the exact number of logged WARN entries non-deterministic.
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
            {
              val ref = spawnProjection(testKit, backend, recordingSourceFactory(events))
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
