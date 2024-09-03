// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{
  CommonMockMetrics,
  MetricsUtils,
  SequencerClientMetrics,
  TrafficConsumptionMetrics,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficReceipt,
  TrafficStateController,
}
import com.digitalasset.canton.sequencing.{
  OrdinaryProtocolEvent,
  RawProtocolEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.topology.DefaultTestIdentities.{domainId, participant1}
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}

class SendTrackerTest extends AsyncWordSpec with BaseTest with MetricsUtils {
  val metrics = CommonMockMetrics.sequencerClient
  val msgId1 = MessageId.tryCreate("msgId1")
  val msgId2 = MessageId.tryCreate("msgId2")

  def sign(event: RawProtocolEvent): SignedContent[RawProtocolEvent] =
    SignedContent(event, SymbolicCrypto.emptySignature, None, testedProtocolVersion)

  def deliverDefault(timestamp: CantonTimestamp): OrdinaryProtocolEvent =
    OrdinarySequencedEvent(
      sign(
        SequencerTestUtils.mockDeliver(
          timestamp = timestamp,
          domainId = DefaultTestIdentities.domainId,
        )
      )
    )(
      traceContext
    )

  def deliver(
      msgId: MessageId,
      timestamp: CantonTimestamp,
      trafficReceipt: Option[TrafficReceipt] = None,
  ): OrdinaryProtocolEvent =
    OrdinarySequencedEvent(
      sign(
        Deliver.create(
          SequencerCounter(0),
          timestamp,
          DefaultTestIdentities.domainId,
          Some(msgId),
          Batch.empty(testedProtocolVersion),
          None,
          testedProtocolVersion,
          trafficReceipt,
        )
      )
    )(traceContext)

  def deliverError(
      msgId: MessageId,
      timestamp: CantonTimestamp,
      trafficReceipt: Option[TrafficReceipt] = None,
  ): OrdinaryProtocolEvent =
    OrdinarySequencedEvent(
      sign(
        DeliverError.create(
          SequencerCounter(0),
          timestamp,
          DefaultTestIdentities.domainId,
          msgId,
          SequencerErrors.SubmissionRequestRefused("test"),
          testedProtocolVersion,
          trafficReceipt,
        )
      )
    )(traceContext)

  case class Env(tracker: MySendTracker, store: InMemorySendTrackerStore)

  class MySendTracker(
      initialPendingSends: Map[MessageId, CantonTimestamp],
      store: SendTrackerStore,
      metrics: SequencerClientMetrics,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      timeoutHandler: MessageId => Future[Unit],
      val trafficStateController: Option[TrafficStateController],
  )(implicit executionContext: ExecutionContext)
      extends SendTracker(
        initialPendingSends,
        store,
        metrics,
        loggerFactory,
        timeouts,
        trafficStateController,
        participant1,
      ) {

    private val calls = new AtomicInteger()

    def callCount = calls.get()

    def assertNotCalled = callCount shouldBe 0

    override def handleTimeout(
        timestamp: CantonTimestamp
    )(msgId: MessageId)(implicit traceContext: TraceContext): Future[Unit] = {
      calls.incrementAndGet()
      timeoutHandler(msgId).flatMap { _ =>
        super.handleTimeout(timestamp)(msgId)
      }
    }

  }

  private val initialTrafficState = TrafficState.empty
  def mkSendTracker(timeoutHandler: MessageId => Future[Unit] = _ => Future.unit): Env = {
    val store = new InMemorySendTrackerStore()
    val topologyClient =
      TestingTopology(Set(DefaultTestIdentities.domainId))
        .build(loggerFactory)
        .forOwnerAndDomain(participant1, domainId)

    val histogramInventory = new HistogramInventory()
    val trafficStateController = new TrafficStateController(
      DefaultTestIdentities.participant1,
      loggerFactory,
      topologyClient,
      initialTrafficState,
      testedProtocolVersion,
      new EventCostCalculator(loggerFactory),
      futureSupervisor,
      timeouts,
      new TrafficConsumptionMetrics(MetricName("test"), metricsFactory(histogramInventory)),
      domainId,
    )
    val tracker =
      new MySendTracker(
        Map.empty,
        store,
        metrics,
        loggerFactory,
        timeouts,
        timeoutHandler,
        Some(trafficStateController),
      )

    Env(tracker, store)
  }

  implicit private val eventSpecificMetricsContext: MetricsContext = MetricsContext(
    "test" -> "value"
  )

  "tracking sends" should {
    "error if there's a previously tracked send with the same message id" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track first")
        error <- tracker.track(msgId1, CantonTimestamp.MinValue).leftOrFailShutdown("track second")
      } yield error shouldBe SavePendingSendError.MessageIdAlreadyTracked
    }

    "is able to track a send with a prior message id if a receipt is observed" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track first")
        _ <- tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
        _ <- tracker
          .track(msgId1, CantonTimestamp.MinValue)
          .valueOrFailShutdown(
            "track same msgId after receipt"
          )
      } yield tracker.assertNotCalled
    }

    "propagate metrics context" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track first")
        _ <- tracker.update(
          Seq(
            deliver(
              msgId1,
              initialTrafficState.timestamp.immediateSuccessor,
              trafficReceipt = Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.tryCreate(1),
                  extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                  baseTrafficRemainder = NonNegativeLong.tryCreate(3),
                )
              ),
            )
          )
        )
        _ = tracker.trafficStateController.value.updateBalance(
          NonNegativeLong.tryCreate(20),
          PositiveInt.one,
          CantonTimestamp.MaxValue,
        )
      } yield {
        assertLongValue("test.extra-traffic-purchased", 20L)
        assertInContext(
          "test.extra-traffic-purchased",
          "member",
          DefaultTestIdentities.participant1.toString,
        )
        assertLongValue("test.event-delivered-cost", 1L)
        assertInContext(
          "test.event-delivered-cost",
          "domain",
          domainId.toString,
        )
        assertInContext(
          "test.event-delivered-cost",
          "member",
          DefaultTestIdentities.participant1.toString,
        )
        // Event specific metrics should contain the event specific metrics context
        assertInContext("test.event-delivered-cost", "test", "value")
        assertLongValue("test.extra-traffic-consumed", 2L)
        assertInContext(
          "test.extra-traffic-consumed",
          "member",
          DefaultTestIdentities.participant1.toString,
        )
        assertInContext(
          "test.extra-traffic-consumed",
          "domain",
          domainId.toString,
        )
        // But not the event agnostic metrics
        assertNotInContext("test.extra-traffic-consumed", "test")
      }
    }

    "not re-export metrics when replaying events older than current state" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track first")
        _ <- tracker.update(
          Seq(
            deliver(
              msgId1,
              initialTrafficState.timestamp,
              trafficReceipt = Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.tryCreate(1),
                  extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                  baseTrafficRemainder = NonNegativeLong.tryCreate(3),
                )
              ),
            )
          )
        )
      } yield {
        assertNoValue("event-delivered-cost")
      }
    }

    "metrics should contain default labels for unknown sends" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.update(
          Seq(
            deliver(
              msgId1,
              initialTrafficState.timestamp.immediateSuccessor,
              trafficReceipt = Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.tryCreate(1),
                  extraTrafficConsumed = NonNegativeLong.tryCreate(2),
                  baseTrafficRemainder = NonNegativeLong.tryCreate(3),
                )
              ),
            )
          )
        )
      } yield {
        assertLongValue("test.event-delivered-cost", 1L)
        assertInContext(
          "test.event-delivered-cost",
          "member",
          DefaultTestIdentities.participant1.toString,
        )
        // Check there are labels for application-id and type
        assertInContext("test.event-delivered-cost", "application-id", "unknown")
        assertInContext("test.event-delivered-cost", "type", "unknown")
      }
    }
  }

  "updating" should {
    def verifyEventRemovesPendingSend(event: OrdinaryProtocolEvent) = {
      val Env(tracker, store) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId1")
        _ <- tracker.track(msgId2, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId2")
        pendingSends1 <- store.fetchPendingSends
        _ = pendingSends1 shouldBe Map(
          msgId1 -> CantonTimestamp.MinValue,
          msgId2 -> CantonTimestamp.MinValue,
        )
        _ <- tracker.update(Seq(event))
        pendingSends2 <- store.fetchPendingSends
        _ = pendingSends2 shouldBe Map(
          msgId2 -> CantonTimestamp.MinValue
        )
      } yield tracker.assertNotCalled
    }

    "remove tracked send on deliver event" in verifyEventRemovesPendingSend(
      deliver(msgId1, CantonTimestamp.MinValue)
    )

    "removed tracked send on deliver error event" in verifyEventRemovesPendingSend(
      deliverError(msgId1, CantonTimestamp.MinValue)
    )

    "notify only timed out events" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId1")
        _ <- tracker
          .track(msgId2, CantonTimestamp.MinValue.plusSeconds(2))
          .valueOrFailShutdown("track msgId2")
        _ <-
          tracker.update(
            Seq(
              deliverDefault(CantonTimestamp.MinValue.plusSeconds(1))
            )
          )
        _ = tracker.callCount shouldBe 1
        _ <- tracker.update(
          Seq(
            deliverDefault(CantonTimestamp.MinValue.plusSeconds(3))
          )
        )
      } yield tracker.callCount shouldBe 2
    }

    "not get upset if we see the same message id twice" in {
      // during reconnects we may replay the same deliver/deliverEvent
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId1")
        _ <- tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
        _ <- tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
      } yield succeed
    }

    "call timeout handlers sequentially" in {
      val concurrentCalls = new AtomicInteger()
      val totalCalls = new AtomicInteger()

      val Env(tracker, _) = mkSendTracker { _msgId =>
        totalCalls.incrementAndGet()
        if (!concurrentCalls.compareAndSet(0, 1)) {
          fail("timeout handler was called concurrently")
        }

        Future {
          if (!concurrentCalls.compareAndSet(1, 0)) {
            fail("timeout handler was called concurrently")
          }
        }
      }

      for {
        _ <- tracker.track(msgId1, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId1")
        _ <- tracker.track(msgId2, CantonTimestamp.MinValue).valueOrFailShutdown("track msgId2")
        _ <- tracker.update(Seq(deliverDefault(CantonTimestamp.MinValue.plusSeconds(1))))
      } yield totalCalls.get() shouldBe 2
    }

    "track callback" should {
      class CaptureSendResultHandler {
        private val calledWithP = Promise[UnlessShutdown[SendResult]]()
        val handler: SendCallback = result => {
          calledWithP.success(result)
        }

        val result: FutureUnlessShutdown[SendResult] = FutureUnlessShutdown(calledWithP.future)
      }

      "callback with successful send" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler

        for {
          _ <- tracker
            .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
            .valueOrFailShutdown("track msgId1")
          _ <- tracker.update(Seq(deliver(msgId1, CantonTimestamp.MinValue)))
          calledWith <- sendResultHandler.result.failOnShutdown
        } yield calledWith should matchPattern { case SendResult.Success(_) =>
        }
      }

      "callback with deliver error" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler

        for {
          _ <- tracker
            .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
            .valueOrFailShutdown("track msgId1")
          _ <- tracker.update(
            Seq(deliverError(msgId1, CantonTimestamp.MinValue))
          )
          calledWith <- sendResultHandler.result.failOnShutdown
        } yield calledWith should matchPattern { case SendResult.Error(_) =>
        }
      }

      "callback with timeout" in {
        val Env(tracker, _) = mkSendTracker()
        val sendResultHandler = new CaptureSendResultHandler
        val sendMaxSequencingTime = CantonTimestamp.MinValue
        val deliverEventTime = sendMaxSequencingTime.plusSeconds(1)

        for {
          _ <- tracker
            .track(msgId1, sendMaxSequencingTime, sendResultHandler.handler)
            .valueOrFailShutdown("track msgId1")
          _ <- tracker.update(Seq(deliverDefault(deliverEventTime)))
          calledWith <- sendResultHandler.result.failOnShutdown
        } yield calledWith should matchPattern { case SendResult.Timeout(deliverEventTime) =>
        }
      }
    }
  }
}
