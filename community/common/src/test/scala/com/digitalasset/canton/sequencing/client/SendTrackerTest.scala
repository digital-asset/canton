// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricName
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.client.SendTrackerUpdateError.TimeoutHandlerError
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.sequencing.{
  OrdinaryProtocolEvent,
  RawProtocolEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SavePendingSendError
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

class SendTrackerTest extends AsyncWordSpec with BaseTest {
  val metrics = new SequencerClientMetrics(MetricName("SendTrackerTest"), new MetricRegistry())
  val msgId1 = MessageId.tryCreate("msgId1")
  val msgId2 = MessageId.tryCreate("msgId2")

  def sign(event: RawProtocolEvent): SignedContent[RawProtocolEvent] =
    SignedContent(event, SymbolicCrypto.emptySignature, None)

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

  def deliver(msgId: MessageId, timestamp: CantonTimestamp): OrdinaryProtocolEvent =
    OrdinarySequencedEvent(
      sign(
        Deliver.create(
          SequencerCounter(0),
          timestamp,
          DefaultTestIdentities.domainId,
          Some(msgId),
          Batch.empty(testedProtocolVersion),
          testedProtocolVersion,
        )
      )
    )(traceContext)

  def deliverError(msgId: MessageId, timestamp: CantonTimestamp): OrdinaryProtocolEvent = {
    OrdinarySequencedEvent(
      sign(
        DeliverError.create(
          SequencerCounter(0),
          timestamp,
          DefaultTestIdentities.domainId,
          msgId,
          DeliverErrorReason.BatchRefused("test"),
          testedProtocolVersion,
        )
      )
    )(traceContext)
  }

  case class Env(tracker: SendTracker, store: InMemorySendTrackerStore)

  def mkSendTracker(): Env = {
    val store = new InMemorySendTrackerStore()
    val tracker = new SendTracker(Map.empty, store, metrics, loggerFactory)

    Env(tracker, store)
  }

  class TestTimeoutHandler {
    private val calls = new AtomicInteger()

    def callCount = calls.get()
    def assertNotCalled = callCount shouldBe 0

    val handler: SendTimeoutHandler = _ => {
      calls.incrementAndGet()
      EitherT.rightT[Future, SendTrackerUpdateError.TimeoutHandlerError](())
    }
  }

  "tracking sends" should {
    "error if there's a previously tracked send with the same message id" in {
      val Env(tracker, _) = mkSendTracker()

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track first")
        error <- tracker.track(msgId1, CantonTimestamp.MinValue).value.map(_.left.value)
      } yield error shouldBe SavePendingSendError.MessageIdAlreadyTracked
    }

    "is able to track a send with a prior message id if a receipt is observed" in {
      val Env(tracker, _) = mkSendTracker()
      val timeoutHandler = new TestTimeoutHandler

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track first")
        _ <- valueOrFail(
          tracker.update(timeoutHandler.handler)(deliver(msgId1, CantonTimestamp.MinValue))
        )("update tracker")
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))(
          "track same msgId after receipt"
        )
      } yield timeoutHandler.assertNotCalled
    }
  }

  "updating" should {
    def verifyEventRemovesPendingSend(event: OrdinaryProtocolEvent) = {
      val Env(tracker, store) = mkSendTracker()
      val timeoutHandler = new TestTimeoutHandler

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track msgId1")
        _ <- valueOrFail(tracker.track(msgId2, CantonTimestamp.MinValue))("track msgId2")
        pendingSends1 <- store.fetchPendingSends
        _ = pendingSends1 shouldBe Map(
          msgId1 -> CantonTimestamp.MinValue,
          msgId2 -> CantonTimestamp.MinValue,
        )
        _ <- valueOrFail(tracker.update(timeoutHandler.handler)(event))("update sendTracker")
        pendingSends2 <- store.fetchPendingSends
        _ = pendingSends2 shouldBe Map(
          msgId2 -> CantonTimestamp.MinValue
        )
      } yield timeoutHandler.assertNotCalled
    }

    "remove tracked send on deliver event" in verifyEventRemovesPendingSend(
      deliver(msgId1, CantonTimestamp.MinValue)
    )

    "removed tracked send on deliver error event" in verifyEventRemovesPendingSend(
      deliverError(msgId1, CantonTimestamp.MinValue)
    )

    "notify only timed out events" in {
      val Env(tracker, _) = mkSendTracker()
      val timeoutHandler = new TestTimeoutHandler

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track msgId1")
        _ <- valueOrFail(tracker.track(msgId2, CantonTimestamp.MinValue.plusSeconds(2)))(
          "track msgId2"
        )
        _ <- valueOrFail(
          tracker.update(timeoutHandler.handler)(
            deliverDefault(CantonTimestamp.MinValue.plusSeconds(1))
          )
        )("update1")
        _ = timeoutHandler.callCount shouldBe 1
        _ <- valueOrFail(
          tracker.update(timeoutHandler.handler)(
            deliverDefault(CantonTimestamp.MinValue.plusSeconds(3))
          )
        )("update2")
      } yield timeoutHandler.callCount shouldBe 2
    }

    "not get upset if we see the same message id twice" in {
      // during reconnects we may replay the same deliver/deliverEvent
      val Env(tracker, _) = mkSendTracker()
      val timeoutHandler = new TestTimeoutHandler

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track msgId1")
        _ <- valueOrFail(
          tracker.update(timeoutHandler.handler)(deliver(msgId1, CantonTimestamp.MinValue))
        )("update sendTracker with msgId1")
        _ <- valueOrFail(
          tracker.update(timeoutHandler.handler)(deliver(msgId1, CantonTimestamp.MinValue))
        )("update sendTracker with msgId1 again")
      } yield succeed
    }

    "call timeout handlers sequentially" in {
      val Env(tracker, _) = mkSendTracker()
      val concurrentCalls = new AtomicInteger()
      val totalCalls = new AtomicInteger()

      val timeoutHandler: SendTimeoutHandler = _msgId => {
        totalCalls.incrementAndGet()
        if (!concurrentCalls.compareAndSet(0, 1)) {
          fail("timeout handler was called concurrently")
        }

        EitherT.right[TimeoutHandlerError](Future {
          if (!concurrentCalls.compareAndSet(1, 0)) {
            fail("timeout handler was called concurrently")
          }
        })
      }

      for {
        _ <- valueOrFail(tracker.track(msgId1, CantonTimestamp.MinValue))("track msgId1")
        _ <- valueOrFail(tracker.track(msgId2, CantonTimestamp.MinValue))("track msgId2")
        _ <- valueOrFail(
          tracker.update(timeoutHandler)(deliverDefault(CantonTimestamp.MinValue.plusSeconds(1)))
        )("update")
      } yield totalCalls.get() shouldBe 2
    }

    "track callback" should {
      class CaptureSendResultHandler {
        private val calledWithP = Promise[SendResult]()
        val handler: SendCallback = result => {
          calledWithP.success(result)
        }

        val result: Future[SendResult] = calledWithP.future
      }

      "callback with successful send" in {
        val Env(tracker, _) = mkSendTracker()
        val timeoutHandler = new TestTimeoutHandler
        val sendResultHandler = new CaptureSendResultHandler

        for {
          _ <- valueOrFail(
            tracker
              .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
          )("track msgId1")
          _ <- valueOrFail(
            tracker.update(timeoutHandler.handler)(deliver(msgId1, CantonTimestamp.MinValue))
          )("update with msgId1")
          calledWith <- sendResultHandler.result
        } yield calledWith should matchPattern { case SendResult.Success(_) =>
        }
      }

      "callback with deliver error" in {
        val Env(tracker, _) = mkSendTracker()
        val timeoutHandler = new TestTimeoutHandler
        val sendResultHandler = new CaptureSendResultHandler

        for {
          _ <- valueOrFail(
            tracker
              .track(msgId1, CantonTimestamp.MinValue, sendResultHandler.handler)
          )("track msgId1")
          _ <- valueOrFail(
            tracker.update(timeoutHandler.handler)(deliverError(msgId1, CantonTimestamp.MinValue))
          )("update with msgId1")
          calledWith <- sendResultHandler.result
        } yield calledWith should matchPattern { case SendResult.Error(_) =>
        }
      }

      "callback with timeout" in {
        val Env(tracker, _) = mkSendTracker()
        val timeoutHandler = new TestTimeoutHandler
        val sendResultHandler = new CaptureSendResultHandler
        val sendMaxSequencingTime = CantonTimestamp.MinValue
        val deliverEventTime = sendMaxSequencingTime.plusSeconds(1)

        for {
          _ <- valueOrFail(
            tracker
              .track(msgId1, sendMaxSequencingTime, sendResultHandler.handler)
          )("track msgId1")
          _ <- valueOrFail(
            tracker.update(timeoutHandler.handler)(deliverDefault(deliverEventTime))
          )(
            "update with deliver event"
          )
          calledWith <- sendResultHandler.result
        } yield calledWith should matchPattern { case SendResult.Timeout(deliverEventTime) =>
        }
      }
    }
  }
}
