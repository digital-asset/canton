// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, Traced}
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Span
import org.scalatest.wordspec.AnyWordSpec

class MempoolStateTest extends AnyWordSpec with BaseTest {

  private val T = CantonTimestamp.Epoch.plusSeconds(10)
  private val generousBytes = NonNegativeInt.tryCreate(1000)

  private def orderRequest(
      payloadBytes: Int,
      maxSequencingTime: Option[CantonTimestamp] = None,
      messageId: String = "",
  ): Mempool.OrderRequest =
    Mempool.OrderRequest(
      Traced(
        OrderingRequest(
          BlockFormat.SendTag,
          messageId,
          ByteString.copyFromUtf8("x" * payloadBytes),
        )
      ),
      maxSequencingTime = maxSequencingTime,
    )

  private def newSpan(): Span =
    NoReportingTracerProvider.tracer.spanBuilder("test").startSpan()

  private def newState(): MempoolState = new MempoolState(weakQuorum = 1)

  "dequeueForBatch" when {

    "the queue is empty" should {
      "return an empty sequence" in {
        newState().dequeueForBatch(5, generousBytes) shouldBe empty
      }
    }

    // This case shouldn't arise in practice, kept this test for completeness since dequeueForBatch's
    // contract doesn't itself rule it out.
    "n is 0" should {
      "return an empty sequence without touching the queue" in {
        val state = newState()
        val span = newSpan()
        // This request is already expired, but n = 0 means dequeueForBatch never peeks at
        // the queue, so it's left untouched rather than discarded.
        state.enqueueRequest(orderRequest(1, maxSequencingTime = Some(T)), span)
        state.updateLatestKnownSequencingTime(T)

        state.dequeueForBatch(0, generousBytes) shouldBe empty
        state.receivedOrderRequests.size shouldBe 1
        span.isRecording shouldBe true
      }
    }

    "there are more live requests than fit in n" should {
      "return them in FIFO order, leaving the rest queued" in {
        val state = newState()
        val a = orderRequest(1, messageId = "a")
        val b = orderRequest(1, messageId = "b")
        val c = orderRequest(1, messageId = "c")
        state.enqueueRequest(a, newSpan())
        state.enqueueRequest(b, newSpan())
        state.enqueueRequest(c, newSpan())

        state.dequeueForBatch(2, generousBytes).map(_.orderRequest) shouldBe Seq(a, b)
        state.receivedOrderRequests.map(_.orderRequest) shouldBe Seq(c)

        state.dequeueForBatch(2, generousBytes).map(_.orderRequest) shouldBe Seq(c)
        state.receivedOrderRequests shouldBe empty
      }
    }

    "a request would exceed the remaining weight budget" should {
      "stop before including it, leaving it queued for the next batch" in {
        val state = newState()
        val a = orderRequest(3, messageId = "a")
        val b = orderRequest(3, messageId = "b")
        val c = orderRequest(3, messageId = "c")
        state.enqueueRequest(a, newSpan())
        state.enqueueRequest(b, newSpan())
        state.enqueueRequest(c, newSpan())

        // budget 8: a (3) + b (3) fit (remaining 2), c (3) doesn't
        val result =
          state.dequeueForBatch(10, NonNegativeInt.tryCreate(8)).map(_.orderRequest)

        result shouldBe Seq(a, b)
        state.receivedOrderRequests.map(_.orderRequest) shouldBe Seq(c)
      }
    }

    // Kept for completeness: the mempool already rejects oversized requests before enqeueing them,
    // so this can only happen if the budget shrinks after a request was accepted.
    "a queued request's weight alone exceeds the maximum combined weight" should {
      "discard it rather than let it stall the queue" in {
        val state = newState()
        val oversizedSpan = newSpan()
        val oversized = orderRequest(20, messageId = "oversized")
        val normal = orderRequest(3, messageId = "normal")
        state.enqueueRequest(oversized, oversizedSpan)
        state.enqueueRequest(normal, newSpan())

        val result =
          state.dequeueForBatch(5, NonNegativeInt.tryCreate(8)).map(_.orderRequest)

        result shouldBe Seq(normal)
        state.receivedOrderRequests shouldBe empty
        oversizedSpan.isRecording shouldBe false
      }

      "discard it even when it isn't the first request in the batch" in {
        val state = newState()
        val oversizedSpan = newSpan()
        val normal1 = orderRequest(2, messageId = "normal1")
        val oversized = orderRequest(20, messageId = "oversized")
        val normal2 = orderRequest(2, messageId = "normal2")
        state.enqueueRequest(normal1, newSpan())
        state.enqueueRequest(oversized, oversizedSpan)
        state.enqueueRequest(normal2, newSpan())

        val result =
          state.dequeueForBatch(5, NonNegativeInt.tryCreate(8)).map(_.orderRequest)

        result shouldBe Seq(normal1, normal2)
        state.receivedOrderRequests shouldBe empty
        oversizedSpan.isRecording shouldBe false
      }
    }

    "a queued request's max sequencing time is at or before the latest known sequencing time" should {
      "discard it without counting it against n or the weight budget, and end its span" in {
        val state = newState()
        val expiredSpan1 = newSpan()
        val expiredSpan2 = newSpan()
        val liveSpan = newSpan()
        state.enqueueRequest(
          orderRequest(1, maxSequencingTime = Some(T), messageId = "expired1"),
          expiredSpan1,
        )
        state.enqueueRequest(
          orderRequest(1, maxSequencingTime = Some(T.minusSeconds(1)), messageId = "expired2"),
          expiredSpan2,
        )
        val live = orderRequest(1, messageId = "live")
        state.enqueueRequest(live, liveSpan)

        state.updateLatestKnownSequencingTime(T)

        val result = state.dequeueForBatch(1, generousBytes).map(_.orderRequest)

        result shouldBe Seq(live)
        state.receivedOrderRequests shouldBe empty
        expiredSpan1.isRecording shouldBe false
        expiredSpan2.isRecording shouldBe false
        // dequeueForBatch doesn't end the spans of live requests, that happens later, once the
        // request has actually been included in a batch to be disseminated.
        liveSpan.isRecording shouldBe true
      }

      "discard it exactly at the boundary (max sequencing time == latest known sequencing time)" in {
        val state = newState()
        val span = newSpan()
        state.enqueueRequest(orderRequest(1, maxSequencingTime = Some(T)), span) // expired request
        state.updateLatestKnownSequencingTime(T)

        state.dequeueForBatch(1, generousBytes) shouldBe empty
        state.receivedOrderRequests shouldBe empty
        // discarding an expired request ends its span
        span.isRecording shouldBe false
      }
    }

    "a queued request's max sequencing time is after the latest known sequencing time" should {
      "include it in the batch" in {
        val state = newState()
        val req = orderRequest(1, maxSequencingTime = Some(T.plusMillis(1)))
        state.enqueueRequest(req, newSpan())
        state.updateLatestKnownSequencingTime(T)

        state.dequeueForBatch(1, generousBytes).map(_.orderRequest) shouldBe Seq(req)
      }
    }

    "a queued request has no max sequencing time" should {
      "never expire it, however far the latest known sequencing time advances" in {
        val state = newState()
        val req = orderRequest(1, maxSequencingTime = None)
        state.enqueueRequest(req, newSpan())
        state.updateLatestKnownSequencingTime(CantonTimestamp.MaxValue)

        state.dequeueForBatch(1, generousBytes).map(_.orderRequest) shouldBe Seq(req)
      }
    }

    "latest known sequencing time updates arrive out of order" should {
      "not let an earlier update move the cutoff backward" in {
        val state = newState()
        val span = newSpan()
        val laterT = T.plusSeconds(1)
        // maxSequencingTime falls strictly between T and laterT
        val req = orderRequest(1, maxSequencingTime = Some(T.plusMillis(1)))
        state.enqueueRequest(req, span)

        // Block storage can complete out of order, so a later block's update can be received
        // before an earlier block's.
        state.updateLatestKnownSequencingTime(laterT)
        state.updateLatestKnownSequencingTime(T)

        // If the cutoff had regressed to T, req would incorrectly look live again.
        state.dequeueForBatch(1, generousBytes) shouldBe empty
        state.receivedOrderRequests shouldBe empty
        span.isRecording shouldBe false
      }
    }

    "expired requests are queued both in a consecutive run and interleaved with live ones" should {
      "only discard the expired requests actually walked over while filling each batch" in {
        val state = newState()

        def expired(id: String) = orderRequest(1, maxSequencingTime = Some(T), messageId = id)
        def live(id: String) =
          orderRequest(1, maxSequencingTime = Some(T.plusSeconds(100)), messageId = id)

        val e1Span = newSpan()
        val e2Span = newSpan()
        val e3Span = newSpan()
        val e4Span = newSpan()
        val l3Span = newSpan()
        val e1 = expired("e1")
        val e2 = expired("e2")
        val e3 = expired("e3")
        val e4 = expired("e4")
        val l1 = live("l1")
        val l2 = live("l2")
        val l3 = live("l3")

        // Pattern: expired, expired, live, expired, live, live, expired (7 total)
        state.enqueueRequest(e1, e1Span)
        state.enqueueRequest(e2, e2Span)
        state.enqueueRequest(l1, newSpan())
        state.enqueueRequest(e3, e3Span)
        state.enqueueRequest(l2, newSpan())
        state.enqueueRequest(l3, l3Span)
        state.enqueueRequest(e4, e4Span)
        state.updateLatestKnownSequencingTime(T)

        // The first batch only has to walk over e1, e2, l1, e3, l2 to collect the 2 live requests
        // it needs, l3 and e4 are left completely untouched, including the already-expired e4.
        val firstBatch = state.dequeueForBatch(2, generousBytes).map(_.orderRequest)
        firstBatch shouldBe Seq(l1, l2)
        state.receivedOrderRequests.map(_.orderRequest) shouldBe Seq(l3, e4)
        e1Span.isRecording shouldBe false
        e2Span.isRecording shouldBe false
        e3Span.isRecording shouldBe false
        // Not yet reached by any dequeueForBatch call, so not yet discarded
        e4Span.isRecording shouldBe true
        l3Span.isRecording shouldBe true

        // A later call that actually reaches it discards e4 too
        val secondBatch = state.dequeueForBatch(5, generousBytes).map(_.orderRequest)
        secondBatch shouldBe Seq(l3)
        state.receivedOrderRequests shouldBe empty
        e4Span.isRecording shouldBe false
      }
    }
  }
}
