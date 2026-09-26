// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

class BftNodeRateLimiterTest extends AnyWordSpec with BftSequencerBaseTest {
  val node1 = BftNodeId("node1")
  val node2 = BftNodeId("node2")

  "BftNodeRateLimiter" should {
    // Note that the BftNodeRateLimiter uses the RateLimiter class, which is already widely tested.
    // So this test is just a much more basic one focusing just on the added elements on top, such as using
    // an injectable monotonic time source and rate limiting by node.
    "perform basic rate limiting per node with max burst" in {
      val nanos = new AtomicLong(0L)
      val maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(3)
      val maxTasksPerSecond = NonNegativeNumeric.tryCreate(1.toDouble)

      val limiter =
        new BftNodeRateLimiter(maxTasksPerSecond, maxBurstFactor, timeSource = nanos.get())

      // after initial max burst is reached, throttling of 1 task per second kicks in
      (0 until (maxBurstFactor.value.toInt)).foreach { _ =>
        limiter.checkAndUpdateRate(node1) shouldBe true
      }
      limiter.checkAndUpdateRate(node1) shouldBe false

      // node2 is tracked separately
      limiter.checkAndUpdateRate(node2) shouldBe true

      // after a bit of time, we can take the next task, but only 1
      nanos.addAndGet(Duration.ofMillis(1).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe true
      limiter.checkAndUpdateRate(node1) shouldBe false

      // not enough time to take the next one
      nanos.addAndGet(Duration.ofMillis(1).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe false

      // at the edge of being able to take the next one
      nanos.addAndGet(Duration.ofSeconds(1).minusMillis(2).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe false

      // after a full second has passed, we can take the next
      nanos.addAndGet(Duration.ofMillis(1).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe true
    }

    "keep refilling from an independent monotonic source even if wall time is static or corrected backwards" in {
      // Regression test for the view-change liveness failure: the rate limiter must consume monotonic
      //  elapsed time (System.nanoTime()-based), independent of the potentially static/non-monotonic wall
      //  clock. Otherwise, once the burst is exhausted, refilling would stall until wall time advances/recovers,
      //  which can keep retransmissions blocked during view-change recovery.
      val nanos = new AtomicLong(0L)
      val maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(3)
      val maxTasksPerSecond = NonNegativeNumeric.tryCreate(1.toDouble)

      val limiter =
        new BftNodeRateLimiter(maxTasksPerSecond, maxBurstFactor, timeSource = nanos.get())

      // Exhaust the initial burst.
      (0 until maxBurstFactor.value.toInt).foreach(_ =>
        limiter.checkAndUpdateRate(node1) shouldBe true
      )
      limiter.checkAndUpdateRate(node1) shouldBe false

      // Advancing only the independent monotonic source (the wall clock is not involved here) refills one
      //  token, so a request resumes even though wall time did not move forward.
      nanos.addAndGet(Duration.ofSeconds(1).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe true
      // ...and without further advancement it is rate-limited again.
      limiter.checkAndUpdateRate(node1) shouldBe false

      // Refilling keeps working on subsequent advances.
      nanos.addAndGet(Duration.ofSeconds(1).toNanos)
      limiter.checkAndUpdateRate(node1) shouldBe true
      limiter.checkAndUpdateRate(node1) shouldBe false
    }
  }
}
