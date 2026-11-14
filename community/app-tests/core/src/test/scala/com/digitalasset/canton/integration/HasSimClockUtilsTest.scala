// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.HasSimClockUtils.advanceClockInStepsTo
import com.digitalasset.canton.time.{PositiveFiniteDuration, SimClock}
import org.scalatest.wordspec.AnyWordSpec

final class HasSimClockUtilsTest extends AnyWordSpec with BaseTest {

  "advanceClockTo" should {
    "stop exactly at the target and run the callback after every bounded step" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      var observed = Seq.empty[CantonTimestamp]
      val target = clock.now.plusSeconds(25)

      advanceClockInStepsTo(clock, target, maxStep = PositiveFiniteDuration.tryOfSeconds(10)) {
        observed = observed :+ clock.now
      }

      observed shouldBe Seq(10L, 20L, 25L).map(CantonTimestamp.Epoch.plusSeconds)
      clock.now shouldBe target
    }

    "honor a custom maximum step" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      var observed = Seq.empty[CantonTimestamp]
      val target = clock.now.plusSeconds(65)

      advanceClockInStepsTo(clock, target, maxStep = PositiveFiniteDuration.tryOfSeconds(30)) {
        observed = observed :+ clock.now
      }

      observed shouldBe Seq(30L, 60L, 65L).map(CantonTimestamp.Epoch.plusSeconds)
      clock.now shouldBe target
    }

    "reject steps smaller than the timestamp resolution without invoking the callback" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val step = PositiveFiniteDuration.tryCreate(java.time.Duration.ofNanos(500))

      val thrown = intercept[IllegalArgumentException] {
        advanceClockInStepsTo(clock, clock.now.plusSeconds(1), maxStep = step) {
          fail("The callback must not run for a step below the timestamp resolution")
        }
      }

      thrown.getMessage should include("maxStep must be at least")
      clock.now shouldBe CantonTimestamp.Epoch
    }

    "accept a step equal to the timestamp resolution" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val target = clock.now.immediateSuccessor
      var callbacks = 0

      advanceClockInStepsTo(
        clock,
        target,
        maxStep = PositiveFiniteDuration.tryCreate(java.time.Duration.ofNanos(1000)),
      ) {
        callbacks += 1
      }

      clock.now shouldBe target
      callbacks shouldBe 1
    }

    "do nothing when the target has already been reached" in {
      val start = CantonTimestamp.Epoch.plusSeconds(20)
      val clock = new SimClock(start, loggerFactory)

      Seq(start, start.minusSeconds(1)).foreach { target =>
        advanceClockInStepsTo(clock, target, maxStep = PositiveFiniteDuration.tryOfSeconds(10)) {
          fail("The callback must not run without a clock advance")
        }
        clock.now shouldBe start
      }
    }

    "propagate a callback failure without advancing again" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val failure = new IllegalStateException("Synchronization failed")

      val thrown = intercept[IllegalStateException] {
        advanceClockInStepsTo(
          clock,
          clock.now.plusSeconds(25),
          maxStep = PositiveFiniteDuration.tryOfSeconds(10),
        ) {
          throw failure
        }
      }

      thrown shouldBe failure
      clock.now shouldBe CantonTimestamp.Epoch.plusSeconds(10)
    }
  }
}
