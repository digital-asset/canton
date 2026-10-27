// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

final class AutomaticAssignmentTest extends AnyWordSpec with BaseTest {

  "randomizedDelay" should {
    "be between the configured delay and twice the configured delay" in {
      val delay = NonNegativeFiniteDuration.ofSeconds(10)

      (1 to 1000).foreach { _ =>
        val randomizedDelay = AutomaticAssignment.randomizedDelay(delay)
        randomizedDelay.compareTo(delay.asJava) should be >= 0
        randomizedDelay.compareTo(delay.asJava.multipliedBy(2)) should be < 0
      }
    }

    "be zero if the configured delay is zero" in {
      AutomaticAssignment.randomizedDelay(NonNegativeFiniteDuration.Zero) shouldBe Duration.ZERO
    }
  }
}
