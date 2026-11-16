// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.EitherValues.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import scala.util.Success

class DeduplicationPeriodSpec extends AnyWordSpec with Matchers {
  "calculating deduplication until" should {
    val time = Timestamp.assertFromLong(100 * 1000 * 1000)

    "return expected result when sending duration" in {
      val deduplicateUntil = DeduplicationPeriod.deduplicateUntil(
        time,
        DeduplicationPeriod.DeduplicationDuration.create(Duration.ofSeconds(3)).value,
      )
      deduplicateUntil shouldEqual Success(time.add(Duration.ofSeconds(3)))
    }

    "accept long durations" in {
      val duration = Duration.ofDays(365 * 10000)
      DeduplicationPeriod.DeduplicationDuration
        .create(duration)
        .value
        .duration shouldBe duration
    }

    "accept zero durations" in {
      val duration = Duration.ZERO
      DeduplicationPeriod.DeduplicationDuration
        .create(duration)
        .value
        .duration shouldBe duration

      DeduplicationPeriod.DeduplicationDuration.Zero.duration shouldBe duration
    }

    "not accept negative durations" in {
      val duration = Duration.ofSeconds(-1)
      DeduplicationPeriod.DeduplicationDuration
        .create(duration)
        .left
        .value should include("not be negative")
    }

    "accept microsecond durations" in {
      val duration = Duration.ofNanos(1000)
      DeduplicationPeriod.DeduplicationDuration
        .create(duration)
        .value
        .duration shouldBe duration
    }

    "not accept nanosecond durations" in {
      val duration = Duration.ofNanos(1001)
      DeduplicationPeriod.DeduplicationDuration
        .create(duration)
        .left
        .value should include("not use nanosecond precision")
    }
  }
}
