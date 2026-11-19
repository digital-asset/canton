// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.SimClock
import com.digitalasset.nonempty.NonEmptyUtil

import java.time

class UniqueTimestampGeneratorTest extends BaseTestWordSpec {
  "ensures timestamps are unique and increasing even if time doesn't move" in {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val generator = new UniqueTimestampGenerator(clock)

    val timestamps = (0 until 10).map(_ => generator.generateNext)

    withClue("should all be unique") {
      timestamps.toSet should have size (timestamps.size.toLong)
    }

    withClue("should increase") {
      NonEmptyUtil.fromUnsafe(timestamps).reduceLeft[CantonTimestamp] { case (first, second) =>
        first shouldBe <(second)
        second
      }
    }
  }

  "generating faster than clock is advancing" in {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val generator = new UniqueTimestampGenerator(clock)

    val timestamps = (0 until 10) flatMap { _ =>
      val ts1 = generator.generateNext
      val ts2 = generator.generateNext

      clock.advance(time.Duration.ofNanos(1000))

      Seq(ts1, ts2)
    }

    withClue("timestamps should be unique") {
      timestamps.toSet should have size (timestamps.size.toLong)
    }

    withClue("should increase") {
      NonEmptyUtil.fromUnsafe(timestamps).reduceLeft[CantonTimestamp] { case (first, second) =>
        first shouldBe <(second)
        second
      }
    }
  }
}
