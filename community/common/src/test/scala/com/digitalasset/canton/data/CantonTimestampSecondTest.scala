// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class CantonTimestampSecondTest extends AnyWordSpec with BaseTest {

  "CantonTimestampSecond" should {

    "have factory method to build from Instant" in {
      val i = Instant.parse("2022-12-12T12:00:00Z")
      CantonTimestampSecond.fromInstant(i).value.underlying.toInstant shouldBe i

      CantonTimestampSecond
        .fromInstant(Instant.parse("2022-12-12T12:00:00.500Z"))
        .left
        .value shouldBe a[String]
    }

    "have factory method to build from CantonTimestamp" in {
      val tsRounded = CantonTimestamp.fromInstant(Instant.parse("2022-12-12T12:00:00Z")).value
      val tsNotRounded = tsRounded + NonNegativeFiniteDuration.tryOfMillis(500)

      CantonTimestampSecond
        .fromCantonTimestamp(tsRounded)
        .value
        .underlying
        .toInstant shouldBe tsRounded.toInstant

      CantonTimestampSecond
        .fromCantonTimestamp(tsNotRounded)
        .left
        .value shouldBe a[String]
    }

    "have floor factory method" in {
      val instant0 = Instant.parse("1500-06-12T12:00:00Z")
      val instant1 = Instant.parse("1500-06-12T12:00:00.200Z")
      val instant2 = Instant.parse("1500-06-12T12:00:00.500Z")
      val instant3 = Instant.parse("1500-06-12T12:00:00Z")

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant0).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant1).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant2).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant3).value)
        .toInstant shouldBe instant3
    }

    "have ceil factory method" in {
      val instant0 = Instant.parse("1500-06-12T12:00:00Z")
      val instant1 = Instant.parse("1500-06-12T12:00:00.200Z")
      val instant2 = Instant.parse("1500-06-12T12:00:00.500Z")
      val instant3 = Instant.parse("1500-06-12T12:00:00Z")

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant0).value)
        .toInstant shouldBe instant0

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant1).value)
        .toInstant shouldBe instant3

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant2).value)
        .toInstant shouldBe instant3

      CantonTimestampSecond
        .floor(CantonTimestamp.fromInstant(instant3).value)
        .toInstant shouldBe instant3
    }
  }

  private val fullRange: PositiveSeconds = PositiveSeconds.tryOfSeconds(
    CantonTimestampSecond.MaxValue.getEpochSecond - CantonTimestampSecond.MinValue.getEpochSecond
  )

  "safeAdd" should {
    "add the duration" in {
      CantonTimestampSecond.Epoch.safeAdd(PositiveSeconds.tryOfSeconds(1)).value shouldBe
        CantonTimestampSecond.ofEpochSecond(1)
    }

    "reach the upper bound" in {
      CantonTimestampSecond.MinValue.safeAdd(fullRange).value shouldBe
        CantonTimestampSecond.MaxValue
    }

    "fail when the result is above the upper bound" in {
      CantonTimestampSecond.MaxValue.safeAdd(PositiveSeconds.tryOfSeconds(1)).left.value should
        include("out of bound Timestamp")
    }

    "fail when the addition overflows" in {
      CantonTimestampSecond.MaxValue
        .safeAdd(PositiveSeconds.tryOfSeconds(Long.MaxValue / 1_000_000L))
        .left
        .value should include("overflows")
    }
  }

  "safeSubtract" should {
    "subtract the duration" in {
      CantonTimestampSecond.Epoch.safeSubtract(PositiveSeconds.tryOfSeconds(1)).value shouldBe
        CantonTimestampSecond.ofEpochSecond(-1)
    }

    "reach the lower bound" in {
      CantonTimestampSecond.MaxValue.safeSubtract(fullRange).value shouldBe
        CantonTimestampSecond.MinValue
    }

    "fail when the result is below the lower bound" in {
      CantonTimestampSecond.MinValue.safeSubtract(PositiveSeconds.tryOfSeconds(1)).left.value should
        include("out of bound Timestamp")
    }

    "fail when the subtraction overflows" in {
      CantonTimestampSecond.MinValue
        .safeSubtract(PositiveSeconds.tryOfSeconds(Long.MaxValue / 1_000_000L))
        .left
        .value should include("overflows")
    }
  }

  "safeAddSeconds" should {
    "add the seconds" in {
      CantonTimestampSecond.Epoch.safeAddSeconds(1).value shouldBe
        CantonTimestampSecond.ofEpochSecond(1)
    }

    "subtract negative seconds" in {
      CantonTimestampSecond.Epoch.safeAddSeconds(-1).value shouldBe
        CantonTimestampSecond.ofEpochSecond(-1)
    }

    "fail when the result is out of bounds" in {
      CantonTimestampSecond.MaxValue.safeAddSeconds(1).left.value should
        include("out of bound Timestamp")
      CantonTimestampSecond.MinValue.safeAddSeconds(-1).left.value should
        include("out of bound Timestamp")
    }

    "fail when the seconds cannot be expressed in microseconds" in {
      CantonTimestampSecond.Epoch.safeAddSeconds(Long.MaxValue).left.value should
        include("cannot be expressed in microseconds")
      CantonTimestampSecond.Epoch.safeAddSeconds(Long.MinValue).left.value should
        include("cannot be expressed in microseconds")
    }
  }

  "safeSubtractSeconds" should {
    "subtract the seconds" in {
      CantonTimestampSecond.Epoch.safeSubtractSeconds(1).value shouldBe
        CantonTimestampSecond.ofEpochSecond(-1)
    }

    "add negative seconds" in {
      CantonTimestampSecond.Epoch.safeSubtractSeconds(-1).value shouldBe
        CantonTimestampSecond.ofEpochSecond(1)
    }

    "fail when the result is out of bounds" in {
      CantonTimestampSecond.MinValue.safeSubtractSeconds(1).left.value should
        include("out of bound Timestamp")
      CantonTimestampSecond.MaxValue.safeSubtractSeconds(-1).left.value should
        include("out of bound Timestamp")
    }

    "fail instead of throwing for Long.MinValue" in {
      CantonTimestampSecond.Epoch.safeSubtractSeconds(Long.MinValue).left.value should
        include("cannot be negated")
    }

    "fail when the seconds cannot be expressed in microseconds" in {
      CantonTimestampSecond.Epoch.safeSubtractSeconds(Long.MaxValue).left.value should
        include("cannot be expressed in microseconds")
    }
  }

  "minus" should {
    "measure the duration" in {
      CantonTimestampSecond.ofEpochSecond(1) - CantonTimestampSecond.Epoch shouldBe
        Duration.ofSeconds(1)
      CantonTimestampSecond.Epoch - CantonTimestampSecond.ofEpochSecond(1) shouldBe
        Duration.ofSeconds(-1)
    }

    "not overflow" in {
      CantonTimestampSecond.MaxValue - CantonTimestampSecond.MinValue shouldBe
        Duration.ofSeconds(fullRange.duration.getSeconds)
      CantonTimestampSecond.MinValue - CantonTimestampSecond.MaxValue shouldBe
        Duration.ofSeconds(-fullRange.duration.getSeconds)
    }
  }
}
