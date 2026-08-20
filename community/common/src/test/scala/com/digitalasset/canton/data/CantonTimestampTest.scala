// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class CantonTimestampTest extends AnyWordSpec with BaseTest {

  "assertFromInstant" should {

    "not fail when the instant must lose precision" in {

      val instantWithNanos = Instant.EPOCH.plusNanos(300L)
      val cantonTimestamp = CantonTimestamp.assertFromInstant(instantWithNanos)
      cantonTimestamp shouldEqual CantonTimestamp.Epoch
    }
  }

  "out of bounds CantonTimestamp" should {

    "throw exception for underflow" in {
      assertThrows[IllegalArgumentException]({
        val tooLow = CantonTimestamp.MinValue.getEpochSecond - 1
        CantonTimestamp.ofEpochSecond(tooLow)
      })
    }

    "throw exception for overflow" in {
      assertThrows[IllegalArgumentException]({
        val tooLarge = CantonTimestamp.MaxValue.getEpochSecond + 1
        CantonTimestamp.ofEpochSecond(tooLarge)
      })
    }
  }

  "fromString and tryFromString" should {
    "return correct result" in {
      val str = "2025-11-27T06:50:55.123Z"
      val t = Instant.parse(str)
      val expectedResult = CantonTimestamp.assertFromInstant(t)

      CantonTimestamp.assertFromString(str) shouldBe expectedResult
      CantonTimestamp.fromString(str).value shouldBe expectedResult
    }

    "compose to identity with toString" in {
      val t = CantonTimestamp.now()
      CantonTimestamp.assertFromString(t.toString) shouldBe t
    }

    "fail on invalid string" in {
      val str = "meh"

      CantonTimestamp.fromString(str).left.value should include(
        s"Unable to parse $str as CantonTimestamp"
      )
      val exception = intercept[IllegalArgumentException](CantonTimestamp.assertFromString(str))
      exception.getMessage should include(s"Unable to parse $str as CantonTimestamp")
    }
  }

  "safeAdd" should {
    "add the duration" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofSeconds(1)).value shouldBe
        CantonTimestamp.ofEpochSecond(1)
    }

    "keep sub-second precision" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofNanos(1500)).value shouldBe
        CantonTimestamp.Epoch.addMicros(1)
    }

    "subtract a negative duration" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofSeconds(-1)).value shouldBe
        CantonTimestamp.ofEpochSecond(-1)
    }

    "round a negative sub-microsecond duration towards the past" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofNanos(-1500)).value shouldBe
        CantonTimestamp.Epoch.addMicros(-2)
    }

    "reach the upper bound" in {
      CantonTimestamp.Epoch
        .safeAdd(Duration.between(Instant.EPOCH, CantonTimestamp.MaxValue.toInstant))
        .value shouldBe CantonTimestamp.MaxValue
    }

    "reach the lower bound" in {
      CantonTimestamp.Epoch
        .safeAdd(Duration.between(Instant.EPOCH, CantonTimestamp.MinValue.toInstant))
        .value shouldBe CantonTimestamp.MinValue
    }

    "fail when the result is out of bounds" in {
      CantonTimestamp.MaxValue.safeAdd(Duration.ofNanos(1000)).left.value should
        include("out of bound Timestamp")
    }

    "fail when the result is below the lower bound" in {
      CantonTimestamp.MinValue.safeAdd(Duration.ofNanos(-1000)).left.value should
        include("out of bound Timestamp")
    }

    "fail when the addition overflows" in {
      CantonTimestamp.MaxValue
        .safeAdd(Duration.ofSeconds(Long.MaxValue / 1_000_000L))
        .left
        .value should include("overflows")
    }

    "fail when the addition underflows" in {
      CantonTimestamp.MinValue
        .safeAdd(Duration.ofSeconds(-(Long.MaxValue / 1_000_000L)))
        .left
        .value should include("overflows")
    }

    "fail when the duration cannot be expressed in microseconds" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofSeconds(Long.MaxValue)).left.value should
        include("cannot be expressed in microseconds")
    }

    "fail when a negative duration cannot be expressed in microseconds" in {
      CantonTimestamp.Epoch.safeAdd(Duration.ofSeconds(Long.MinValue)).left.value should
        include("cannot be expressed in microseconds")
    }

    "accept a refined duration" in {
      CantonTimestamp.Epoch.safeAdd(NonNegativeFiniteDuration.tryOfSeconds(1)).value shouldBe
        CantonTimestamp.ofEpochSecond(1)
    }
  }

  "minus" should {
    "measure the duration" in {
      CantonTimestamp.ofEpochSecond(1) - CantonTimestamp.Epoch shouldBe
        java.time.Duration.ofSeconds(1)
    }

    "not overflow" in {
      CantonTimestamp.Epoch - CantonTimestamp.MinValue shouldBe
        java.time.Duration.ofSeconds(CantonTimestamp.MinValue.toMicros / -1_000_000L)
    }
  }
}
