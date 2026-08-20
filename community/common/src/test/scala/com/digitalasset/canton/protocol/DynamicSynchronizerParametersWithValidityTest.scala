// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

class DynamicSynchronizerParametersWithValidityTest extends AnyWordSpec with BaseTest {

  private def withExclusivityTimeout(
      timeout: NonNegativeFiniteDuration
  ): DynamicSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      DynamicSynchronizerParameters
        .defaultValues(testedProtocolVersion)
        .tryUpdate(assignmentExclusivityTimeout = timeout),
      CantonTimestamp.MinValue,
      None,
    )

  "assignmentExclusivityLimitFor" should {

    "add the exclusivity timeout to the baseline" in {
      withExclusivityTimeout(NonNegativeFiniteDuration.tryOfSeconds(10L))
        .assignmentExclusivityLimitFor(CantonTimestamp.Epoch)
        .value shouldBe CantonTimestamp.ofEpochSecond(10L)
    }

    "fail if the parameters are not valid at the baseline" in {
      withExclusivityTimeout(NonNegativeFiniteDuration.tryOfSeconds(10L))
        .assignmentExclusivityLimitFor(CantonTimestamp.MinValue)
        .left
        .value should include("validity of parameters is")
    }

    "fail instead of exceeding the maximal timestamp" in {
      withExclusivityTimeout(NonNegativeFiniteDuration.tryOfDays(365L * 10000L))
        .assignmentExclusivityLimitFor(CantonTimestamp.Epoch)
        .left
        .value should include("out of bound Timestamp")
    }

    "fail instead of overflowing" in {
      withExclusivityTimeout(NonNegativeFiniteDuration.tryOfSeconds(Long.MaxValue / 1_000_000L))
        .assignmentExclusivityLimitFor(CantonTimestamp.MaxValue)
        .left
        .value should include("overflows")
    }
  }
}
