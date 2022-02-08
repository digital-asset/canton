// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import java.time.Instant

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class CantonTimestampTest extends AnyWordSpec with BaseTest {

  "assertFromInstant" should {

    "not fail when the instant must lose precision" in {

      val instantWithNanos = Instant.EPOCH.plusNanos(300L)
      val cantonTimestamp = CantonTimestamp.assertFromInstant(instantWithNanos)
      cantonTimestamp shouldEqual CantonTimestamp.Epoch
    }
  }
}
