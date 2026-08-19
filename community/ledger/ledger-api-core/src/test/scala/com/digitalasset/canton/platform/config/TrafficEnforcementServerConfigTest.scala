// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.PositiveFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

class TrafficEnforcementServerConfigTest extends AnyWordSpec with BaseTest {

  "TrafficEnforcementServerConfig.Internal" should {

    "accept the defaults, where databaseQueryTimeout is strictly less than accountLookupTimeout" in {
      noException should be thrownBy TrafficEnforcementServerConfig.Internal()
    }

    "reject a databaseQueryTimeout equal to accountLookupTimeout" in {
      intercept[IllegalArgumentException](
        TrafficEnforcementServerConfig.Internal(
          databaseQueryTimeout = PositiveFiniteDuration.ofSeconds(5),
          accountLookupTimeout = PositiveFiniteDuration.ofSeconds(5),
        )
      )
    }

    "reject a databaseQueryTimeout greater than accountLookupTimeout" in {
      intercept[IllegalArgumentException](
        TrafficEnforcementServerConfig.Internal(
          databaseQueryTimeout = PositiveFiniteDuration.ofSeconds(10),
          accountLookupTimeout = PositiveFiniteDuration.ofSeconds(5),
        )
      )
    }

    "reject a databaseQueryTimeout below one millisecond" in {
      intercept[IllegalArgumentException](
        TrafficEnforcementServerConfig.Internal(
          databaseQueryTimeout = PositiveFiniteDuration.tryFromDuration(500.micros),
          accountLookupTimeout = PositiveFiniteDuration.ofSeconds(5),
        )
      )
    }
  }
}
