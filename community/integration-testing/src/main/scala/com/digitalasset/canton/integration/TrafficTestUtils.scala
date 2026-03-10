// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters

/** Utilities to configure traffic control in tests
  */
object TrafficTestUtils {
  // Free confirmation responses and no base cost
  // This means essentially only topology transactions and confirmation requests cost traffic
  // which makes it easier to make assertion on traffic spent by nodes
  // For maximum predictability in tests, disable ACS commitments
  val predictableTraffic: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeLong.zero,
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.zero,
    freeConfirmationResponses = true,
  )
}
