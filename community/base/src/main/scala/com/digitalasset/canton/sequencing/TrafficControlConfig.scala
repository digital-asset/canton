// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveNumeric}
import com.digitalasset.canton.sequencing.TrafficControlConfig.{
  DefaultBaseRate,
  DefaultMaxBurstDuration,
}

/** Traffic control configuration values - stored as dynamic domain parameters
  *
  * @param baseRate amount of byte per second acquired as "free" traffic per member
  * @param storageCostMultiplier multiplier used to compute storage cost of an event
  * @param deliveryCostMultiplier multiplier used to compute delivery cost of an event
  * @param maxBurstDuration maximum amount of time the base rate traffic will accumulate before being capped
  */
// TODO(i12858): Move to dynamic domain parameters
final case class TrafficControlConfig(
    baseRate: NonNegativeLong = DefaultBaseRate,
    storageCostMultiplier: PositiveInt = PositiveNumeric.tryCreate(1),
    deliveryCostMultiplier: PositiveInt = PositiveNumeric.tryCreate(1),
    maxBurstDuration: config.NonNegativeDuration = DefaultMaxBurstDuration,
) {
  lazy val maxBurst: NonNegativeLong =
    baseRate * NonNegativeLong.tryCreate(maxBurstDuration.duration.toSeconds)
}

object TrafficControlConfig {
  val DefaultBaseRate: NonNegativeLong = NonNegativeLong.tryCreate(333L)
  val DefaultMaxBurstDuration: config.NonNegativeDuration =
    config.NonNegativeDuration.ofMinutes(10L)
}
