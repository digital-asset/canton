// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.sequencing.TrafficControlConfig.{
  DefaultBaseRate,
  DefaultMaxBurstDuration,
}

/** Traffic control configuration values - stored as dynamic domain parameters
  *
  * @param baseRate amount of byte per second acquired as "free" traffic per member
  * @param readVsWriteScalingFactor multiplier used to compute cost of an event. In per ten-mil (1 / 10 000). Defaults to 200 (=2%).
  *                                 A multiplier of 2% means the base cost will be increased by 2% to produce the effective cost.
  * @param maxBurstDuration maximum amount of time the base rate traffic will accumulate before being capped
  */
// TODO(i12858): Move to dynamic domain parameters
final case class TrafficControlConfig(
    baseRate: NonNegativeLong = DefaultBaseRate,
    readVsWriteScalingFactor: PositiveInt = PositiveInt.tryCreate(200),
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
