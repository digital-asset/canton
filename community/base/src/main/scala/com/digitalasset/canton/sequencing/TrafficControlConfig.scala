// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveNumeric}

/** Traffic control configuration values - stored as dynamic domain parameters
  *
  * @param baseRate amount of byte per second acquired as "free" traffic per member
  * @param storageCostMultiplier multiplier used to compute storage cost of an event
  * @param deliveryCostMultiplier multiplier used to compute delivery cost of an event
  * @param maxBurst maximum "free" traffic from the base rate a member can accumulate
  */
// TODO(i12858): Move to dynamic domain parameters
final case class TrafficControlConfig(
    baseRate: NonNegativeNumeric[Long] = NonNegativeNumeric.tryCreate(20L),
    storageCostMultiplier: PositiveNumeric[Int] = PositiveNumeric.tryCreate(1),
    deliveryCostMultiplier: PositiveNumeric[Int] = PositiveNumeric.tryCreate(1),
    maxBurst: PositiveNumeric[Long] = PositiveNumeric.tryCreate(60L * 20L),
)
