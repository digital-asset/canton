// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Configuration for monitoring the cost of db queries.
  *
  * @param every determines the duration between reports
  * @param resetOnOutput determines whether the statistics will be reset after creating a report
  */
final case class QueryCostMonitoringConfig(
    every: NonNegativeFiniteDuration,
    resetOnOutput: Boolean = true,
)
