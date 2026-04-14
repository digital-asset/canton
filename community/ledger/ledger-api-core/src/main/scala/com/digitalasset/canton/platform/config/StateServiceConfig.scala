// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.platform.config.StateServiceConfig.*

/** @param maxAcsPageSize
  *   inclusive limit for max_page_size for GetActiveContractsPage. Requests exceeding this value
  *   will be rejected.
  * @param defaultAcsPageSize
  *   max_page_size value for GetActiveContractsPage request without max_page_size specified
  */
final case class StateServiceConfig(
    maxAcsPageSize: PositiveInt = AcsPageSizeLimit,
    defaultAcsPageSize: PositiveInt = DefaultAcsPageSize,
)

object StateServiceConfig {
  private val DefaultAcsPageSize = PositiveInt.tryCreate(100)
  private val AcsPageSizeLimit = PositiveInt.tryCreate(1000)
}
