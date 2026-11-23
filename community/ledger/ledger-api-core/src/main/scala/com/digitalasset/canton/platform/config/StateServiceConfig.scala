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
  * @param maxHostedPartiesPageSize
  *   inclusive limit for max_page_size for GetHostedPartiesPage. Requests exceeding this value will
  *   be rejected.
  * @param defaultHostedPartiesPageSize
  *   max_page_size value for GetHostedPartiesPage request without max_page_size specified
  */
final case class StateServiceConfig(
    maxAcsPageSize: PositiveInt = AcsPageSizeLimit,
    defaultAcsPageSize: PositiveInt = DefaultAcsPageSize,
    maxHostedPartiesPageSize: PositiveInt = HostedPartiesPageSizeLimit,
    defaultHostedPartiesPageSize: PositiveInt = DefaultHostedPartiesPageSize,
)

object StateServiceConfig {
  private val DefaultAcsPageSize = PositiveInt.tryCreate(2000)
  private val AcsPageSizeLimit = PositiveInt.tryCreate(10000)
  private val DefaultHostedPartiesPageSize = PositiveInt.tryCreate(1000)
  private val HostedPartiesPageSizeLimit = PositiveInt.tryCreate(10000)
}
