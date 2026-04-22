// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.RequireTypes.{DoubleGreaterEqual1, PositiveInt}
import com.digitalasset.canton.platform.config.UpdateServiceConfig.{
  AscendingFirstPageDynamicBoundOverfetch,
  DefaultUpdatesPageSize,
  UpdatePageSizeLimit,
}

/** @param maxUpdatesPageSize
  *   inclusive limit for max_page_size for getUpdatesPage. Requests exceeding this value will be
  *   rejected.
  * @param defaultUpdatesPageSize
  *   max_page_size value for getUpdatesPage request without max_page_size specified
  *
  * @param ascendingFirstPageDynamicBoundOverfetch
  *   A multiplier by which the page size is multiplied when fetching an ascending updates page with
  *   dynamic lower bound to avoid interference with pruning offset. The implementation fetches
  *   (pageSize+1)*ascendingFirstPageDynamicBoundOverfetch elements and if pruning offset in a
  *   meantime moved in a way that less than pageSize+1 elements are after the pruning offset, the
  *   page fetch fails, otherwise the first page is successfully returned. Try increasing this value
  *   if fetching the first page with dynamic lower bound fails ofter.
  */
final case class UpdateServiceConfig(
    maxUpdatesPageSize: PositiveInt = UpdatePageSizeLimit,
    defaultUpdatesPageSize: PositiveInt = DefaultUpdatesPageSize,
    ascendingFirstPageDynamicBoundOverfetch: DoubleGreaterEqual1 =
      AscendingFirstPageDynamicBoundOverfetch,
)

object UpdateServiceConfig {
  private val DefaultUpdatesPageSize =
    PositiveInt.tryCreate(100)
  private val UpdatePageSizeLimit =
    PositiveInt.tryCreate(2000)
  private val AscendingFirstPageDynamicBoundOverfetch: DoubleGreaterEqual1 =
    DoubleGreaterEqual1.tryCreate(4)
}
