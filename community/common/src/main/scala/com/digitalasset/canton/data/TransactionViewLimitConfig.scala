// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.TransactionProtocolLimits

/** @param maxRootViews
  *   maximum number of root level views that a transaction may have
  * @param maxSubViews
  *   maximum number of subviews that any transaction may have
  * @param maxTreeDepth
  *   the maximum parse depth of the view
  */
final case class TransactionViewLimitConfig(
    maxRootViews: PositiveInt,
    maxSubViews: PositiveInt,
    maxTreeDepth: PositiveInt,
)

object TransactionViewLimitConfig {
  val Off: TransactionViewLimitConfig =
    TransactionViewLimitConfig(TransactionProtocolLimits.max)

  def apply(
      protocolLimits: TransactionProtocolLimits
  ): TransactionViewLimitConfig = TransactionViewLimitConfig(
    maxRootViews = protocolLimits.maxTransactionRootViews,
    maxSubViews = protocolLimits.maxTransactionSubViews,
    maxTreeDepth = protocolLimits.maxTransactionTreeDepth,
  )
}
