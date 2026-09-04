// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

/** @param maxRootViews
  *   maximum number of root level views that a transaction may have
  * @param maxSubViews
  *   maximum number of subviews that any transaction may have
  */
final case class TransactionViewLimitConfig(maxRootViews: Int, maxSubViews: Int)

object TransactionViewLimitConfig {
  val Off: TransactionViewLimitConfig = TransactionViewLimitConfig(
    maxRootViews = Int.MaxValue,
    maxSubViews = Int.MaxValue,
  )

  // TODO (#35427): should be configured using static synchronizer parameters
  val Default: TransactionViewLimitConfig = TransactionViewLimitConfig(
    maxRootViews = Int.MaxValue,
    maxSubViews = Int.MaxValue,
  )
}
