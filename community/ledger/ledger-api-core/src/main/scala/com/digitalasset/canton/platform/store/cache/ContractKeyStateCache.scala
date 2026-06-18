// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.caching.SizedCache
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.daml.lf.transaction.GlobalKey

import scala.concurrent.ExecutionContext

object ContractKeyStateCache {
  def apply(
      initialCacheEventSeqIdIndex: Long,
      cacheSize: Long,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): StateCache[GlobalKey, ContractKeyStateValue] =
    StateCache(
      initialCacheEventSeqIdIndex = initialCacheEventSeqIdIndex,
      emptyLedgerState = ContractKeyStateValue.Empty,
      cache = SizedCache.from[GlobalKey, ContractKeyStateValue](
        SizedCache.Configuration(cacheSize),
        metrics.execution.cache.keyState.stateCache,
      ),
      registerUpdateTimer = metrics.execution.cache.keyState.registerCacheUpdate,
      loggerFactory = loggerFactory,
    )
}

sealed trait ContractKeyStateValue extends Product with Serializable

object ContractKeyStateValue {

  /** @param contractId
    *   The contract ID assigned to this key.
    * @param eventSequentialId
    *   The event sequential ID at which this assignment was observed.
    * @param thereMightBeMore
    *   Flag indicating whether there might be older active contracts for the same key in the DB.
    */
  final case class Last(
      contractId: ContractId,
      eventSequentialId: Long,
      thereMightBeMore: Boolean,
  ) extends ContractKeyStateValue

  case object Empty extends ContractKeyStateValue
}
