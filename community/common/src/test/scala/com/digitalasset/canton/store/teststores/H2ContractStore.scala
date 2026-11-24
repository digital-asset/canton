// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.teststores

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{BatchAggregatorConfig, CacheConfig}
import com.digitalasset.canton.participant.store.db.DbContractStore
import org.scalatest.Suite

trait H2ContractStore extends H2StoresTest {
  self: Suite with TestEssentials =>

  def createContractStore(): DbContractStore = new DbContractStore(
    storage = inMemoryH2Storage,
    cacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(100)),
    readBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
    writeBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
    insertMaxBatchSizeBytes = PositiveNumeric.tryCreate(1024 * 1024),
    timeouts = timeouts,
    loggerFactory = loggerFactory,
  )(h2InMemoryEc)

  override protected def tablesToClean: Seq[String] = super.tablesToClean :+ "par_contracts"
}
