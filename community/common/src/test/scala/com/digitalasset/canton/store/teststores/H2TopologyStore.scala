// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.teststores

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.store.IndexedTopologyStoreId
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import org.scalatest.Suite

trait H2TopologyStore extends H2StoresTest {
  self: Suite & TestEssentials =>

  def createTopologyStore[StoreId <: TopologyStoreId](
      storeId: StoreId,
      storeIndex: Int,
      loggerFactory: NamedLoggerFactory = self.loggerFactory,
  ): DbTopologyStore[StoreId] =
    new DbTopologyStore[StoreId](
      storage = inMemoryH2Storage,
      storeId = storeId,
      storeIndex = IndexedTopologyStoreId.tryCreate(storeId, storeIndex),
      predecessor = None,
      protocolVersion = testedProtocolVersion,
      timeouts = timeouts,
      batchingConfig = BatchingConfig(),
      loggerFactory = loggerFactory,
    )(h2InMemoryEc)

  override protected def tablesToClean: Seq[String] =
    super.tablesToClean ++ Seq("common_topology_transactions", "common_topology_dispatching")
}
