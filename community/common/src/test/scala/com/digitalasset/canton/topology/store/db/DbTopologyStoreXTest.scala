// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreXTest}

import scala.concurrent.Future

trait DbTopologyStoreXTest extends TopologyStoreXTest {

  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table topology_transactions_x"
      ),
      operationName = s"${this.getClass}: Truncate tables topology_transactions_x",
    )
  }

  private def createTopologyStore(
      storeId: TopologyStoreId = TopologyStoreId.DomainStore(
        DefaultTestIdentities.domainId,
        // Using test name as discriminator to avoid db-unit-test clashes such as non-X DbTopologyStoreTest.
        // We reuse the `topology_dispatching` table from non-X topology management.
        "DbTopologyStoreXTest",
      )
  ): DbTopologyStoreX[TopologyStoreId] =
    new DbTopologyStoreX(
      storage,
      storeId,
      timeouts,
      loggerFactory,
    )

  "DbTopologyStore" should {
    behave like topologyStore(() => createTopologyStore())
  }

}

class TopologyStoreXTestPostgres extends DbTopologyStoreXTest with PostgresTest {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}

class TopologyStoreXTestH2 extends DbTopologyStoreXTest with H2Test {

  // TODO(#12373) remove this from unstable/dev when releasing BFT
  override val migrationMode: MigrationMode = MigrationMode.DevVersion
}
