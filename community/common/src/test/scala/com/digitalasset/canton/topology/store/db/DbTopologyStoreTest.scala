// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.topology.client.DbStoreBasedTopologySnapshotTest
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreTest}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}

import scala.concurrent.Future

trait DbTopologyStoreTest extends TopologyStoreTest {

  this: DbTest =>

  val pureCryptoApi: CryptoPureApi = TestingIdentityFactory.pureCrypto()

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._
    storage.update(
      DBIO.seq(
        sqlu"truncate table party_metadata",
        sqlu"truncate table topology_transactions",
      ),
      operationName = s"${this.getClass}: Truncate tables party_metadata and topology_transactions",
    )
  }

  "DbPartyMetadataStore" should {
    behave like partyMetadataStore(() => new DbPartyMetadataStore(storage, timeouts, loggerFactory))
  }

  private def createTopologyStore(): DbTopologyStore =
    new DbTopologyStore(
      storage,
      TopologyStoreId.AuthorizedStore,
      maxItemsInSqlQuery = 1,
      timeouts,
      loggerFactory,
    )

  "DbTopologyStore" should {
    behave like topologyStore(() => createTopologyStore())
  }

  "Storage implicits" should {
    "should be stack safe" in {
      val tmp = storage
      import tmp.api._
      import DbStorage.Implicits.BuilderChain._
      (1 to 100000).map(_ => sql" 1 == 1").toList.intercalate(sql" AND ").discard
      assert(true)
    }
  }

}

class TopologyStoreTestH2
    extends DbTopologyStoreTest
    with DbStoreBasedTopologySnapshotTest
    with H2Test

// merging both tests into a single runner, as both tests will write to topology_transactions, creating conflicts
class TopologyStoreTestPostgres
    extends DbTopologyStoreTest
    with DbStoreBasedTopologySnapshotTest
    with PostgresTest
