// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{IndexedStringType, IndexedSynchronizer}
import com.digitalasset.canton.tracing.TraceContext

sealed trait DbPartyReplicationIndexingStoreTest
    extends BaseTest
    with PartyReplicationIndexingStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table par_party_replication_indexing"),
      "clean-up par_party_replication_indexing for test",
    )
  }

  "DbPartyReplicationIndexingStore" should {
    behave like partyReplicationIndexingStore { () =>
      val indexStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)
      val synchronizerId = IndexedSynchronizer.tryCreate(
        storeSynchronizerId,
        indexStore.getOrCreateIndexForTesting(
          IndexedStringType.synchronizerId,
          storeSynchronizerStr,
        ),
      )
      new DbPartyReplicationIndexingStore(
        storage,
        synchronizerId,
        timeouts,
        loggerFactory,
      )
    }
  }
}

class DbPartyReplicationIndexingStoreTestH2 extends DbPartyReplicationIndexingStoreTest with H2Test

class DbPartyReplicationIndexingStoreTestPostgres
    extends DbPartyReplicationIndexingStoreTest
    with PostgresTest
