// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveNumeric}
import com.digitalasset.canton.data.CantonTimestamp.ofEpochSecond
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.data.UnassignmentData.UnassignmentGlobalOffset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{MonadUtil, ReassignmentTag}
import org.scalatest.wordspec.AsyncWordSpec

trait DbReassignmentStoreTest extends AsyncWordSpec with BaseTest with ReassignmentStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_reassignments", functionFullName)
  }

  "DbReassignmentStore" should {

    val indexStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 100)

    def mkStore(
        synchronizerId: IndexedSynchronizer,
        batchingConfig: BatchingConfig = BatchingConfig(),
    ): DbReassignmentStore =
      new DbReassignmentStore(
        storage,
        ReassignmentTag.Target(synchronizerId),
        indexStore,
        futureSupervisor,
        exitOnFatalFailures = true,
        batchingConfig,
        timeouts,
        loggerFactory,
      )

    behave like reassignmentStore(mkStore(_))

    "findIncomplete" should {
      "return all the incomplete reassignments when they span several pages" in {
        val pageSize = 3
        val store = mkStore(
          ReassignmentStoreTest.indexedTargetSynchronizer,
          BatchingConfig(maxItemsInBatch = PositiveNumeric.tryCreate(pageSize)),
        )
        val offset = Offset.tryFromLong(10L)

        val reassignments = (1 to 3 * pageSize + 1).map(i =>
          ReassignmentStoreTest.mkUnassignmentDataForSynchronizer(
            sourceMediator = ReassignmentStoreTest.mediator1,
            sourceSynchronizerId = ReassignmentStoreTest.sourceSynchronizer1,
            targetSynchronizerId = ReassignmentStoreTest.targetSynchronizerId,
            unassignmentTs = ofEpochSecond(i.toLong),
          )
        )

        for {
          _ <- MonadUtil
            .sequentialTraverse_(reassignments)(store.addUnassignmentData)
            .valueOrFail("add unassignment data")
          _ <- store
            .addReassignmentsOffsets(
              reassignments.map(_.reassignmentId -> UnassignmentGlobalOffset(offset)).toMap
            )
            .valueOrFail("add unassignment offsets")

          found <- store.findIncomplete(
            sourceSynchronizer = None,
            validAt = offset,
            stakeholders = None,
            limit = NonNegativeInt.tryCreate(reassignments.size * 2),
          )
        } yield found.map(_.reassignmentId) should contain theSameElementsAs reassignments.map(
          _.reassignmentId
        )
      }
    }
  }

}

class ReassignmentStoreTestH2 extends DbReassignmentStoreTest with H2Test

class ReassignmentStoreTestPostgres extends DbReassignmentStoreTest with PostgresTest
