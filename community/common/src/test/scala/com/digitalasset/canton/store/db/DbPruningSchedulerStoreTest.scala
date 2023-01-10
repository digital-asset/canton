// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config.RequireTypes.String3
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.PruningSchedulerStoreTest
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbPruningSchedulerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with PruningSchedulerStoreTest {
  this: DbTest =>
  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(DBIO.seq(sqlu"truncate table pruning_schedules"), functionFullName)
  }

  "DbPruningSchedulerStore" should {
    behave like pruningSchedulerStore(() =>
      new DbPruningSchedulerStore(
        String3.tryCreate("DBT"),
        storage,
        timeouts,
        loggerFactory,
      )
    )

  }
}

class DbPruningSchedulerStoreTestH2 extends DbPruningSchedulerStoreTest with H2Test

class DbPruningSchedulerStoreTestPostgres extends DbPruningSchedulerStoreTest with PostgresTest
