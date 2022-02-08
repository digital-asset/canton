// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.store.DamlPackagesDarsStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.BaseTest
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbDamlPackageStoreTest extends AsyncWordSpec with BaseTest with DamlPackagesDarsStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._
    storage.update(
      DBIO.seq(sqlu"truncate table daml_packages", sqlu"truncate table dars"),
      functionFullName,
    )
  }

  "DbDamlPackagesDarsStore" should {
    behave like damlPackageStore(() => new DbDamlPackageStore(storage, timeouts, loggerFactory))
  }
}

class DamlPackageStoreTestH2 extends DbDamlPackageStoreTest with H2Test

class DamlPackageStoreTestPostgres extends DbDamlPackageStoreTest with PostgresTest
