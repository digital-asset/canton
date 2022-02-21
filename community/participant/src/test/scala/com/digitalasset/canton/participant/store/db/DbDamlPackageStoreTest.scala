// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.participant.store.DamlPackageStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import io.functionmeta.functionFullName

import scala.concurrent.Future

trait DbDamlPackageStoreTest extends DamlPackageStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._
    storage.update(
      DBIO.seq(
        sqlu"delete from dar_packages",
        sqlu"delete from daml_packages",
        sqlu"delete from dars",
      ),
      functionFullName,
    )
  }

  "DbDamlPackagesDarsStore" should {
    behave like damlPackageStore(() =>
      new DbDamlPackageStore(PositiveNumeric.tryCreate(2), storage, timeouts, loggerFactory)
    )
  }
}

class DamlPackageStoreTestH2 extends DbDamlPackageStoreTest with H2Test

class DamlPackageStoreTestPostgres extends DbDamlPackageStoreTest with PostgresTest
