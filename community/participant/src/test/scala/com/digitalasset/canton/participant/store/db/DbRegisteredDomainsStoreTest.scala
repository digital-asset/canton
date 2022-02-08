// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.RegisteredDomainsStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbRegisteredDomainsStoreTest
    extends AsyncWordSpec
    with BaseTest
    with RegisteredDomainsStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api._
    storage.update(sqlu"truncate table participant_domains", functionFullName)
  }

  "DbRegisteredDomainsStore" should {
    behave like registeredDomainsStore(() => new DbRegisteredDomainsStore(storage))
  }
}

class RegisteredDomainsStoreTestH2 extends DbRegisteredDomainsStoreTest with H2Test

class RegisteredDomainsStoreTestPostgres extends DbRegisteredDomainsStoreTest with PostgresTest
