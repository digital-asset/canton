// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.participant.store.TransferStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbTransferStoreTest extends AsyncWordSpec with BaseTest with TransferStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api._
    storage.update(sqlu"truncate table transfers", functionFullName)
  }

  "DbTransferStore" should {
    behave like transferStore(domain =>
      new DbTransferStore(
        storage,
        domain,
        TestingIdentityFactory.pureCrypto(),
        timeouts,
        loggerFactory,
      )
    )
  }
}

class TransferStoreTestH2 extends DbTransferStoreTest with H2Test

class TransferStoreTestPostgres extends DbTransferStoreTest with PostgresTest
