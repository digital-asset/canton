// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import com.digitalasset.canton.crypto.store.CryptoPrivateStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbCryptoPrivateStoreTest extends AsyncWordSpec with CryptoPrivateStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._

    /* We delete all private keys that ARE NOT encrypted (wrapper_key_id == NULL).
    This conditional delete is to avoid conflicts with the encrypted crypto private store tests. */
    storage.update(
      DBIO.seq(
        sqlu"delete from crypto_private_keys where wrapper_key_id IS NULL"
      ),
      operationName = s"${this.getClass}: Delete from private crypto table",
    )
  }

  "DbCryptoPrivateStore" can {
    behave like cryptoPrivateStore(new DbCryptoPrivateStore(storage, timeouts, loggerFactory))
  }
}

class CryptoPrivateStoreTestH2 extends DbCryptoPrivateStoreTest with H2Test

class CryptoPrivateStoreTestPostgres extends DbCryptoPrivateStoreTest with PostgresTest
