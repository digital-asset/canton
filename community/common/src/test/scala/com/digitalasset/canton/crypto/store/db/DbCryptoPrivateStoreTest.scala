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

    storage.update(
      DBIO.seq(
        sqlu"truncate table crypto_hmac_secret",
        sqlu"truncate table crypto_private_keys",
      ),
      operationName = s"${this.getClass}: Truncate private crypto tables",
    )
  }

  "DbCryptoPrivateStore" can {
    behave like cryptoPrivateStore(new DbCryptoPrivateStore(storage, timeouts, loggerFactory))
  }
}

class CryptoPrivateStoreTestH2 extends DbCryptoPrivateStoreTest with H2Test

class CryptoPrivateStoreTestPostgres extends DbCryptoPrivateStoreTest with PostgresTest
