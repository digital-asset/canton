// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.store.ServiceAgreementStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.BaseTest
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbServiceAgreementStoreTest
    extends AsyncWordSpec
    with BaseTest
    with ServiceAgreementStoreTest { this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._

    storage.update(
      DBIO.seq(sqlu"truncate table accepted_agreements", sqlu"truncate table service_agreements"),
      functionFullName,
    )
  }

  "DbServiceAgreementStore" should {
    behave like serviceAgreementStore(() => new DbServiceAgreementStore(storage, loggerFactory))
  }
}

class ServiceAgreementStoreTestH2 extends DbServiceAgreementStoreTest with H2Test

class ServiceAgreementStoreTestPostgres extends DbServiceAgreementStoreTest with PostgresTest
