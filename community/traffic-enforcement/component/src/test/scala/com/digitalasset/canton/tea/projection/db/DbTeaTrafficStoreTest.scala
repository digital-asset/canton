// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tea.projection.TeaTrafficStoreTest
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbTeaTrafficStoreTest extends AsyncWordSpec with BaseTest with TeaTrafficStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_traffic_enforcement_event",
        sqlu"truncate table par_traffic_enforcement_balance",
        sqlu"truncate table pekko_projection_offset_store",
        sqlu"truncate table pekko_projection_management",
      ),
      functionFullName,
    )
  }

  "TeaTrafficStore" should {
    behave like teaTrafficStore(() =>
      new TeaDbTrafficStore(
        storage,
        loggerFactory,
        timeouts,
      )
    )
  }
}

class DbTeaTrafficStorePostgresTest extends DbTeaTrafficStoreTest with PostgresTest

class DbTeaTrafficStoreH2Test extends DbTeaTrafficStoreTest with H2Test
