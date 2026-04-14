// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.SynchronizerConnectivityStatusStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbSynchronizerConnectivityStatusStoreTest
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerConnectivityStatusStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(sqlu"truncate table par_synchronizer_connectivity_status", functionFullName)
  }

  "DbSynchronizerConnectivityStatusStore" should {
    behave like synchronizerConnectivityStatusStore(synchronizerId =>
      new DbSynchronizerConnectivityStatusStore(synchronizerId, storage, timeouts, loggerFactory)
    )
  }
}

class SynchronizerConnectivityStatusStoreTestH2
    extends DbSynchronizerConnectivityStatusStoreTest
    with H2Test

class SynchronizerConnectivityStatusStoreTestPostgres
    extends DbSynchronizerConnectivityStatusStoreTest
    with PostgresTest
