// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedPhysicalSynchronizer, SequencerCounterTrackerStoreTest}
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

trait DbSequencerCounterTrackerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with SequencerCounterTrackerStoreTest
    with FailOnShutdown {
  this: DbTest =>

  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("da::default")
  ).toPhysical

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table #${DbSequencerCounterTrackerStore.cursorTable}"),
      functionFullName,
    )
  }

  "DbSequencerCounterTrackerStore" should {
    behave like sequencerCounterTrackerStore(() =>
      new DbSequencerCounterTrackerStore(
        IndexedPhysicalSynchronizer.tryCreate(synchronizerId, 1),
        storage,
        timeouts,
        loggerFactory,
      )
    )
  }
}

class SequencerCounterTrackerStoreTestH2 extends DbSequencerCounterTrackerStoreTest with H2Test

class SequencerCounterTrackerStoreTestPostgres
    extends DbSequencerCounterTrackerStoreTest
    with PostgresTest
