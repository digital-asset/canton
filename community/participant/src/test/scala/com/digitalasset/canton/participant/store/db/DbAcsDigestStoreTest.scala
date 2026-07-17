// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.AcsDigestStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait BaseDbAcsDigestStoreTest { self: DbTest =>
  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_acs_running_digests_checkpoint",
        sqlu"truncate table par_acs_participant_running_digest",
        sqlu"truncate table par_acs_party_running_digest",
      ),
      functionFullName,
    )
  }

}

trait DbAcsDigestStoreTest
    extends AsyncWordSpec
    with BaseDbAcsDigestStoreTest
    with BaseTest
    with AcsDigestStoreTest {
  self: DbTest =>

  private val defaultSync = indexedSynchronizer(1, "synchronizer")

  "DbAcsDigestStore" should {
    behave like acsDigestSingleStoreTests((ec) =>
      new DbAcsDigestStore(
        indexedSynchronizer = defaultSync,
        Eval.now(mockStringInterning),
        storage,
        loggerFactory,
        timeouts,
      )(ec)
    )
    behave like acsDigestMultiStoresTests((ec, indexedSynchronizer) =>
      new DbAcsDigestStore(
        indexedSynchronizer = indexedSynchronizer,
        Eval.now(mockStringInterning),
        storage,
        loggerFactory,
        timeouts,
      )(ec)
    )
  }
}

@AcsCommitmentTest
class AcsDigestStoreTestH2 extends DbAcsDigestStoreTest with H2Test

@AcsCommitmentTest
class AcsDigestStoreTestPostgres extends DbAcsDigestStoreTest with PostgresTest
