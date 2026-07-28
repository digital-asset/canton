// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStoreTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

trait DbAcsCommitmentPeriodStoreTest extends AcsCommitmentPeriodStoreTest with DbTest {

  override def minimumProtocolVersion: ProtocolVersion = ProtocolVersion.acsCommitmentRedesign

  override protected def cleanDb(storage: DbStorage)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_acs_commitment_period_outstanding",
        sqlu"truncate table par_acs_commitment_period_mismatch",
        sqlu"truncate table par_acs_commitment_period_match",
        sqlu"truncate table par_acs_commitment_period_watermark",
        sqlu"truncate table par_acs_commitment_period_pruning",
      ),
      functionFullName,
    )
  }

  private val synchronizerId = DefaultTestIdentities.synchronizerId.toPhysical

  "DbAcsCommitmentPeriodStore" should {
    behave like acsCommitmentPeriodStore(
      (stringInterning, enableConsistencyChecks, executionContext) =>
        new DbAcsCommitmentPeriodStore(
          storage,
          IndexedSynchronizer.tryCreate(synchronizerId, 1),
          Eval.now(stringInterning),
          timeouts,
          loggerFactory,
          enableConsistencyChecks,
        )(executionContext)
    )
  }

}

@AcsCommitmentTest
final class DbAcsCommitmentPeriodStoreTestPostgres
    extends DbAcsCommitmentPeriodStoreTest
    with PostgresTest

@AcsCommitmentTest
final class DbAcsCommitmentPeriodStoreTestH2 extends DbAcsCommitmentPeriodStoreTest with H2Test
