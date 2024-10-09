// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.domain.sequencing.sequencer.store.DbSequencerStoreTest.MaxInClauseSize
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.{DbStorage, Storage}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait DbSequencerStoreTest extends SequencerStoreTest with MultiTenantedSequencerStoreTest {
  this: DbTest =>

  override lazy val dbStorage: Option[Storage] = Some(storage)

  override protected val trafficReceiptSupport: Boolean = true

  override def cleanDb(storage: DbStorage): Future[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  "DbSequencerStore" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        MaxInClauseSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        unifiedSequencer = testedUseUnifiedSequencer,
        CachingConfigs(),
      )
    )
    behave like multiTenantedSequencerStore(() =>
      new DbSequencerStore(
        storage,
        testedProtocolVersion,
        MaxInClauseSize,
        timeouts,
        loggerFactory,
        sequencerMember,
        unifiedSequencer = testedUseUnifiedSequencer,
        CachingConfigs(),
      )
    )
  }
}

object DbSequencerStoreTest {
  // intentionally low to expose any problems with usage of IN builder
  val MaxInClauseSize = PositiveNumeric.tryCreate(2)

  def cleanSequencerTables(
      storage: DbStorage
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] = {
    import storage.api.*

    storage.update(
      DBIO.seq(
        Seq(
          "sequencer_members",
          "sequencer_counter_checkpoints",
          "sequencer_payloads",
          "sequencer_watermarks",
          "sequencer_events",
          "sequencer_acknowledgements",
          "sequencer_lower_bound",
          "seq_traffic_control_consumed_journal",
        )
          .map(name => sqlu"truncate table #$name")*
      ),
      functionFullName,
    )
  }
}

class SequencerStoreTestH2 extends DbSequencerStoreTest with H2Test

class SequencerStoreTestPostgres extends DbSequencerStoreTest with PostgresTest
