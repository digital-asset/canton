// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.domain.sequencing.sequencer.store.DbSequencerStoreTest.MaxInClauseSize
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test}
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName

import scala.concurrent.Future

trait DbSequencerStoreTest extends SequencerStoreTest with MultiTenantedSequencerStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] =
    DbSequencerStoreTest.cleanSequencerTables(storage)

  "DbSequencerStore" should {
    behave like sequencerStore(() =>
      new DbSequencerStore(storage, MaxInClauseSize, timeouts, loggerFactory)
    )
    behave like multiTenantedSequencerStore(() =>
      new DbSequencerStore(storage, MaxInClauseSize, timeouts, loggerFactory)
    )
  }
}

object DbSequencerStoreTest {
  // intentionally low to expose any problems with usage of IN builder
  val MaxInClauseSize = PositiveNumeric.tryCreate(2)

  def cleanSequencerTables(
      storage: DbStorage
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[Unit] = {
    import storage.api._

    storage.update(
      DBIO.seq(
        Seq(
          "members",
          "counter_checkpoints",
          "payloads",
          "watermarks",
          "events",
          "acknowledgements",
          "lower_bound",
        )
          .map(name => sqlu"truncate table sequencer_#$name"): _*
      ),
      functionFullName,
    )
  }
}

class SequencerStoreTestH2 extends DbSequencerStoreTest with H2Test
