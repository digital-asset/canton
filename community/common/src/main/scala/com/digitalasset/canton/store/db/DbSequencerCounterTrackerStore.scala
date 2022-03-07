// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.SequencerCounterTrackerStore

import scala.concurrent.ExecutionContext

class DbSequencerCounterTrackerStore(
    client: SequencerClientDiscriminator,
    storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerCounterTrackerStore
    with FlagCloseable
    with NamedLogging {
  override protected[store] val cursorStore = new DbCursorPreheadStore[SequencerCounter](
    client,
    storage,
    DbSequencerCounterTrackerStore.cursorTable,
    storage.metrics.loadGaugeM("sequencer-counter-tracker-store"),
    timeouts,
    loggerFactory,
  )

  override def onClosed(): Unit = Lifecycle.close(cursorStore)(logger)
}

object DbSequencerCounterTrackerStore {
  val cursorTable = "head_sequencer_counters"
}
