// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.SequencerCounterTrackerStore

import scala.concurrent.ExecutionContext

class DbSequencerCounterTrackerStore(
    client: SequencerClientDiscriminator,
    storage: DbStorage,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerCounterTrackerStore {
  override protected[store] val cursorStore = new DbCursorPreheadStore[SequencerCounter](
    client,
    storage,
    DbSequencerCounterTrackerStore.cursorTable,
    storage.metrics.loadGaugeM("sequencer-counter-tracker-store"),
    loggerFactory,
  )
}

object DbSequencerCounterTrackerStore {
  val cursorTable = "head_sequencer_counters"
}
