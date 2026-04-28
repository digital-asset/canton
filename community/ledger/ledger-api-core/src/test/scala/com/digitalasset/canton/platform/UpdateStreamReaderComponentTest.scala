// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForUpdatesLedgerEffects
import com.digitalasset.canton.platform.store.dao.events.{
  EventsRange,
  QueryValidRange,
  UpdatesStreamReader,
}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.atomic.AtomicReference

class UpdateStreamReaderComponentTest extends AnyFlatSpec with IndexComponentTest {

  private lazy val eventStorageBackend = dbSupport.storageBackendFactory.createEventStorageBackend(
    ledgerEndCache = ledgerEndCache,
    stringInterning = stringInterning,
    loggerFactory = loggerFactory,
  )

  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  private val nextRecordTime: () => CantonTimestamp =
    () => recordTimeRef.updateAndGet(_.immediateSuccessor)

  private val create1 = creates(nextRecordTime, 10)(1)
  private val create2 = creates(nextRecordTime, 10)(1)
  private val create3 = creates(nextRecordTime, 10)(1)
  private val create4 = creates(nextRecordTime, 10)(1)

  private var upd1: Offset = _
  private var upd4: Offset = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    upd1 = ingestUpdates(create1)
    ingestUpdates(create2)
    ingestUpdates(create3)
    upd4 = ingestUpdates(create4)
  }

  private def readUpdates =
    UpdatesStreamReader
      .fetchContractPayloadsInternal(
        queryRange = EventsRange(upd1, 1, upd4, 1000),
        dbMetric = DatabaseMetrics.ForTesting("test"),
        contractStore = contractStore,
        skipPruningChecks = true,
        ids = Vector.range(1L, 1000L),
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsLedgerEffects(
              EventPayloadSourceForUpdatesLedgerEffects.Activate
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx = None,
              requestingPartiesForReassignment = None,
            )(connection),
        pruningOffsetService = pruningOffsetService,
        queryValidRange = mock[QueryValidRange],
        dbDispatcher = dbDispatcher,
      )
      .futureValue

  behavior of "UpdateStreamReaderComponent"

  it should "fetch all updates from the beginning when nothing was pruned" in {
    val updates = readUpdates
    updates should have size 4
  }

  it should "fetch all updates from the beginning when first offset was pruned" in {
    index.prune(index.latestPrunedOffset().futureValue, Vector(), upd1, Vector()).futureValue
    val updates = readUpdates
    updates should have size 3
  }

  it should "fetch empty vector from the beginning when all updates were pruned" in {
    index.prune(index.latestPrunedOffset().futureValue, Vector(), upd4, Vector()).futureValue
    val updates = readUpdates
    updates should be(empty)
  }
}
