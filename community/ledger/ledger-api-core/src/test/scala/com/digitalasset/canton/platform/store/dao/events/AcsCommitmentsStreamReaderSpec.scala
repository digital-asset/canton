// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.DatabaseMetrics
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.PruningOffsetService
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawAcsCommitment
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, LedgerEnd}
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.AcsCommitmentsStreamReader.AcsCommitmentsStreamQueryParams
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.StatusRuntimeException
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event.Level

import java.sql.Connection
import scala.concurrent.Future

class AcsCommitmentsStreamReaderSpec
    extends AnyFlatSpec
    with Matchers
    with BaseTest
    with PekkoBeforeAndAfterAll
    with HasExecutionContext {

  behavior of "AcsCommitmentsStreamReader.streamAcsCommitments"

  import AcsCommitmentsStreamReaderSpec.*

  private implicit val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace(loggerFactory)(TraceContext.empty)

  it should "fail with a beyond-ledger-end error when the requested range exceeds the ledger end" in {
    // ledger end is at offset 5, but the requested range ends at offset 10
    val ledgerEndCache = MutableLedgerEndCache()
    ledgerEndCache.set(Some(ledgerEnd(offset = 5L, seqId = 5L)))

    val pruningOffsetService = mock[PruningOffsetService]
    when(pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(None))

    // the storage backend should not be hit, as the range check fails before the query runs
    val eventStorageBackend = mock[EventStorageBackend]
    val dbDispatcher = mock[DbDispatcher]

    val reader = newReader(ledgerEndCache, pruningOffsetService, dbDispatcher, eventStorageBackend)

    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = reader
          .streamAcsCommitments(
            AcsCommitmentsStreamQueryParams(
              queryRange = EventsRange(
                startInclusiveOffset = offset(6L),
                startInclusiveEventSeqId = 6L,
                endInclusiveOffset = offset(10L),
                endInclusiveEventSeqId = 10L,
              ),
              synchronizerId = synchronizerId,
              descendingOrder = false,
              maxParallelPayloadQueries = 1,
            )
          )
          .runWith(Sink.seq),
        assertions = _.infoMessage should include(
          "PARTICIPANT_DATA_ACCESSED_AFTER_LEDGER_END(9,0): ACS commitments request from 6 to 10 is beyond ledger end offset 5"
        ),
      )
      .futureValue
  }

  it should "fail with a pruned-data error when the requested range precedes the pruning offset" in {
    // ledger end is at offset 10, pruning offset is 6, requested range starts at offset 3
    val ledgerEndCache = MutableLedgerEndCache()
    ledgerEndCache.set(Some(ledgerEnd(offset = 10L, seqId = 10L)))

    val pruningOffsetService = mock[PruningOffsetService]
    when(pruningOffsetService.pruningOffset(any[TraceContext]))
      .thenReturn(Future.successful(Some(offset(6L))))

    // the query itself succeeds (returns no commitments), the pruning check runs afterwards
    val eventStorageBackend = mock[EventStorageBackend]
    val dbDispatcher = mock[DbDispatcher]
    when(
      dbDispatcher.executeSql(any[DatabaseMetrics])(any[Connection => Vector[RawAcsCommitment]])(
        any[LoggingContextWithTrace]
      )
    ).thenReturn(Future.successful(Vector.empty[RawAcsCommitment]))

    val reader = newReader(ledgerEndCache, pruningOffsetService, dbDispatcher, eventStorageBackend)

    loggerFactory
      .assertThrowsAndLogsSuppressingAsync[StatusRuntimeException](
        SuppressionRule.Level(Level.INFO)
      )(
        within = reader
          .streamAcsCommitments(
            AcsCommitmentsStreamQueryParams(
              queryRange = EventsRange(
                startInclusiveOffset = offset(3L),
                startInclusiveEventSeqId = 3L,
                endInclusiveOffset = offset(8L),
                endInclusiveEventSeqId = 8L,
              ),
              synchronizerId = synchronizerId,
              descendingOrder = false,
              maxParallelPayloadQueries = 1,
            )
          )
          .runWith(Sink.seq),
        assertions = _.infoMessage should include(
          "PARTICIPANT_PRUNED_DATA_ACCESSED(9,0): ACS commitments request from 3 to 8 precedes pruned offset 6"
        ),
      )
      .futureValue
  }

  private def newReader(
      ledgerEndCache: MutableLedgerEndCache,
      pruningOffsetService: PruningOffsetService,
      dbDispatcher: DbDispatcher,
      eventStorageBackend: EventStorageBackend,
  ): AcsCommitmentsStreamReader = {
    val noOpLimiter: ConcurrencyLimiter =
      new QueueBasedConcurrencyLimiter(parallelism = 1, executionContext = parallelExecutionContext)
    new AcsCommitmentsStreamReader(
      globalPayloadQueriesLimiter = noOpLimiter,
      dbDispatcher = dbDispatcher,
      queryValidRange = QueryValidRangeImpl(
        ledgerEndCache = ledgerEndCache,
        pruningOffsetService = pruningOffsetService,
        loggerFactory = loggerFactory,
      ),
      eventStorageBackend = eventStorageBackend,
      metrics = LedgerApiServerMetrics.ForTesting,
      loggerFactory = loggerFactory,
    )
  }
}

object AcsCommitmentsStreamReaderSpec {

  private val synchronizerId: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizer1")

  private def offset(value: Long): Offset = Offset.tryFromLong(value)

  private def ledgerEnd(offset: Long, seqId: Long): LedgerEnd =
    LedgerEnd(
      lastOffset = Offset.tryFromLong(offset),
      lastEventSeqId = seqId,
      lastStringInterningId = 0,
      lastPublicationTime = CantonTimestamp.MinValue,
      synchronizerIndices = Map(),
    )
}
