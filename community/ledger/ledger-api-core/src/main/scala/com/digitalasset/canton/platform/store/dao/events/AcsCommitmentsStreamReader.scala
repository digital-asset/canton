// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.{
  ReceivedAcsCommitment,
  UpdateResponse,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawAcsCommitment
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.{Conversions, EventStorageBackend}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.AcsCommitmentsStreamReader.AcsCommitmentsStreamQueryParams
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.ExecutionContext

class AcsCommitmentsStreamReader(
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val dbMetrics = metrics.index.db

  def streamAcsCommitments(
      params: AcsCommitmentsStreamQueryParams
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, UpdateResponse.AcsCommitment), NotUsed] = {
    import params.*

    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)

    fetchCommitments(
      queryRange = queryRange,
      synchronizerId = synchronizerId,
      descendingOrder = descendingOrder,
      dbMetric = dbMetrics.acsCommitmentsStream.fetchAcsCommitments,
      payloadQueriesLimiter = payloadQueriesLimiter,
    )
      .map(toUpdateResponse)
  }

  private def fetchCommitments(
      queryRange: EventsRange,
      synchronizerId: SynchronizerId,
      descendingOrder: Boolean,
      dbMetric: DatabaseMetrics,
      payloadQueriesLimiter: ConcurrencyLimiter,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawAcsCommitment, NotUsed] =
    Source
      .single(
        IdRange(
          fromInclusive = queryRange.startInclusiveEventSeqId,
          toInclusive = queryRange.endInclusiveEventSeqId,
        )
      )
      // TODO(#33578) add configuration for parallelism if fetching acs commitments stays as is
      .mapAsync(parallelism = 2) { idRange =>
        payloadQueriesLimiter.execute {
          globalPayloadQueriesLimiter.execute {
            queryValidRange.withRangeNotPruned(
              minOffsetInclusive = queryRange.startInclusiveOffset,
              maxOffsetInclusive = queryRange.endInclusiveOffset,
              errorPruning = (prunedOffset: Offset) =>
                s"ACS commitments request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
              errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                s"ACS commitments request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                    .fold(0L)(_.unwrap)}",
            ) {
              dbDispatcher.executeSql(dbMetric)(
                eventStorageBackend.fetchAcsCommitments(
                  eventSequentialIds = idRange,
                  synchronizerId = synchronizerId,
                  descendingOrder = descendingOrder,
                )
              )
            }
          }
        }
      }
      .mapConcat(identity)

  private def toUpdateResponse(raw: RawAcsCommitment): (Offset, UpdateResponse.AcsCommitment) =
    raw.offset -> UpdateResponse.AcsCommitment(
      ReceivedAcsCommitment(
        offset = raw.offset,
        synchronizerId = raw.synchronizerId,
        recordTime = raw.recordTime,
        payload = ByteString.copyFrom(raw.payload),
        traceContext = Conversions.traceContextFrom(noTracingLogger)(raw.traceContext),
      )
    )
}

object AcsCommitmentsStreamReader {

  final case class AcsCommitmentsStreamQueryParams(
      queryRange: EventsRange,
      synchronizerId: SynchronizerId,
      descendingOrder: Boolean,
      maxParallelPayloadQueries: Int,
  )
}
