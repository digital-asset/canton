// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.topology_transaction.{TopologyEvent, TopologyTransaction}
import com.daml.ledger.api.v2.trace_context.TraceContext as ProtoTraceContext
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TopologyFormat
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.GenericTopologyEvent.SynchronizerParametersState
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.ScalaPbMessageWithPrecomputedSerializedSize
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawDynamicSynchronizerParameters,
  RawParticipantAuthorization,
  RawTopologyEvent,
  SequentialIdBatch,
}
import com.digitalasset.canton.platform.store.backend.{Conversions, EventStorageBackend}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPageQuery
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.OrderingUtils.orderingBasedOnDescending
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.{
  CommonTopologyTransactionProperties,
  PayloadDbQuery,
  TopologyTransactionResponse,
  TopologyTransactionsStreamQueryParams,
}
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.ExecutionContext
import scala.util.chaining.*

class TopologyTransactionsStreamReader(
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val dbMetrics = metrics.index.db

  def streamTopologyTransactions(
      topologyTransactionsStreamQueryParams: TopologyTransactionsStreamQueryParams
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, TopologyTransactionResponse), NotUsed] = {
    import topologyTransactionsStreamQueryParams.*

    val assignedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdQueries, executionContext)

    def streamIds(
        idStreamName: String,
        metric: DatabaseMetrics,
        idPageQuery: IdPageQuery,
    ): Source[Long, NotUsed] =
      paginatingAsyncStream.streamIdsFromSeekPaginationWithoutIdFilter(
        idStreamName = idStreamName,
        idPageSizing = idPageSizing,
        idPageBufferSize = maxPagesPerIdPagesBuffer,
        initialEventSeqIdRange = queryRange.eventSeqIdRange,
        descendingOrder = descendingOrder,
      )(idPageQuery)(
        executeIdQuery = f =>
          assignedEventIdQueriesLimiter.execute {
            globalIdQueriesLimiter.execute {
              dbDispatcher.executeSql(metric)(f)
            }
          }
      )

    def fetchIds(
        maxOutputBatchCount: Int,
        idStreams: Vector[Source[Long, NotUsed]],
    ): Source[Iterable[Long], NotUsed] =
      idStreams
        .pipe(EventIdsUtils.sortAndDeduplicateIds(descendingOrder = descendingOrder))
        .batchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = maxOutputBatchCount,
        )

    def fetchPayloads(
        ids: Source[Iterable[Long], NotUsed],
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
        payloadDbQuery: PayloadDbQuery,
    ): Source[RawTopologyEvent, NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              queryValidRange.withRangeNotPruned(
                offsetRange = queryRange.offsetRange,
                errorPruning = (prunedOffset: Offset) =>
                  s"Topology events request for ${queryRange.offsetRange} precedes pruned offset ${prunedOffset.unwrap}",
                errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                  s"Topology events request for ${queryRange.offsetRange} is beyond ledger end offset ${ledgerEndOffset
                      .fold(0L)(_.unwrap)}",
              ) {
                dbDispatcher.executeSql(dbMetric)(
                  payloadDbQuery.fetchPayloads(eventSequentialIds = Ids(ids))
                )
              }
            }
          }
        )
        .mapConcat(identity)
    }

    val partyEvents: Source[RawTopologyEvent, NotUsed] =
      topologyFormat.participantAuthorizationFormat match {
        case Some(participantAuthorizationFormat) =>
          val partyEventIds =
            fetchIds(
              maxOutputBatchCount = maxParallelPayloadQueries + 1,
              idStreams = {
                val partiesO: Vector[Option[Party]] =
                  participantAuthorizationFormat.parties match {
                    case Some(parties) => parties.map(Some(_)).toVector
                    // fetch ids for all the parties
                    case None => Vector(None)
                  }
                partiesO.map(partyO =>
                  streamIds(
                    idStreamName = s"Event IDs for topology transaction events for partyO:$partyO",
                    metric = dbMetrics.topologyTransactionsStream.fetchTopologyPartyEventIds,
                    idPageQuery = eventStorageBackend.fetchTopologyPartyEventIds(partyO),
                  )
                )
              },
            )
          fetchPayloads(
            ids = partyEventIds,
            maxParallelPayloadQueries = maxParallelPayloadQueries,
            dbMetric = dbMetrics.topologyTransactionsStream.fetchTopologyPartyEventPayloads,
            payloadDbQuery = eventStorageBackend.topologyPartyEventBatch,
          )
        case None =>
          Source.empty
      }

    val synchronizerParameterEvents: Source[RawTopologyEvent, NotUsed] =
      if (topologyFormat.synchronizerParametersFormat) {
        val synchronizerParameterEventIds =
          fetchIds(
            maxOutputBatchCount = maxParallelPayloadQueries + 1,
            idStreams = Vector(
              streamIds(
                idStreamName = "Event IDs for synchronizer parameters events",
                metric =
                  dbMetrics.topologyTransactionsStream.fetchDynamicSynchronizerParametersEventIds,
                idPageQuery = eventStorageBackend.fetchDynamicSynchronizerParametersEventIds,
              )
            ),
          )
        fetchPayloads(
          ids = synchronizerParameterEventIds,
          dbMetric =
            dbMetrics.topologyTransactionsStream.fetchDynamicSynchronizerParametersEventPayloads,
          maxParallelPayloadQueries = maxParallelPayloadQueries,
          payloadDbQuery = eventStorageBackend.dynamicSynchronizerParametersBatch,
        )
      } else
        Source.empty

    val merged: Source[RawTopologyEvent, NotUsed] =
      partyEvents.mergeSorted(synchronizerParameterEvents)(
        Ordering.by[RawTopologyEvent, Long](_.offset.unwrap)(
          orderingBasedOnDescending(descendingOrder)
        )
      )

    val filtered: Source[RawTopologyEvent, NotUsed] =
      topologyFormat.synchronizerId match {
        case Some(synchronizerId) =>
          merged.filter(_.synchronizerId == synchronizerId.toProtoPrimitive)
        case None =>
          merged
      }

    UpdateReader
      .groupContiguous(filtered)(by = _.updateId)
      .mapConcat(group => toTopologyTransactionResponse(group).toList)
  }

  private def toTopologyTransactionResponse(
      payloads: Vector[RawTopologyEvent]
  ): Option[(Offset, TopologyTransactionResponse)] =
    payloads.headOption.map { first =>
      val events = payloads.collect { case raw: RawParticipantAuthorization =>
        TransactionConversions.toTopologyEvent(
          partyId = raw.partyId,
          participantId = raw.participantId,
          authorizationEvent = raw.authorizationEvent,
        )
      }
      val synchronizerParametersState = payloads.reverseIterator.collectFirst {
        case raw: RawDynamicSynchronizerParameters =>
          SynchronizerParametersState(ByteString.copyFrom(raw.payload))
      }
      first.offset -> TopologyTransactionResponse(
        commonTopologyTransactionProperties = CommonTopologyTransactionProperties(
          updateId = first.updateId,
          offset = first.offset.unwrap,
          synchronizerId = first.synchronizerId,
          recordTime = Some(TimestampConversion.fromLf(first.recordTime)),
          traceContext = Conversions.protoTraceContextFrom(noTracingLogger)(first.traceContext),
        ),
        events = events,
        synchronizerParametersState = synchronizerParametersState,
      )
    }

}

object TopologyTransactionsStreamReader {

  final case class CommonTopologyTransactionProperties(
      updateId: String,
      offset: Long,
      synchronizerId: String,
      recordTime: Option[ProtoTimestamp],
      traceContext: Option[ProtoTraceContext],
  )

  object CommonTopologyTransactionProperties {
    def fromProto(topologyTx: TopologyTransaction): CommonTopologyTransactionProperties =
      CommonTopologyTransactionProperties(
        updateId = topologyTx.updateId,
        offset = topologyTx.offset,
        synchronizerId = topologyTx.synchronizerId,
        recordTime = topologyTx.recordTime,
        traceContext = topologyTx.traceContext,
      )
  }

  final case class TopologyTransactionResponse(
      commonTopologyTransactionProperties: CommonTopologyTransactionProperties,
      events: Seq[TopologyEvent],
      synchronizerParametersState: Option[SynchronizerParametersState],
  ) {

    def toProtoTopologyTransaction: Option[TopologyTransaction] =
      Option.when(events.nonEmpty)(
        TopologyTransaction(
          updateId = commonTopologyTransactionProperties.updateId,
          offset = commonTopologyTransactionProperties.offset,
          synchronizerId = commonTopologyTransactionProperties.synchronizerId,
          recordTime = commonTopologyTransactionProperties.recordTime,
          events = events,
          traceContext = commonTopologyTransactionProperties.traceContext,
        ).withPrecomputedSerializedSize()
      )
  }

  final case class SynchronizerParametersResponse(
      commonTopologyTransactionProperties: CommonTopologyTransactionProperties,
      synchronizerParametersState: SynchronizerParametersState,
  )

  final case class TopologyTransactionsStreamQueryParams(
      queryRange: EventsRange,
      descendingOrder: Boolean,
      payloadQueriesLimiter: ConcurrencyLimiter,
      idPageSizing: IdPageSizing,
      topologyFormat: TopologyFormat,
      maxParallelIdQueries: Int,
      maxPagesPerIdPagesBuffer: Int,
      maxPayloadsPerPayloadsPage: Int,
      maxParallelPayloadQueries: Int,
  )

  @FunctionalInterface
  trait PayloadDbQuery {
    def fetchPayloads(
        eventSequentialIds: SequentialIdBatch
    ): Connection => Vector[RawTopologyEvent]
  }
}
