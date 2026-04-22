// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.metrics.DatabaseMetrics
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.Spans
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.api.{TraceIdentifiers, TransactionShape}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.config.UpdatesStreamsConfig
import com.digitalasset.canton.platform.store.ScalaPbStreamingOptimizations.ScalaPbMessageWithPrecomputedSerializedSize
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.Ids
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{RawEvent, RawThinEvent}
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, PersistentEventType}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterPageQuery,
  IdPageQuery,
}
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.TopologyTransactionsStreamQueryParams
import com.digitalasset.canton.platform.store.dao.events.UpdatesStreamReader.VectorOps
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
  Telemetry,
}
import com.digitalasset.canton.platform.store.{LedgerApiContractStore, PruningOffsetService}
import com.digitalasset.canton.platform.{
  FatContract,
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class UpdatesStreamReader(
    config: UpdatesStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    contractStore: LedgerApiContractStore,
    metrics: LedgerApiServerMetrics,
    tracer: Tracer,
    topologyTransactionsStreamReader: TopologyTransactionsStreamReader,
    pruningOffsetService: PruningOffsetService,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  import UpdateReader.*
  import config.*

  private val dbMetrics = metrics.index.db

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  def streamUpdates(
      queryRange: EventsRange,
      internalUpdateFormat: InternalUpdateFormat,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val span =
      Telemetry.Updates.createSpan(
        tracer,
        queryRange.startInclusiveOffset,
        queryRange.endInclusiveOffset,
      )(
        qualifiedNameOfCurrentFunc
      )
    logger.debug(
      s"streamUpdates(${queryRange.startInclusiveOffset}, ${queryRange.endInclusiveOffset}, descending: $descendingOrder, $internalUpdateFormat)"
    )
    doStreamUpdates(
      queryRange = queryRange,
      internalUpdateFormat = internalUpdateFormat,
      descendingOrder = descendingOrder,
      skipPruningChecks = skipPruningChecks,
    )
      .wireTap(_ match {
        case (_, getUpdatesResponse) =>
          getUpdatesResponse.update match {
            case GetUpdatesResponse.Update.Transaction(value) =>
              val event = tracing.Event("update", TraceIdentifiers.fromTransaction(value))
              Spans.addEventToSpan(event, span)
            case GetUpdatesResponse.Update.Reassignment(reassignment) =>
              val event =
                tracing.Event("update", TraceIdentifiers.fromReassignment(reassignment))
              Spans.addEventToSpan(event, span)
            case GetUpdatesResponse.Update.TopologyTransaction(topologyTransaction) =>
              val event = tracing
                .Event("update", TraceIdentifiers.fromTopologyTransaction(topologyTransaction))
              Spans.addEventToSpan(event, span)
            case _ => ()
          }
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamUpdates(
      queryRange: EventsRange,
      internalUpdateFormat: InternalUpdateFormat,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, GetUpdatesResponse), NotUsed] = {
    val longOrdering: Ordering[Long] =
      orderingBasedOnDescending(descendingOrder = descendingOrder)

    val payloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelPayloadQueries, executionContext)
    val deserializationQueriesLimiter =
      new QueueBasedConcurrencyLimiter(transactionsProcessingParallelism, executionContext)
    val txFilters: Set[DecomposedFilter] =
      internalUpdateFormat.includeTransactions
        .map(_.internalEventFormat.templatePartiesFilter)
        .toList
        .flatMap(txFilteringConstraints =>
          FilterUtils.decomposeFilters(txFilteringConstraints).toList
        )
        .toSet
    val reassignmentsFilters: Set[DecomposedFilter] =
      internalUpdateFormat.includeReassignments
        .map(_.templatePartiesFilter)
        .toList
        .flatMap(reassignmentsFilteringConstraints =>
          FilterUtils.decomposeFilters(reassignmentsFilteringConstraints).toList
        )
        .toSet
    val txAndReassignmentFilters =
      txFilters.filter(reassignmentsFilters)
    val justTxFilters =
      txFilters.filterNot(txAndReassignmentFilters)
    val justReassignmentFilters =
      reassignmentsFilters.filterNot(txAndReassignmentFilters)
    val numTxAndReassignmentFilters = txAndReassignmentFilters.size *
      internalUpdateFormat.includeTransactions.fold(0)(_.transactionShape match {
        // The ids for ledger effects transactions are retrieved from 5 separate id tables: (activate stakeholder,
        // activate witness, deactivate stakeholder, deactivate witness, various witnessed)
        case TransactionShape.LedgerEffects => 5
        // The ids for acs delta transactions are retrieved from 2 separate id tables: (activate stakeholder, deactivate stakeholder)
        case TransactionShape.AcsDelta => 2
      })
    val numJustTxFilters = justTxFilters.size *
      internalUpdateFormat.includeTransactions.fold(0)(_.transactionShape match {
        // The ids for ledger effects transactions are retrieved from 5 separate id tables: (activate stakeholder,
        // activate witness, deactivate stakeholder, deactivate witness, various witnessed)
        case TransactionShape.LedgerEffects => 5
        // The ids for acs delta transactions are retrieved from 2 separate id tables: (activate stakeholder, deactivate stakeholder)
        case TransactionShape.AcsDelta => 2
      })
    // The ids for reassignments are retrieved from 2 separate id tables: (assign stakeholder, unassign stakeholder)
    val numJustReassignmentsFilters = justReassignmentFilters.size * 2
    // The ids for topology updates are retrieved from 1 id table: (party_to_participant)
    val numTopologyDecomposedFilters = internalUpdateFormat.includeTopologyEvents
      .flatMap(_.participantAuthorizationFormat)
      .fold(0)(_.parties.fold(1)(_.size))

    val idPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = maxIdsPerIdPage,
      workingMemoryInBytesForIdPages = maxWorkingMemoryInBytesForIdPages,
      numOfDecomposedFilters = numTxAndReassignmentFilters +
        numJustTxFilters +
        numJustReassignmentsFilters +
        numTopologyDecomposedFilters,
      numOfPagesInIdPageBuffer = maxPagesPerIdPagesBuffer,
      loggerFactory = loggerFactory,
    )

    val sourceOfTransactionsAndReassignments = internalUpdateFormat.includeTransactions match {
      case Some(InternalTransactionFormat(internalEventFormat, AcsDelta)) =>
        doStreamAcsDelta(
          queryRange = queryRange,
          txInternalEventFormat = Some(internalEventFormat),
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          idPageSizing = idPageSizing,
          descendingOrder = descendingOrder,
          skipPruningChecks = skipPruningChecks,
        )
      case Some(InternalTransactionFormat(internalEventFormat, LedgerEffects)) =>
        doStreamLedgerEffects(
          queryRange = queryRange,
          txInternalEventFormat = Some(internalEventFormat),
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          idPageSizing = idPageSizing,
          descendingOrder = descendingOrder,
          skipPruningChecks = skipPruningChecks,
        )
      case None if internalUpdateFormat.includeReassignments.isDefined =>
        doStreamAcsDelta(
          queryRange = queryRange,
          txInternalEventFormat = None,
          reassignmentInternalEventFormat = internalUpdateFormat.includeReassignments,
          txAndReassignmentFilters = txAndReassignmentFilters,
          justTxFilters = justTxFilters,
          justReassignmentFilters = justReassignmentFilters,
          payloadQueriesLimiter = payloadQueriesLimiter,
          idPageSizing = idPageSizing,
          descendingOrder = descendingOrder,
          skipPruningChecks = skipPruningChecks,
        )

      case None => Source.empty
    }

    val topologyTransactions =
      internalUpdateFormat.includeTopologyEvents.flatMap(_.participantAuthorizationFormat) match {
        case Some(participantAuthorizationFormat) =>
          topologyTransactionsStreamReader
            .streamTopologyTransactions(
              TopologyTransactionsStreamQueryParams(
                queryRange = queryRange,
                descendingOrder = descendingOrder,
                payloadQueriesLimiter = payloadQueriesLimiter,
                idPageSizing = idPageSizing,
                participantAuthorizationFormat = participantAuthorizationFormat,
                maxParallelIdQueries = maxParallelIdTopologyEventsQueries,
                maxPagesPerIdPagesBuffer = maxPayloadsPerPayloadsPage,
                maxPayloadsPerPayloadsPage = maxParallelPayloadTopologyEventsQueries,
                maxParallelPayloadQueries = transactionsProcessingParallelism,
              )
            )
            .map { case (offset, topologyTransaction) =>
              offset -> GetUpdatesResponse(
                GetUpdatesResponse.Update.TopologyTransaction(topologyTransaction)
              ).withPrecomputedSerializedSize()
            }
        case None => Source.empty
      }

    UpdateReader
      .groupContiguous(sourceOfTransactionsAndReassignments)(_.offset)
      .mapAsync(transactionsProcessingParallelism) { rawEvents =>
        deserializationQueriesLimiter.execute(
          UpdateReader.toApiUpdate(
            reassignmentEventProjectionProperties = internalUpdateFormat.includeReassignments.map(
              _.eventProjectionProperties
            ),
            transactionEventProjectionProperties = internalUpdateFormat.includeTransactions.map(
              _.internalEventFormat.eventProjectionProperties
            ),
            lfValueTranslation = lfValueTranslation,
          )(reverseIfDescendingOrder(descendingOrder, rawEvents))(
            convertReassignment = reassignment =>
              Offset.tryFromLong(reassignment.offset) -> GetUpdatesResponse(
                GetUpdatesResponse.Update.Reassignment(reassignment)
              ).withPrecomputedSerializedSize(),
            convertTransaction = transaction =>
              Offset.tryFromLong(transaction.offset) -> GetUpdatesResponse(
                GetUpdatesResponse.Update.Transaction(transaction)
              ).withPrecomputedSerializedSize(),
          )
        )
      }
      .mapConcat(identity)
      .mergeSorted(topologyTransactions)(
        Ordering.by[(Offset, GetUpdatesResponse), Long](_._1.unwrap)(longOrdering)
      )
  }

  private def reverseIfDescendingOrder(
      descendingOrder: Boolean,
      rawEvents: Vector[RawEvent],
  ): Vector[RawEvent] =
    if (descendingOrder) rawEvents.reverse else rawEvents

  private def doStreamAcsDelta(
      queryRange: EventsRange,
      txInternalEventFormat: Option[InternalEventFormat],
      reassignmentInternalEventFormat: Option[InternalEventFormat],
      txAndReassignmentFilters: Set[DecomposedFilter],
      justTxFilters: Set[DecomposedFilter],
      justReassignmentFilters: Set[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    val longOrdering: Ordering[Long] =
      orderingBasedOnDescending(descendingOrder = descendingOrder)
    val activateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdActivateQueries, executionContext)
    val deactivateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdDeactivateQueries, executionContext)

    val idsActivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            idStreamName = s"Update event seq IDs for ActivateStakeholder $filter",
            idPageQuery = eventStorageBackend.updateStreamingQueries.activateStakeholderIds(
              witnessO = filter.party,
              templateIdO = filter.templateId,
            ),
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
            metric = dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholder,
            descendingOrder = descendingOrder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for ActivateStakeholder $filter for only Create type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .activateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Create)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for ActivateStakeholder $filter for only Assign type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .activateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Assign)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesAcsDeltaStream.fetchEventActivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadActivateQueries + 1,
            descendingOrder = descendingOrder,
          )
        )

    val idsDeactivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            idStreamName = s"Update event seq IDs for DeactivateStakeholder $filter",
            idPageQuery = eventStorageBackend.updateStreamingQueries.deactivateStakeholderIds(
              witnessO = filter.party,
              templateIdO = filter.templateId,
            ),
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
            metric = dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholder,
            descendingOrder = descendingOrder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for DeactivateStakeholder $filter for only ConsumingExercise type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .deactivateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.ConsumingExercise)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for DeactivateStakeholder $filter for only Unassign type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .deactivateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Unassign)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesAcsDeltaStream.fetchEventDeactivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadDeactivateQueries + 1,
            descendingOrder = descendingOrder,
          )
        )

    val payloadsActivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsActivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Activate
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx =
                txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
              requestingPartiesForReassignment =
                reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            )(connection)
            .reverseIfDescendingOrder(descendingOrder),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventActivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        skipPruningChecks = skipPruningChecks,
      )
    val payloadsDeactivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsDeactivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsAcsDelta(
              EventPayloadSourceForUpdatesAcsDelta.Deactivate
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx =
                txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
              requestingPartiesForReassignment =
                reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            )(connection)
            .reverseIfDescendingOrder(descendingOrder),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesAcsDeltaStream.fetchEventDeactivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        skipPruningChecks = skipPruningChecks,
      )

    payloadsActivate
      .mergeSorted(payloadsDeactivate)(Ordering.by[RawEvent, Long](_.eventSeqId)(longOrdering))
  }

  private def doStreamLedgerEffects(
      queryRange: EventsRange,
      txInternalEventFormat: Option[InternalEventFormat],
      reassignmentInternalEventFormat: Option[InternalEventFormat],
      txAndReassignmentFilters: Set[DecomposedFilter],
      justTxFilters: Set[DecomposedFilter],
      justReassignmentFilters: Set[DecomposedFilter],
      payloadQueriesLimiter: QueueBasedConcurrencyLimiter,
      idPageSizing: IdPageSizing,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    implicit val longOrdering: Ordering[Long] =
      orderingBasedOnDescending(descendingOrder = descendingOrder)

    val activateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdActivateQueries, executionContext)
    val deactivateEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdDeactivateQueries, executionContext)
    val variousWitnessedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdVariousWitnessedQueries, executionContext)

    val idsActivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            idStreamName = s"Update event seq IDs for ActivateStakeholder $filter",
            idPageQuery = eventStorageBackend.updateStreamingQueries.activateStakeholderIds(
              witnessO = filter.party,
              templateIdO = filter.templateId,
            ),
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholder,
            descendingOrder = descendingOrder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for ActivateStakeholder $filter for only Create type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .activateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Create)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for ActivateStakeholder $filter for only Assign type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .activateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Assign)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          txAndReassignmentFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              idStreamName = s"Update event seq IDs for ActivateWitnesses $filter",
              idPageQuery = eventStorageBackend.updateStreamingQueries.activateWitnessesIds(
                witnessO = filter.party,
                templateIdO = filter.templateId,
              ),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsWitness,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            // there are no witnessed only reassignments
            fetchIdsNonFiltered(
              queryRange = queryRange,
              idStreamName = s"Update event seq IDs for ActivateWitnesses $filter",
              idPageQuery = eventStorageBackend.updateStreamingQueries.activateWitnessesIds(
                witnessO = filter.party,
                templateIdO = filter.templateId,
              ),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = activateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivateIdsWitness,
              descendingOrder = descendingOrder,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadActivateQueries + 1,
            descendingOrder = descendingOrder,
          )
        )
    val idsDeactivate =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            idStreamName = s"Update event seq IDs for DeactivateStakeholder $filter",
            idPageQuery = eventStorageBackend.updateStreamingQueries.deactivateStakeholderIds(
              witnessO = filter.party,
              templateIdO = filter.templateId,
            ),
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholder,
            descendingOrder = descendingOrder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for DeactivateStakeholder $filter for only ConsumingExercise type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .deactivateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.ConsumingExercise)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justReassignmentFilters.iterator.map(filter =>
            fetchIdsFiltered(
              queryRange = queryRange,
              idStreamName =
                s"Update event seq IDs for DeactivateStakeholder $filter for only Unassign type",
              idFilterPageQuery = eventStorageBackend.updateStreamingQueries
                .deactivateStakeholderIds(
                  witnessO = filter.party,
                  templateIdO = filter.templateId,
                )
                .filteredForEventTypes(Set(PersistentEventType.Unassign)),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metricForLast =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredRange,
              metricFiltered =
                dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsStakeholderFilteredIds,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          txAndReassignmentFilters.iterator.map(filter =>
            fetchIdsNonFiltered(
              queryRange = queryRange,
              idStreamName = s"Update event seq IDs for DeactivateWitnesses $filter",
              idPageQuery = eventStorageBackend.updateStreamingQueries.deactivateWitnessesIds(
                witnessO = filter.party,
                templateIdO = filter.templateId,
              ),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsWitness,
              descendingOrder = descendingOrder,
            )
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            // only tx-es can have only witnesses, so no filtering needed
            fetchIdsNonFiltered(
              queryRange = queryRange,
              idStreamName = s"Update event seq IDs for DeactivateWitnesses $filter",
              idPageQuery = eventStorageBackend.updateStreamingQueries.deactivateWitnessesIds(
                witnessO = filter.party,
                templateIdO = filter.templateId,
              ),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = deactivateEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivateIdsWitness,
              descendingOrder = descendingOrder,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadDeactivateQueries + 1,
            descendingOrder = descendingOrder,
          )
        )
    val idsVariousWitnessed =
      txAndReassignmentFilters.iterator
        .map(filter =>
          fetchIdsNonFiltered(
            queryRange = queryRange,
            idStreamName = s"Update event seq IDs for VariousWitnesses $filter",
            idPageQuery = eventStorageBackend.updateStreamingQueries.variousWitnessIds(
              witnessO = filter.party,
              templateIdO = filter.templateId,
            ),
            idPageSizing = idPageSizing,
            maxParallelIdQueriesLimiter = variousWitnessedEventIdQueriesLimiter,
            metric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousIdsWitness,
            descendingOrder = descendingOrder,
          )
        )
        .++(
          justTxFilters.iterator.map(filter =>
            // only tx-es can be witnessed only
            fetchIdsNonFiltered(
              queryRange = queryRange,
              idStreamName = s"Update event seq IDs for VariousWitnesses $filter",
              idPageQuery = eventStorageBackend.updateStreamingQueries.variousWitnessIds(
                witnessO = filter.party,
                templateIdO = filter.templateId,
              ),
              idPageSizing = idPageSizing,
              maxParallelIdQueriesLimiter = variousWitnessedEventIdQueriesLimiter,
              metric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousIdsWitness,
              descendingOrder = descendingOrder,
            )
          )
        )
        .toVector
        .pipe(
          mergeSortAndBatch(
            maxOutputBatchSize = maxPayloadsPerPayloadsPage,
            maxOutputBatchCount = maxParallelPayloadVariousWitnessedQueries + 1,
            descendingOrder = descendingOrder,
          )
        )

    val payloadsActivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsActivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsLedgerEffects(
              EventPayloadSourceForUpdatesLedgerEffects.Activate
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx =
                txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
              requestingPartiesForReassignment =
                reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            )(connection)
            .reverseIfDescendingOrder(descendingOrder),
        maxParallelPayloadQueries = maxParallelPayloadActivateQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventActivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        skipPruningChecks = skipPruningChecks,
      )
    val payloadsDeactivate =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsDeactivate,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsLedgerEffects(
              EventPayloadSourceForUpdatesLedgerEffects.Deactivate
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx =
                txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
              requestingPartiesForReassignment =
                reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            )(connection)
            .reverseIfDescendingOrder(descendingOrder),
        maxParallelPayloadQueries = maxParallelPayloadDeactivateQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventDeactivatePayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        skipPruningChecks = skipPruningChecks,
      )
    val payloadsVariousWitnessed =
      fetchPayloads(
        queryRange = queryRange,
        ids = idsVariousWitnessed,
        fetchEvents = (ids, connection) =>
          eventStorageBackend
            .fetchEventPayloadsLedgerEffects(
              EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
            )(
              eventSequentialIds = Ids(ids),
              requestingPartiesForTx =
                txInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
              requestingPartiesForReassignment =
                reassignmentInternalEventFormat.flatMap(_.templatePartiesFilter.allFilterParties),
            )(connection)
            .reverseIfDescendingOrder(descendingOrder),
        maxParallelPayloadQueries = maxParallelPayloadVariousWitnessedQueries,
        dbMetric = dbMetrics.updatesLedgerEffectsStream.fetchEventVariousWitnessedPayloads,
        payloadQueriesLimiter = payloadQueriesLimiter,
        contractStore = contractStore,
        skipPruningChecks = skipPruningChecks,
      )

    payloadsActivate
      .mergeSorted(payloadsDeactivate)(Ordering.by(_.eventSeqId))
      .mergeSorted(payloadsVariousWitnessed)(Ordering.by(_.eventSeqId))
  }

  private def fetchIdsNonFiltered(
      queryRange: EventsRange,
      idStreamName: String,
      idPageQuery: IdPageQuery,
      idPageSizing: IdPageSizing,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      metric: DatabaseMetrics,
      descendingOrder: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[Long, NotUsed] =
    paginatingAsyncStream.streamIdsFromSeekPaginationWithoutIdFilter(
      idStreamName = idStreamName,
      idPageSizing = idPageSizing,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
      initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
      initialEndInclusive = queryRange.endInclusiveEventSeqId,
      descendingOrder = descendingOrder,
    )(idPageQuery)(
      executeIdQuery = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metric)(f)
          }
        }
    )

  private def fetchIdsFiltered(
      queryRange: EventsRange,
      idStreamName: String,
      idFilterPageQuery: IdFilterPageQuery,
      idPageSizing: IdPageSizing,
      maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
      metricForLast: DatabaseMetrics,
      metricFiltered: DatabaseMetrics,
      descendingOrder: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[Long, NotUsed] =
    paginatingAsyncStream.streamIdsFromSeekPaginationWithIdFilter(
      idStreamName = idStreamName,
      idPageSizing = idPageSizing,
      idPageBufferSize = maxPagesPerIdPagesBuffer,
      initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
      initialEndInclusive = queryRange.endInclusiveEventSeqId,
      descendingOrder = descendingOrder,
    )(idFilterPageQuery)(
      executeFetchBounds = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metricForLast)(f)
          }
        },
      idFilterQueryParallelism = idFilterQueryParallelism,
      executeFetchPage = f =>
        maxParallelIdQueriesLimiter.execute {
          globalIdQueriesLimiter.execute {
            dbDispatcher.executeSql(metricFiltered)(f)
          }
        },
    )

  private def mergeSortAndBatch(
      maxOutputBatchSize: Int,
      maxOutputBatchCount: Int,
      descendingOrder: Boolean,
  )(sourcesOfIds: Vector[Source[Long, NotUsed]]): Source[Iterable[Long], NotUsed] =
    EventIdsUtils
      .sortAndDeduplicateIds(descendingOrder)(sourcesOfIds)
      .batchN(
        maxBatchSize = maxOutputBatchSize,
        maxBatchCount = maxOutputBatchCount,
      )

  private def fetchPayloads(
      queryRange: EventsRange,
      ids: Source[Iterable[Long], NotUsed],
      fetchEvents: (Iterable[Long], Connection) => Vector[RawThinEvent],
      maxParallelPayloadQueries: Int,
      dbMetric: DatabaseMetrics,
      payloadQueriesLimiter: ConcurrencyLimiter,
      contractStore: LedgerApiContractStore,
      skipPruningChecks: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[RawEvent, NotUsed] = {
    // Pekko requires for this buffer's size to be a power of two.
    val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
    ids
      .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
      .mapAsync(maxParallelPayloadQueries)(ids =>
        payloadQueriesLimiter.execute {
          globalPayloadQueriesLimiter.execute {
            val pruningCheck: Future[Vector[(RawThinEvent, Option[FatContract])]] => Future[
              Vector[(RawThinEvent, Option[FatContract])]
            ] = if (skipPruningChecks) {
              _.flatMap(events =>
                pruningOffsetService.pruningOffset.map(prunedTo =>
                  events.filter(_._1.offset > prunedTo.fold(0L)(_.unwrap))
                )
              ) // Remove all elements after a pruning offset not to break tryToResolveFatInstance
            } else {
              queryValidRange
                .withRangeNotPruned(
                  minOffsetInclusive = queryRange.startInclusiveOffset,
                  maxOffsetInclusive = queryRange.endInclusiveOffset,
                  errorPruning = (prunedOffset: Offset) =>
                    s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                  errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                    s"Updates request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                        .fold(0L)(_.unwrap)}",
                )(_)
            }
            pruningCheck {
              dbDispatcher
                .executeSql(dbMetric)(fetchEvents(ids, _))
                .flatMap(UpdateReader.withFatContractIfNeeded(contractStore))
            }
              .map(UpdateReader.tryToResolveFatInstance)
          }
        }
      )
      .mapConcat(identity)
  }

  private def orderingBasedOnDescending(descendingOrder: Boolean): Ordering[Long] =
    if (descendingOrder)
      Ordering.Long.reverse
    else
      Ordering.Long
}

object UpdatesStreamReader {
  final implicit class VectorOps[T](vec: Vector[T]) {
    def reverseIfDescendingOrder(descendingOrder: Boolean): Vector[T] =
      if (descendingOrder) vec.reverse else vec
  }
}
