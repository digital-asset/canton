// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v2.state_service.{ActiveContract, GetActiveContractsResponse}
import com.daml.metrics.{Metrics, Timed}
import com.daml.nameof.NameOf.qualifiedNameOfCurrentFunc
import com.daml.tracing
import com.daml.tracing.{SpanAttribute, Spans}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.configuration.AcsStreamsConfig
import com.digitalasset.canton.platform.indexer.parallel.BatchN
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawActiveContract
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.TransactionsReader.{
  deserializeEntry,
  endSpanOnTermination,
}
import com.digitalasset.canton.platform.store.dao.{
  DbDispatcher,
  EventProjectionProperties,
  PaginatingAsyncStream,
}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
  Telemetry,
}
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

/** Streams ACS events (active contracts) in a two step process consisting of:
  * 1) fetching event sequential ids of the active contracts based on the filtering constraints,
  * 2) fetching the active contracts based on the fetched event sequential ids.
  *
  * Details:
  * An input filtering constraint (consisting of parties and template ids) is converted into
  * decomposed filtering constraints (a constraint with exactly one party and at most one template id).
  * For each decomposed filter, the matching event sequential ids are fetched in parallel and then merged into
  * a strictly increasing sequence. The elements from this sequence are then batched and the batch ids serve as
  * the input to the payload fetching step.
  */
class ACSReader(
    config: AcsStreamsConfig,
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    dispatcher: DbDispatcher,
    queryNonPruned: QueryNonPruned,
    eventStorageBackend: EventStorageBackend,
    lfValueTranslation: LfValueTranslation,
    metrics: Metrics,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  private val dbMetrics = metrics.daml.index.db

  def streamActiveContracts(
      filteringConstraints: TemplatePartiesFilter,
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val (activeAtOffset, _) = activeAt
    val span =
      Telemetry.Transactions.createSpan(tracer, activeAtOffset)(qualifiedNameOfCurrentFunc)
    logger.debug(
      s"getActiveContracts($activeAtOffset, $filteringConstraints, $eventProjectionProperties)"
    )
    doStreamActiveContracts(
      filteringConstraints,
      activeAt,
      eventProjectionProperties,
      multiDomainEnabled,
    )
      .wireTap(getActiveContractsResponse => {
        val event =
          tracing.Event("contract", Map((SpanAttribute.Offset, getActiveContractsResponse.offset)))
        Spans.addEventToSpan(event, span)
      })
      .watchTermination()(endSpanOnTermination(span))
  }

  private def doStreamActiveContracts(
      filter: TemplatePartiesFilter,
      activeAt: (Offset, Long),
      eventProjectionProperties: EventProjectionProperties,
      multiDomainEnabled: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val allFilterParties = filter.allFilterParties
    val decomposedFilters = FilterUtils.decomposeFilters(filter).toVector
    val createIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelIdCreateQueries, executionContext)
    val assignIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelIdCreateQueries, executionContext)
    val localPayloadQueriesLimiter =
      new QueueBasedConcurrencyLimiter(config.maxParallelPayloadCreateQueries, executionContext)
    val idQueryPageSizing = IdPageSizing.calculateFrom(
      maxIdPageSize = config.maxIdsPerIdPage,
      workingMemoryInBytesForIdPages =
        config.maxWorkingMemoryInBytesForIdPages / (if (multiDomainEnabled) 2 else 1),
      numOfDecomposedFilters = decomposedFilters.size,
      numOfPagesInIdPageBuffer = config.maxPagesPerIdPagesBuffer,
    )

    def fetchCreateIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
      )((state: IdPaginationState) =>
        createIdQueriesLimiter.execute(
          globalIdQueriesLimiter.execute(
            dispatcher.executeSql(metrics.daml.index.db.getActiveContractIdsForCreated) { connection =>
              val ids =
                eventStorageBackend.transactionStreamingQueries
                  .fetchIdsOfCreateEventsForStakeholder(
                    stakeholder = filter.party,
                    templateIdO = filter.templateId,
                    startExclusive = state.fromIdExclusive,
                    endInclusive = activeAt._2,
                    limit = state.pageSize,
                  )(connection)
              logger.debug(
                s"ActiveContractIds for create events $filter returned #${ids.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              ids
            }
          )
        )
      )

    def fetchAssignIds(filter: DecomposedFilter): Source[Long, NotUsed] =
      PaginatingAsyncStream.streamIdsFromSeekPagination(
        idPageSizing = idQueryPageSizing,
        idPageBufferSize = config.maxPagesPerIdPagesBuffer,
        initialFromIdExclusive = 0L,
      )((state: IdPaginationState) =>
        assignIdQueriesLimiter.execute(
          globalIdQueriesLimiter.execute(
            dispatcher.executeSql(metrics.daml.index.db.getActiveContractIdsForAssigned) {
              connection =>
                val ids =
                  eventStorageBackend.fetchAssignEventIdsForStakeholder(
                    stakeholder = filter.party,
                    templateId = filter.templateId,
                    startExclusive = state.fromIdExclusive,
                    endInclusive = activeAt._2,
                    limit = state.pageSize,
                  )(connection)
                logger.debug(
                  s"ActiveContractIds for assign events $filter returned #${ids.size} ${ids.lastOption
                      .map(last => s"until $last")
                      .getOrElse("")}"
                )
                ids
            }
          )
        )
      )

    def fetchNotArchivedCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.Entry[Raw.FlatEvent]]] =
      globalPayloadQueriesLimiter.execute(
        dispatcher.executeSql(metrics.daml.index.db.getActiveContractBatchForNotArchived) {
          connection =>
            val result = queryNonPruned.executeSql(
              eventStorageBackend.activeContractCreateEventBatch(
                eventSequentialIds = ids,
                allFilterParties = allFilterParties,
                endInclusive = activeAt._2,
              )(connection),
              activeAt._1,
              pruned =>
                ACSReader.acsBeforePruningErrorReason(
                  acsOffset = activeAt._1.toHexString,
                  prunedUpToOffset = pruned.toHexString,
                ),
            )(connection, implicitly)
            logger.debug(
              s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                  .map(last => s"until $last")
                  .getOrElse("")}"
            )
            result
        }
      )

    def fetchActiveCreatePayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.daml.index.db.getActiveContractBatchForCreated) {
            connection =>
              val result = queryNonPruned.executeSql(
                eventStorageBackend.activeContractCreateEventBatchV2(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAt._2,
                )(connection),
                activeAt._1,
                pruned =>
                  ACSReader.acsBeforePruningErrorReason(
                    acsOffset = activeAt._1.toHexString,
                    prunedUpToOffset = pruned.toHexString,
                  ),
              )(connection, implicitly)
              logger.debug(
                s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
          }
        )
      )

    def fetchActiveAssignPayloads(
        ids: Iterable[Long]
    ): Future[Vector[EventStorageBackend.RawActiveContract]] =
      localPayloadQueriesLimiter.execute(
        globalPayloadQueriesLimiter.execute(
          dispatcher.executeSql(metrics.daml.index.db.getActiveContractBatchForAssigned) {
            connection =>
              val result = queryNonPruned.executeSql(
                eventStorageBackend.activeContractAssignEventBatch(
                  eventSequentialIds = ids,
                  allFilterParties = allFilterParties,
                  endInclusive = activeAt._2,
                )(connection),
                activeAt._1,
                pruned =>
                  ACSReader.acsBeforePruningErrorReason(
                    acsOffset = activeAt._1.toHexString,
                    prunedUpToOffset = pruned.toHexString,
                  ),
              )(connection, implicitly)
              logger.debug(
                s"getActiveContractBatch returned ${ids.size}/${result.size} ${ids.lastOption
                    .map(last => s"until $last")
                    .getOrElse("")}"
              )
              result
          }
        )
      )
    // Akka requires for this buffer's size to be a power of two.
    val inputBufferSize =
      Utils.largestSmallerOrEqualPowerOfTwo(config.maxParallelPayloadCreateQueries)

    if (multiDomainEnabled) {
      val activeFromCreatePipe =
        decomposedFilters
          .map(fetchCreateIds)
          .pipe(EventIdsUtils.sortAndDeduplicateIds)
          .via(
            BatchN(
              maxBatchSize = config.maxPayloadsPerPayloadsPage,
              maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
            )
          )
          .async
          .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
          .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActiveCreatePayloads)
          .mapConcat(identity)
      val activeFromAssignPipe =
        decomposedFilters
          .map(fetchAssignIds)
          .pipe(EventIdsUtils.sortAndDeduplicateIds)
          .via(
            BatchN(
              maxBatchSize = config.maxPayloadsPerPayloadsPage,
              maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
            )
          )
          .async
          .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
          .mapAsync(config.maxParallelPayloadCreateQueries)(fetchActiveAssignPayloads)
          .mapConcat(identity)

      activeFromCreatePipe
        .mergeSorted(activeFromAssignPipe)(Ordering.by(_.eventSequentialId))
        .mapAsync(config.contractProcessingParallelism)(
          toApiResponse(_, eventProjectionProperties)
        )
    } else {
      decomposedFilters
        .map(fetchCreateIds)
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .via(
          BatchN(
            maxBatchSize = config.maxPayloadsPerPayloadsPage,
            maxBatchCount = config.maxParallelPayloadCreateQueries + 1,
          )
        )
        .async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(config.maxParallelPayloadCreateQueries)(fetchNotArchivedCreatePayloads)
        .mapAsync(config.contractProcessingParallelism)(
          deserializeLfValues(_, eventProjectionProperties)
        )
        .mapConcat(
          TransactionConversions.toGetActiveContractsResponse(_)(
            ErrorLoggingContext(logger, loggingContext)
          )
        )
    }
  }

  private def deserializeLfValues(
      rawEvents: Vector[EventStorageBackend.Entry[Raw.FlatEvent]],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[Vector[EventStorageBackend.Entry[Event]]] = {
    Timed.future(
      future = Future.delegate(
        Future.traverse(rawEvents)(
          deserializeEntry(eventProjectionProperties, lfValueTranslation)
        )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )
  }

  private def toApiResponse(
      rawActiveContract: RawActiveContract,
      eventProjectionProperties: EventProjectionProperties,
  )(implicit lc: LoggingContextWithTrace): Future[GetActiveContractsResponse] =
    Timed.future(
      future = Future.delegate(
        TransactionsReader
          .deserializeRawCreatedEvent(lfValueTranslation, eventProjectionProperties)(
            rawActiveContract.rawCreatedEvent
          )
          .map(createdEvent =>
            GetActiveContractsResponse(
              offset = "", // empty for all data entries
              workflowId = rawActiveContract.workflowId.getOrElse(""),
              contractEntry = GetActiveContractsResponse.ContractEntry.ActiveContract(
                ActiveContract(
                  createdEvent = Some(createdEvent),
                  domainId = rawActiveContract.domainId,
                  reassignmentCounter = rawActiveContract.reassignmentCounter,
                )
              ),
            )
          )
      ),
      timer = dbMetrics.getActiveContracts.translationTimer,
    )

}

object ACSReader {

  def acsBeforePruningErrorReason(acsOffset: String, prunedUpToOffset: String): String =
    s"Active contracts request at offset ${acsOffset} precedes pruned offset ${prunedUpToOffset}"

}
