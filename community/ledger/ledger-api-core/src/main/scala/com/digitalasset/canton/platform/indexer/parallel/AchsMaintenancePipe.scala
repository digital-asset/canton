// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsAddActivationsParams,
  AchsLastPointers,
  AchsRemoveDeactivatedParams,
  AchsState,
}
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.cache.AchsStateCache
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

/** A pipe that runs ACHS (Active Contracts Head Snapshot) maintenance. It consists of 5 stages:
  *   1. Aggregate: convert incoming batches into AchsWorkDistance, accumulate until the work in
  *      either dimension breaches the aggregation threshold, then emit AchsWorkRange chunks
  *   1. Bump ACHS validAt: update the ACHS validAt based on the max event sequential id seen
  *   1. Populate: add activations to the ACHS
  *   1. Remove: remove deactivated entries from the ACHS
  *   1. Update last pointers: batch and update lastPopulated/lastRemoved in the ACHS state table
  */
object AchsMaintenancePipe {

  def apply[T](
      parameterStorageBackend: ParameterStorageBackend,
      eventStorageBackend: EventStorageBackend,
      dbDispatcher: DbDispatcher,
      achsStateCache: AchsStateCache,
      toAchsWorkDistance: T => AchsWorkDistance,
      initialWork: AchsWorkDistance,
      populationParallelism: Int,
      removalParallelism: Int,
      aggregationThreshold: Long,
      metrics: LedgerApiServerMetrics,
      executionContext: ExecutionContext,
      logger: TracedLogger,
      fullDrain: Boolean,
  )(implicit traceContext: TraceContext): Flow[T, AchsWorkRange, NotUsed] =
    maintenanceFlow(
      toAchsWork = toAchsWorkDistance,
      initialWork = initialWork,
      bumpAchsValidAt = bumpAchsValidAt(
        storeAchsValidAt = storeAchsState(
          storeAchsStateFunction = parameterStorageBackend.updateACHSValidAt(_: Long),
          dbDispatcher = dbDispatcher,
          metrics = metrics,
          logger = logger,
        ),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = logger,
        metrics = metrics,
      ),
      populateAchsActivations = populateAchsActivations(
        persistActivationsF = persistChangesF(
          persistChanges = eventStorageBackend.addActivationsToAchs,
          dbDispatcher = dbDispatcher,
          metrics = metrics,
        ),
        logger = logger,
        executionContext = executionContext,
      ),
      removeDeactivatedFromAchs = removeDeactivatedFromAchs(
        removeDeactivatedF = persistChangesF(
          persistChanges = eventStorageBackend.removeDeactivatedFromAchs,
          dbDispatcher = dbDispatcher,
          metrics = metrics,
        ),
        executionContext = executionContext,
        logger = logger,
      ),
      populationParallelism = populationParallelism,
      removalParallelism = removalParallelism,
      updateAchsLastPointers = storeAchsLastPointersF(
        persistAchsLastPointersF = storeAchsState(
          storeAchsStateFunction = parameterStorageBackend.updateACHSLastPointers,
          dbDispatcher = dbDispatcher,
          metrics = metrics,
          logger = logger,
        ),
        achsStateCache = achsStateCache,
        executionContext = executionContext,
        logger = logger,
        metrics = metrics,
      ),
      aggregationThreshold = aggregationThreshold,
      initialAchsState = achsStateCache.get(),
      fullDrain = fullDrain,
    )

  /** Computes the initial work distance for the ACHS maintenance pipe. This can be negative,
    * representing a debt that must be absorbed by incoming batches before any real work starts.
    */
  private[platform] def initialWork(
      achsState: AchsState,
      lastEventSeqId: Long,
      achsConfig: AchsConfig,
  ): AchsWorkDistance = {
    val validAtDist = achsConfig.validAtDistanceTarget.unwrap
    val populateDist = achsConfig.lastPopulatedDistanceTarget.unwrap
    // How far the ledger head is from the ACHS pointers, accounting for the configured distances.
    // Negative values mean the ledger hasn't advanced far enough for work to begin in that dimension.
    AchsWorkDistance(
      populate =
        (lastEventSeqId - validAtDist - populateDist) - achsState.lastPointers.lastPopulated,
      remove = (lastEventSeqId - validAtDist) - achsState.lastPointers.lastRemoved,
    )
  }

  /** Represents the accumulated work to be done, measured in event sequential id deltas. */
  final case class AchsWorkDistance(populate: Long, remove: Long) extends PrettyPrinting {
    def +(other: AchsWorkDistance): AchsWorkDistance =
      AchsWorkDistance(populate = populate + other.populate, remove = remove + other.remove)

    def -(other: AchsWorkDistance): AchsWorkDistance =
      AchsWorkDistance(populate = populate - other.populate, remove = remove - other.remove)

    /** For each dimension, returns threshold if value >= threshold, else 0. This ensures we only
      * emit work for a dimension that has accumulated enough.
      */
    def cap(threshold: Long): AchsWorkDistance =
      AchsWorkDistance(
        populate = if (populate >= threshold) threshold else 0L,
        remove = if (remove >= threshold) threshold else 0L,
      )

    override protected def pretty: Pretty[AchsWorkDistance] = prettyOfClass(
      param("populate", _.populate),
      param("remove", _.remove),
    )
  }

  /** Represents a range of event sequential ids. */
  final case class EventSeqIdRange(
      startExclusive: Long,
      endInclusive: Long,
  )

  /** Represents a concrete range of event sequential ids for population and removal. */
  final case class AchsWorkRange(
      activationsPopulation: EventSeqIdRange,
      deactivatedRemoval: EventSeqIdRange,
  )

  private[platform] def maintenanceFlow[T](
      toAchsWork: T => AchsWorkDistance,
      bumpAchsValidAt: AchsWorkRange => Future[AchsWorkRange],
      populateAchsActivations: AchsWorkRange => Future[AchsWorkRange],
      removeDeactivatedFromAchs: AchsWorkRange => Future[AchsWorkRange],
      populationParallelism: Int,
      removalParallelism: Int,
      updateAchsLastPointers: AchsWorkRange => Future[AchsWorkRange],
      aggregationThreshold: Long,
      initialAchsState: AchsState,
      initialWork: AchsWorkDistance,
      fullDrain: Boolean,
  ): Flow[T, AchsWorkRange, NotUsed] =
    Flow[T]
      // Stage 0: Convert to work distance, accumulate, and drain threshold-sized AchsWorkRange chunks.
      .map(toAchsWork)
      .statefulMap(create = () => initialAchsState -> initialWork)(
        f = { case ((achsState, accWorkDistance), incomingWorkDistance) =>
          val totalWork = accWorkDistance + incomingWorkDistance

          // repeatedly drain threshold-sized chunks while the threshold is breached
          val (newState, remainingWork, workRanges) = drain(
            aggregationThreshold = aggregationThreshold,
            state = achsState,
            remaining = totalWork,
            acc = Vector.empty,
            fullDrain = fullDrain,
          )
          (newState, remainingWork) -> workRanges
        },
        onComplete = _ => None,
      )
      .mapConcat(identity)
      // Stage 1: Bump the ACHS validAt
      .mapAsync(1)(bumpAchsValidAt)
      // Stage 2: Populate ACHS with activations
      .async
      .mapAsync(populationParallelism)(populateAchsActivations)
      // Stage 3: Remove deactivated entries from the ACHS
      .async
      .mapAsync(removalParallelism)(removeDeactivatedFromAchs)
      // Stage 4: Batch and update ACHS last pointers (lastPopulated, lastRemoved)
      // keep the latest maintenance params
      .conflate((_, latest) => latest)
      .mapAsync(1)(updateAchsLastPointers)

  private def applyWork(
      state: AchsState,
      work: AchsWorkDistance,
  ): (AchsWorkRange, AchsState) = {
    val workRange = AchsWorkRange(
      activationsPopulation = EventSeqIdRange(
        startExclusive = state.lastPointers.lastPopulated,
        endInclusive = state.lastPointers.lastPopulated + work.populate,
      ),
      deactivatedRemoval = EventSeqIdRange(
        startExclusive = state.lastPointers.lastRemoved,
        endInclusive = state.lastPointers.lastRemoved + work.remove,
      ),
    )
    val newState = AchsState(
      validAt = state.validAt,
      lastPointers = AchsLastPointers(
        lastPopulated = state.lastPointers.lastPopulated + work.populate,
        lastRemoved = state.lastPointers.lastRemoved + work.remove,
      ),
    )
    (workRange, newState)
  }

  @scala.annotation.tailrec
  private[platform] def drain(
      aggregationThreshold: Long,
      state: AchsState,
      remaining: AchsWorkDistance,
      acc: Vector[AchsWorkRange],
      fullDrain: Boolean,
  ): (AchsState, AchsWorkDistance, Vector[AchsWorkRange]) =
    if (remaining.populate >= aggregationThreshold || remaining.remove >= aggregationThreshold) {
      val chunk = remaining.cap(aggregationThreshold)
      val (workRange, newState) = applyWork(state, chunk)
      drain(
        aggregationThreshold = aggregationThreshold,
        state = newState,
        remaining = remaining - chunk,
        acc = acc :+ workRange,
        fullDrain = fullDrain,
      )
    } else if (fullDrain && (remaining.populate > 0L || remaining.remove > 0L)) {
      // Flush any positive sub-threshold remainder as a final undersized chunk.
      // Negative dimensions are clamped to 0 (they represent debt, not real work).
      val clamped = AchsWorkDistance(
        populate = remaining.populate.max(0L),
        remove = remaining.remove.max(0L),
      )
      val (workRange, newState) = applyWork(state, clamped)
      (newState, remaining - clamped, acc :+ workRange)
    } else {
      (state, remaining, acc)
    }

  /** Bumps the ACHS validAt based on the maximum event sequential id seen (newValidAt =
    * deactivatedRemoval.endInclusive).
    */
  private[platform] def bumpAchsValidAt(
      storeAchsValidAt: Long => Future[Unit],
      achsStateCache: AchsStateCache,
      executionContext: ExecutionContext,
      logger: TracedLogger,
      metrics: LedgerApiServerMetrics,
  )(workRange: AchsWorkRange)(implicit traceContext: TraceContext): Future[AchsWorkRange] = {
    val newValidAt = workRange.deactivatedRemoval.endInclusive.max(0L)
    val currentValidAt = achsStateCache.get().validAt

    if (currentValidAt >= newValidAt) {
      logger.trace(
        s"Not bumping ACHS validAt as the new validAt $newValidAt is not greater than the current validAt $currentValidAt."
      )
      Future.successful(workRange)
    } else {
      logger.debug(s"Bumping ACHS validAt from $currentValidAt to $newValidAt.")
      // the in-memory state is used to determine whether the ACHS is valid to fetch from it, so it must be updated before persisting to the database
      achsStateCache
        .updateValidAt(newValidAt)
      metrics.indexer.achsValidAt.updateValue(newValidAt)
      storeAchsValidAt(newValidAt)
        .map(_ => workRange)(executionContext)
    }
  }

  /** Adds activations to the ACHS. */
  private[platform] def populateAchsActivations(
      persistActivationsF: AchsAddActivationsParams => LoggingContextWithTrace => Future[Unit],
      logger: TracedLogger,
      executionContext: ExecutionContext,
  )(workRange: AchsWorkRange)(implicit traceContext: TraceContext): Future[AchsWorkRange] = {
    val loggingContextWithTrace: LoggingContextWithTrace =
      new LoggingContextWithTrace(LoggingEntries.empty, traceContext)

    val endInclusive = workRange.activationsPopulation.endInclusive
    val startExclusive = workRange.activationsPopulation.startExclusive
    // the populations should be active at the latest validAt which is the end of the deactivated removal range
    val activeAt = workRange.deactivatedRemoval.endInclusive

    if (endInclusive > startExclusive) {
      logger.debug(
        s"Adding activations to ACHS in range ($startExclusive, $endInclusive] active at $activeAt."
      )
      persistActivationsF(
        AchsAddActivationsParams(
          startExclusive = startExclusive,
          endInclusive = endInclusive,
          activeAt = activeAt,
        )
      )(loggingContextWithTrace)
    } else Future.unit
  }.map(_ => workRange)(executionContext)

  /** Removes deactivated entries from the ACHS. It must run after the parallel population stage to
    * guarantee that activations from prior batches have been added to the ACHS before potentially
    * removing them.
    */
  private[platform] def removeDeactivatedFromAchs(
      removeDeactivatedF: AchsRemoveDeactivatedParams => LoggingContextWithTrace => Future[Unit],
      executionContext: ExecutionContext,
      logger: TracedLogger,
  )(workRange: AchsWorkRange)(implicit traceContext: TraceContext): Future[AchsWorkRange] = {
    val endInclusive = workRange.deactivatedRemoval.endInclusive
    val startExclusive = workRange.deactivatedRemoval.startExclusive
    val populationEnd = workRange.activationsPopulation.endInclusive

    if (populationEnd <= 0L) {
      logger.debug(
        s"Skipping ACHS removal as no population has been assigned up to this point (populationEnd=$populationEnd, removalStart=$startExclusive, removalEnd=$endInclusive)."
      )
      Future.unit
    } else if (endInclusive > startExclusive) {
      val loggingContextWithTrace: LoggingContextWithTrace =
        new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
      logger.debug(
        s"Removing deactivated entries from ACHS in range ($startExclusive, $endInclusive]."
      )
      removeDeactivatedF(
        AchsRemoveDeactivatedParams(
          startExclusive = startExclusive,
          endInclusive = endInclusive,
        )
      )(loggingContextWithTrace)
    } else Future.unit
  }.map(_ => workRange)(executionContext)

  /** Persists the ACHS lastPopulated and lastRemoved pointers and updates the in-memory cache. */
  private[platform] def storeAchsLastPointersF(
      persistAchsLastPointersF: AchsLastPointers => Future[Unit],
      achsStateCache: AchsStateCache,
      executionContext: ExecutionContext,
      logger: TracedLogger,
      metrics: LedgerApiServerMetrics,
  )(workRange: AchsWorkRange)(implicit traceContext: TraceContext): Future[AchsWorkRange] = {
    val lastPopulated = workRange.activationsPopulation.endInclusive
    val lastRemoved = workRange.deactivatedRemoval.endInclusive

    if (lastRemoved > 0L) {
      val lastPointers = AchsLastPointers(lastRemoved = lastRemoved, lastPopulated = lastPopulated)
      // the in-memory state is used to determine whether the ACHS is valid to fetch from it, so it must be updated before persisting to the database
      achsStateCache.updateLastPointers(lastPointers)
      metrics.indexer.achsLastPopulated.updateValue(lastPopulated)
      metrics.indexer.achsLastRemoved.updateValue(lastRemoved)
      persistAchsLastPointersF(lastPointers)
        .map { _ =>
          logger.debug(
            s"Updated ACHS last pointers: lastRemoved=$lastRemoved, lastPopulated=$lastPopulated."
          )
          workRange
        }(executionContext)
    } else {
      logger.debug(
        s"Skipping ACHS last pointers update as the new lastRemoved and lastPopulated are not greater than 0."
      )
      Future.successful(workRange)
    }
  }

  private def persistChangesF[T](
      persistChanges: T => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
  )(params: T)(loggingContext: LoggingContextWithTrace): Future[Unit] =
    dbDispatcher.executeSql(metrics.indexer.achsProcessing) { connection =>
      persistChanges(params)(connection)
    }(loggingContext)

  private def storeAchsState[T](
      storeAchsStateFunction: T => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      logger: TracedLogger,
  )(changes: T)(implicit traceContext: TraceContext): Future[Unit] =
    LoggingContextWithTrace.withNewLoggingContext() { implicit loggingContext =>
      dbDispatcher.executeSql(metrics.indexer.achsProcessing) { connection =>
        storeAchsStateFunction(changes)(connection)
        logger.debug(s"Changed ACHS state to $changes.")(traceContext)
      }
    }
}
