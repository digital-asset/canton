// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.indexer.parallel.AchsMaintenancePipe.{
  AchsWorkDistance,
  AchsWorkRange,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.{
  AchsLastPointers,
  AchsState,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.DbLockMeta
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  LedgerEnd,
  ParameterStorageBackend,
  StringInterningStorageBackend,
}
import com.digitalasset.canton.platform.store.cache.AchsStateCache
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interning.UpdatingStringInterningView
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.ShutdownInProgress
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Cancellable
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import java.sql.Connection
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

private[platform] final case class InitializeParallelIngestion(
    providedParticipantId: Ref.ParticipantId,
    ingestionStorageBackend: IngestionStorageBackend[?],
    parameterStorageBackend: ParameterStorageBackend,
    eventStorageBackend: EventStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    stringInterningStorageBackend: StringInterningStorageBackend,
    updatingStringInterningView: UpdatingStringInterningView,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    achsStateCache: AchsStateCache,
    achsConfig: Option[AchsConfig],
    contractPruningDbLockMeta: DbLockMeta,
    metrics: LedgerApiServerMetrics,
    loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer)
    extends NamedLogging {

  def apply(
      dbDispatcher: DbDispatcher,
      initializeInMemoryState: (Option[LedgerEnd], AchsState) => Future[Unit],
      shutdownRequested: ShutdownInProgress,
  ): Future[Future[(Option[LedgerEnd], AchsWorkDistance)]] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    logger.info(s"Attempting to initialize with participant ID $providedParticipantId")
    for {
      _ <- dbDispatcher.executeSql(metrics.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            participantId = ParticipantId(providedParticipantId)
          ),
          loggerFactory,
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.indexer.initialization) { connection =>
        // addContractPruningCandidatesAfter must execute before deletePartiallyIngestedData
        // addContractPruningCandidatesAfter and deletePartiallyIngestedData must be in one transaction
        // so that it cannot be intersected with pruning contract candidate cleansing: that might remove candidates
        // as they are referenced after the ledger-end in corner cases.
        eventStorageBackend.addContractPruningCandidatesAfter(
          eventSeqIdExclusive = ledgerEnd
            .map(_.lastEventSeqId)
            .getOrElse(-1L), // if no watermark we gather candidates from all events
          contractPruningDbLockMeta = contractPruningDbLockMeta,
        )(
          connection = connection,
          traceContext = loggingContext.traceContext,
        )
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)(connection)
      }
      _ <- updatingStringInterningView.update(ledgerEnd.map(_.lastStringInterningId)) {
        (fromExclusive, toInclusive) =>
          implicit val loggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.empty
          dbDispatcher.executeSql(metrics.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
      }
      // post processing recovery should come after initializing string interning when the dependent storage backend operations are running
      postProcessingEndOffset <- dbDispatcher.executeSql(metrics.index.db.getPostProcessingEnd)(
        parameterStorageBackend.postProcessingEnd
      )
      potentiallyNonPostProcessedCompletions <- ledgerEnd.map(_.lastOffset) match {
        case Some(lastOffset) =>
          dbDispatcher.executeSql(
            metrics.index.db.getPostProcessingEnd
          )(
            completionStorageBackend.commandCompletionsForRecovery(
              startInclusive = postProcessingEndOffset.fold(Offset.firstOffset)(_.increment),
              endInclusive = lastOffset,
            )
          )
        case None => Future.successful(Vector.empty)
      }
      _ <- postProcessor(potentiallyNonPostProcessedCompletions, loggingContext.traceContext)
      _ <- dbDispatcher.executeSql(metrics.indexer.postProcessingEndIngestion)(
        parameterStorageBackend.updatePostProcessingEnd(ledgerEnd.map(_.lastOffset))
      )
      (achsState, achsRequiresVacuumingAndReindexing) <- dbDispatcher.executeSql(
        metrics.indexer.achsStateInitialization
      )(
        initializeAchsState(achsConfig)
      )
      _ <-
        if (achsRequiresVacuumingAndReindexing) {
          dbDispatcher.executeSql(metrics.indexer.vacuumAndReindexAchsState)(implicit c =>
            parameterStorageBackend.vacuumAndReindexAchsTables()
          )
        } else Future.unit
      _ <- initializeInMemoryState(ledgerEnd, achsState)
    } yield {
      logger.info(s"Indexer initialized at $ledgerEnd with ACHS state $achsState")
      initializeAchs(
        achsConfig = achsConfig,
        ledgerEnd = ledgerEnd,
        dbDispatcher = dbDispatcher,
        shutdownRequested = shutdownRequested,
      )
    }
  }

  private def initializeAchsState(
      achsConfigO: Option[AchsConfig]
  )(connection: Connection)(implicit
      loggingContext: LoggingContextWithTrace
  ): (AchsState, Boolean) =
    (parameterStorageBackend.fetchAchsState(connection), achsConfigO) match {
      case (None, None) =>
        logger.info(s"ACHS State not found, also not configured")
        AchsState.empty -> false

      case (None, Some(_)) =>
        logger.info(s"ACHS State not found, creating new empty state as ACHS is enabled")
        // Clearing ACHS data first to start from scratch
        parameterStorageBackend.clearAchsStateAndData()(connection, implicitly)
        parameterStorageBackend.insertAchsState(AchsState.empty)(connection)
        AchsState.empty -> true

      case (Some(_), None) =>
        logger.info("ACHS is disabled, clearing existing ACHS data")
        // Clearing ACHS data here is safe because configuration is not changing dynamically.
        // Otherwise, clearing could race with ACS retrieval that relies on ACHS data,
        // as pointers would be updated after the data is already evicted.
        parameterStorageBackend.clearAchsStateAndData()(connection, implicitly)
        AchsState.empty -> true

      case (Some(existingState), Some(_)) =>
        // snapshot exists (behind or ahead), use existing state.
        // initialWork will compute the correct work distance:
        //   - positive values for catch-up (behind)
        //   - negative values as debt (ahead, will be absorbed by incoming batches)
        logger.info(s"ACHS resuming from existing state: $existingState")
        eventStorageBackend.deletePartiallyIngestedAchsData(
          fromExclusiveEventSeqId = existingState.lastPointers.lastPopulated
        )(connection)
        existingState -> false
    }

  private def initializeAchs(
      achsConfig: Option[AchsConfig],
      ledgerEnd: Option[LedgerEnd],
      dbDispatcher: DbDispatcher,
      shutdownRequested: ShutdownInProgress,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[(Option[LedgerEnd], AchsWorkDistance)] =
    achsConfig match {
      case Some(config) =>
        createAchsSnapshot(
          lastEventSeqId = ledgerEnd.map(_.lastEventSeqId).getOrElse(0L),
          config = config,
          dbDispatcher = dbDispatcher,
          shutdownRequested = shutdownRequested,
        ).map(ledgerEnd -> _)
      case None =>
        // not used when ACHS is disabled
        Future.successful(ledgerEnd -> AchsWorkDistance(populate = 0L, remove = 0L))
    }

  /** Eagerly creates the ACHS snapshot by running AchsMaintenancePipe in two phases:
    *
    *   - Phase 1 (Removal): Removes deactivated entries and bumps validAt. No population is done.
    *   - Phase 2 (Copy): Copies over activations using the updated validAt from Phase 1.
    *
    * By splitting into two phases, the copy phase only adds entries that are still active after all
    * removals and validAt located at its final position.
    *
    * The pipe runs in fullDrain mode, flushing any sub-threshold remainder when the finite init
    * stream completes, so all positive initial work is fully processed. After both phases complete,
    * the remaining work distance is recalculated from the updated in-memory ACHS state, which
    * correctly accounts for debt (negative work when ACHS is ahead).
    */
  private def createAchsSnapshot(
      lastEventSeqId: Long,
      config: AchsConfig,
      dbDispatcher: DbDispatcher,
      shutdownRequested: ShutdownInProgress,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Future[AchsWorkDistance] = {
    val initialState = achsStateCache.get()
    val initialWork = AchsMaintenancePipe.initialWork(initialState, lastEventSeqId, config)
    logger.info(s"Initializing ACHS snapshot with initial work distance: $initialWork")

    val lastPointers = new AtomicReference[AchsLastPointers](initialState.lastPointers)
    val cancellable =
      logProgress[AchsLastPointers](
        action = "ACHS initialization",
        targetState = AchsLastPointers(
          lastPopulated = initialState.lastPointers.lastPopulated +
            initialWork.populate.max(0L),
          lastRemoved = initialState.lastPointers.lastRemoved +
            initialWork.remove.max(0L),
        ),
        state = lastPointers,
        calcProgress = (from, to) =>
          (to.lastPopulated - from.lastPopulated) + (to.lastRemoved - from.lastRemoved),
      )

    achsMaintenancePipeSource(
      initialWork = initialWork,
      dbDispatcher = dbDispatcher,
      achsConfig = config,
    )
      .map { elem =>
        lastPointers.set(
          AchsLastPointers(
            lastPopulated = elem.activationsPopulation.endInclusive,
            lastRemoved = elem.deactivatedRemoval.endInclusive,
          )
        )
        elem
      }
      .takeWhile(_ => !shutdownRequested())
      .runWith(Sink.ignore)
      .map { _ =>
        cancellable.cancel().discard
        // After both phases, the in-memory ACHS state reflects the actual pointers
        // advanced by the pipe (including the flushed sub-threshold remainder via fullDrain).
        // Recalculate remaining work from the updated state to correctly account for
        // debt (negative work that couldn't be consumed).
        val updatedAchsState = achsStateCache.get()
        val remainingWork =
          AchsMaintenancePipe.initialWork(updatedAchsState, lastEventSeqId, config)
        if (!shutdownRequested())
          logger.info(
            s"ACHS snapshot initialization finished. Initial work: $initialWork, remaining work: $remainingWork (updated ACHS state: $updatedAchsState)"
          )
        else
          logger.info(
            s"ACHS snapshot initialization interrupted by shutdown request. Initial work: $initialWork, work at interruption: $remainingWork (updated ACHS state: $updatedAchsState)"
          )
        remainingWork
      }
  }

  private def logProgress[S](
      action: String,
      targetState: S,
      state: AtomicReference[S],
      calcProgress: (S, S) => Long,
      interval: FiniteDuration = 10.seconds,
  )(implicit loggingContext: LoggingContextWithTrace): Cancellable = {
    val startTime = System.currentTimeMillis()
    val initialState = state.get()
    val totalDist = calcProgress(initialState, targetState)
    val prevReportedState = new AtomicReference[S](initialState)

    materializer.system.scheduler.scheduleWithFixedDelay(
      initialDelay = interval,
      delay = interval,
    ) { () =>
      val currentState = state.get()
      val prevState = prevReportedState.getAndSet(currentState)
      val delta = calcProgress(prevState, currentState)
      val currentDist = calcProgress(initialState, currentState)
      val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000
      val intervalSeconds = interval.toSeconds.max(1)
      val reportRate = delta / intervalSeconds
      val avgRate = if (elapsedSeconds > 0) currentDist / elapsedSeconds else 0L
      val percentage = if (totalDist > 0) s"${100 * currentDist / totalDist}%" else "N/A"
      val minutesLeft =
        if (avgRate > 0 && totalDist > 0) s"${(totalDist - currentDist) / avgRate / 60}"
        else "N/A"
      logger.info(
        s"$action current: $currentState, target: $targetState $currentDist/$totalDist events processed, $percentage, (since last: $delta, $reportRate events/s) (avg: $avgRate events/s, estimated minutes left: $minutesLeft)"
      )
    }(materializer.executionContext)
  }

  private def achsMaintenancePipeSource(
      initialWork: AchsWorkDistance,
      dbDispatcher: DbDispatcher,
      achsConfig: AchsConfig,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContextWithTrace,
  ): Source[AchsWorkRange, NotUsed] = {
    // Split initialWork into two phases:
    //   Phase 1 (Removal): removes deactivated entries and bumps validAt, no population.
    //   Phase 2 (Copy): copies over activations using the updated validAt from Phase 1.
    val removalOnlyWork = AchsWorkDistance(populate = 0, remove = initialWork.remove)
    val copyOnlyWork = AchsWorkDistance(populate = initialWork.populate, remove = 0)
    Source(List(removalOnlyWork, copyOnlyWork))
      .via(
        AchsMaintenancePipe(
          parameterStorageBackend = parameterStorageBackend,
          eventStorageBackend = eventStorageBackend,
          dbDispatcher = dbDispatcher,
          achsStateCache = achsStateCache,
          toAchsWorkDistance = identity[AchsWorkDistance],
          initialWork = AchsWorkDistance(populate = 0, remove = 0),
          populationParallelism = achsConfig.initParallelism.unwrap,
          removalParallelism = achsConfig.initParallelism.unwrap,
          aggregationThreshold = achsConfig.initAggregationThreshold,
          metrics = metrics,
          executionContext = ec,
          logger = logger,
          fullDrain = true,
        )
      )
  }
}
