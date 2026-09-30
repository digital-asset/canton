// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.scheduler.ParticipantPurgeStoresAfterLsuScheduler.ComputedPurgeableStores
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.scheduler.{IndividualSchedule, JobSchedule, JobScheduler}
import com.digitalasset.canton.store.ChunkPurgeable
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** @param schedule
  *   Schedule for the job
  * @param purgeableStoresComputation
  *   Class that computes the list of purgeable stores
  * @param chunkSize
  *   Chunk size for the purge operation.
  * @param recomputePurgeableStoresAfter
  *   Reuse the computed purgeable stores for recomputePurgeableStoresAfter iterations (because the
  *   list does not change often)
  */
final class ParticipantPurgeStoresAfterLsuScheduler(
    schedule: Option[JobSchedule],
    purgeableStoresComputation: PostLsuPurgeableStoresComputation,
    chunkSize: PositiveInt,
    recomputePurgeableStoresAfter: PositiveInt,
    batchingConfig: BatchingConfig,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextIdlenessExecutorService
) extends JobScheduler("post_lsu_gc", timeouts, loggerFactory) {

  private val purgeableStoresO = new AtomicReference[Option[ComputedPurgeableStores]](None)

  override protected def schedulerJob(
      schedule: IndividualSchedule
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] = {

    /*
      Empty iff the purgeable stores need to be recomputed.
      Recomputation happen after reuseComputedPurgeableStoresCount calls or if computedPurgeableStores is empty
     */
    val computedPurgeableStores: Option[ComputedPurgeableStores] =
      purgeableStoresO.get().filter { computedPurgeableStores =>
        computedPurgeableStores.used < recomputePurgeableStoresAfter && computedPurgeableStores.stores.nonEmpty
      }

    for {
      purgeableStores <- computedPurgeableStores match {
        case Some(value) =>
          logger.debug(s"Reusing purgeable stores: ${value.stores.map(_.name)}")
          FutureUnlessShutdown.pure(
            value.copy(used = checked(value.used.tryIncrement.toNonNegative))
          )
        case None =>
          purgeableStoresComputation.compute().map { purgeableStores =>
            logger.debug(s"Purgeable stores: ${purgeableStores.map(_.name)}")
            ComputedPurgeableStores(NonNegativeInt.one, purgeableStores)
          }
      }

      // We accept potential races here because the goal is to limit the number of recomputations,
      // but it is a heuristic.
      _ = purgeableStoresO.set(Some(purgeableStores))
      deletedSomething <- MonadUtil.parTraverseWithLimit(batchingConfig.pruningParallelism)(
        purgeableStores.stores
      )(_.deleteDataChunk(chunkSize))
    } yield {
      if (deletedSomething.contains(true)) JobScheduler.MoreWorkToPerform else JobScheduler.Done
    }
  }

  override protected def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] = Future(schedule)
}

object ParticipantPurgeStoresAfterLsuScheduler {
  val recomputePurgeableStoresAfterDefault: PositiveInt = PositiveInt.tryCreate(100)
  private final case class ComputedPurgeableStores(
      used: NonNegativeInt,
      stores: Seq[ChunkPurgeable],
  )

  def create(
      schedule: Option[JobSchedule],
      chunkSize: PositiveInt,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      syncPersistentStateManager: SyncPersistentStateManager,
      batchingConfig: BatchingConfig,
      acsDigestProcessorEnabled: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService
  ): ParticipantPurgeStoresAfterLsuScheduler = {

    val purgeableStoresComputation = new PostLsuPurgeableStoresComputation(
      synchronizerConnectionConfigStore,
      syncPersistentStateManager,
      acsDigestProcessorEnabled,
      loggerFactory,
    )

    new ParticipantPurgeStoresAfterLsuScheduler(
      schedule,
      purgeableStoresComputation,
      chunkSize = chunkSize,
      recomputePurgeableStoresAfter = recomputePurgeableStoresAfterDefault,
      batchingConfig,
      timeouts,
      loggerFactory,
    )
  }
}
