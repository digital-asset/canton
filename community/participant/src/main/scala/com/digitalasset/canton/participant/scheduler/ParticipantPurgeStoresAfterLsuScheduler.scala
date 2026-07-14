// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.scheduler.{IndividualSchedule, JobSchedule, JobScheduler}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.Future

final class ParticipantPurgeStoresAfterLsuScheduler(
    schedule: Option[JobSchedule],
    purgeableStoresComputation: PostLsuPurgeableStoresComputation,
    chunkSize: PositiveInt,
    batchingConfig: BatchingConfig,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextIdlenessExecutorService
) extends JobScheduler("post_lsu_gc", timeouts, loggerFactory) {

  override protected def schedulerJob(
      schedule: IndividualSchedule
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] = {
    val purgeableStores = purgeableStoresComputation.compute()
    logger.debug(s"Purgeable stores: ${purgeableStores.map(_.name)}")

    for {
      deletedSomething <- MonadUtil.parTraverseWithLimit(batchingConfig.pruningParallelism)(
        purgeableStores
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
  def create(
      schedule: Option[JobSchedule],
      chunkSize: PositiveInt,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      syncPersistentStateManager: SyncPersistentStateManager,
      batchingConfig: BatchingConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService
  ): ParticipantPurgeStoresAfterLsuScheduler = {

    val purgeableStoresComputation = new PostLsuPurgeableStoresComputation(
      synchronizerConnectionConfigStore,
      syncPersistentStateManager,
    )

    new ParticipantPurgeStoresAfterLsuScheduler(
      schedule,
      purgeableStoresComputation,
      chunkSize,
      batchingConfig,
      timeouts,
      loggerFactory,
    )
  }
}
