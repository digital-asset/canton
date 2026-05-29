// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.scheduler.ParticipantPurgeStoresAfterLsuScheduler.GetPurgeableStores
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.LsuSource
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.scheduler.{IndividualSchedule, JobSchedule, JobScheduler}
import com.digitalasset.canton.store.ChunkPurgeable
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

final class ParticipantPurgeStoresAfterLsuScheduler(
    schedule: Option[JobSchedule],
    getPurgeableStores: GetPurgeableStores,
    chunkSize: PositiveInt,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextIdlenessExecutorService
) extends JobScheduler("post_lsu_gc", timeouts, loggerFactory) {

  override protected def schedulerJob(
      schedule: IndividualSchedule
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] = {
    val purgeableStores = getPurgeableStores()
    val deletions = purgeableStores.map(_.deleteDataChunk(chunkSize))

    for {
      deletedSomething <- FutureUnlessShutdown.sequence(deletions)
    } yield {
      if (deletedSomething.contains(true)) JobScheduler.MoreWorkToPerform else JobScheduler.Done
    }
  }

  override protected def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] = Future(schedule)
}

object ParticipantPurgeStoresAfterLsuScheduler {
  trait GetPurgeableStores {
    def apply(): Seq[ChunkPurgeable]
  }

  def create(
      schedule: Option[JobSchedule],
      chunkSize: PositiveInt,
      synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
      syncPersistentStateManager: SyncPersistentStateManager,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService
  ): ParticipantPurgeStoresAfterLsuScheduler = {

    val getPurgeableStores: GetPurgeableStores = () => {
      val purgeableSynchronizers = synchronizerConnectionConfigStore
        .getAll()
        // keep only synchronizer that were LSUed
        .filter(_.status == LsuSource)
        .flatMap(_.configuredPsid.toOption)
        // and that have an active physical connection
        .filter(psid => synchronizerConnectionConfigStore.getActive(psid.logical).isRight)

      purgeableSynchronizers.flatMap { psid =>
        syncPersistentStateManager
          .get(psid)
          .toList
          .flatMap(_.purgeableStores)
      }
    }

    new ParticipantPurgeStoresAfterLsuScheduler(
      schedule,
      getPurgeableStores,
      chunkSize,
      timeouts,
      loggerFactory,
    )
  }
}
