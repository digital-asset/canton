// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.scheduler.ParticipantPurgeObsoleteTopologyScheduler.GetObsoleteTopologyStores
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.LsuSource
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.scheduler.{IndividualSchedule, JobSchedule, JobScheduler}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

final class ParticipantPurgeObsoleteTopologyScheduler(
    schedule: Option[JobSchedule],
    getObsoleteTopologyStores: GetObsoleteTopologyStores,
    chunkSize: PositiveInt,
    timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextIdlenessExecutorService
) extends JobScheduler("topology_gc", timeouts, loggerFactory) {

  override protected def schedulerJob(
      schedule: IndividualSchedule
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[JobScheduler.ScheduledRunResult] =
    for {
      obsoleteTopologyStores <- getObsoleteTopologyStores()
      deletions = obsoleteTopologyStores.map(_.deleteDataChunk(chunkSize))
      deletedSomething <- FutureUnlessShutdown.sequence(deletions)
    } yield {
      if (deletedSomething.contains(true)) JobScheduler.MoreWorkToPerform else JobScheduler.Done
    }

  override protected def initializeSchedule()(implicit
      traceContext: TraceContext
  ): Future[Option[JobSchedule]] = Future(schedule)
}

object ParticipantPurgeObsoleteTopologyScheduler {
  trait GetObsoleteTopologyStores {
    def apply(): FutureUnlessShutdown[Seq[TopologyStore[TopologyStoreId.SynchronizerStore]]]
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
  ) = {
    val getObsoleteTopologyStores: GetObsoleteTopologyStores = () =>
      FutureUnlessShutdown.pure {
        synchronizerConnectionConfigStore
          .getAll()
          .filter(_.status == LsuSource)
          .flatMap(_.configuredPsid.toOption)
          .filter(psid => synchronizerConnectionConfigStore.getActive(psid.logical).isRight)
          .flatMap(syncPersistentStateManager.get)
          .map(_.topologyStore)
      }

    new ParticipantPurgeObsoleteTopologyScheduler(
      schedule,
      getObsoleteTopologyStores,
      chunkSize,
      timeouts,
      loggerFactory,
    )
  }
}
