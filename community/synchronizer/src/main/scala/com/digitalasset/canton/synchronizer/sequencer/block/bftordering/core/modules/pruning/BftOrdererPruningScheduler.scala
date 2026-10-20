// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.scheduler.{HasPruningSchedulerStore, ScheduleRefresher}
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class BftOrdererPruningScheduler(
    store: BftOrdererPruningSchedulerStore[PekkoEnv],
    pruningModuleRef: ModuleRef[Pruning.Message],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit
    val ec: ExecutionContext,
    metricsContext: MetricsContext,
) extends HasPruningSchedulerStore
    with ScheduleRefresher
    with NamedLogging {

  override protected def pruningSchedulerStore: PruningSchedulerStore = store

  override def start()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    for {
      scheduleO <- store.getBftOrdererSchedule().futureUnlessShutdown()
    } yield scheduleO.foreach(schedule =>
      pruningModuleRef.asyncSend(Pruning.StartPruningSchedule(schedule))
    )

  override def stop()(implicit traceContext: TraceContext): Unit =
    pruningModuleRef.asyncSend(Pruning.CancelPruningSchedule)

  override def reactivateSchedulerIfActive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    stop()
    start()
  }

  def setBftOrdererSchedule(schedule: BftOrdererPruningSchedule)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = updateScheduleAndReactivateIfActive(
    store.setBftOrdererSchedule(schedule).futureUnlessShutdown()
  )

  def getBftOrdererSchedule()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[BftOrdererPruningSchedule]] =
    store.getBftOrdererSchedule().futureUnlessShutdown()

  def updateMinBlocksToKeep(
      minBlocksToKeep: Int
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _updated <- store.updateMinBlocksToKeep(minBlocksToKeep)
      _reactivated <- reactivateSchedulerIfActiveET()
    } yield ()
}
