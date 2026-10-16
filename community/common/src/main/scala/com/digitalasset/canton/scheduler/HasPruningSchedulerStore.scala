// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.store.PruningSchedulerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait HasPruningSchedulerStore extends PruningScheduler with FlagCloseable {
  this: ScheduleRefresher =>

  implicit val ec: ExecutionContext

  protected def pruningSchedulerStore: PruningSchedulerStore

  override def clearSchedule()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    updateScheduleAndReactivateIfActive(pruningSchedulerStore.clearSchedule())

  override def updateCron(
      cron: Cron
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _updated <- pruningSchedulerStore.updateCron(cron)
      _reactivated <- reactivateSchedulerIfActiveET()
    } yield ()

  override def updateMaxDuration(maxDuration: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      _updated <- pruningSchedulerStore.updateMaxDuration(maxDuration)
      _reactivated <- reactivateSchedulerIfActiveET()
    } yield ()

  override def updateRetention(retention: PositiveSeconds)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _updated <- pruningSchedulerStore.updateRetention(retention)
    _reactivated <- reactivateSchedulerIfActiveET()
  } yield ()

  override def setSchedule(schedule: PruningSchedule)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = updateScheduleAndReactivateIfActive(
    pruningSchedulerStore.setSchedule(schedule)
  )

  override def getSchedule()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningSchedule]] =
    pruningSchedulerStore.getSchedule()

  override def onClosed(): Unit = pruningSchedulerStore.close()
}
