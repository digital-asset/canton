// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait ScheduleRefresher {

  implicit val ec: ExecutionContext

  def reactivateSchedulerIfActive()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  def updateScheduleAndReactivateIfActive(
      update: => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = for {
    _ <- update
    _ <- reactivateSchedulerIfActive()
  } yield ()

  def reactivateSchedulerIfActiveET()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT(
      reactivateSchedulerIfActive().map(_ => Either.unit[String])
    )

}
