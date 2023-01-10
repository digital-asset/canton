// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.grpc

import cats.data.EitherT
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule, PruningScheduler}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

trait GrpcPruningScheduler {
  this: HasPruningScheduler =>

  def setSchedule(request: v0.SetSchedule.Request): Future[v0.SetSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      schedule <- ensureValidO("schedule", request.schedule, PruningSchedule.fromProtoV0)
      _scheduleSuccessfullySet <- scheduler.setScheduleWithRetention(schedule)
    } yield v0.SetSchedule.Response()
  }

  def clearSchedule(
      request: v0.ClearSchedule.Request
  ): Future[v0.ClearSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      _ <- scheduler.clearSchedule()
    } yield v0.ClearSchedule.Response()
  }

  def setCron(request: v0.SetCron.Request): Future[v0.SetCron.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      cron <- ensureValid(Cron.fromProtoPrimitive(request.cron))
      _cronSuccessfullySet <- handleUserError(scheduler.updateCron(cron))
    } yield v0.SetCron.Response()
  }

  def setMaxDuration(
      request: v0.SetMaxDuration.Request
  ): Future[v0.SetMaxDuration.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- ensureValid(
        PositiveSeconds
          .fromProtoPrimitiveO("max_duration")(request.maxDuration)
      )
      _maxDurationSuccessfullySet <- handleUserError(scheduler.updateMaxDuration(positiveDuration))
    } yield v0.SetMaxDuration.Response()
  }

  def setRetention(
      request: v0.SetRetention.Request
  ): Future[v0.SetRetention.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- ensureValid(
        PositiveSeconds
          .fromProtoPrimitiveO("retention")(request.retention)
      )
      _retentionSuccessfullySet <- handleUserError(scheduler.updateRetention(positiveDuration))
    } yield v0.SetRetention.Response()
  }

  def getSchedule(
      request: v0.GetSchedule.Request
  ): Future[v0.GetSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      scheduleWithRetention <- scheduler.getScheduleWithRetention()
    } yield v0.GetSchedule.Response(scheduleWithRetention.map(_.toProtoV0))
  }

  private def ensureValid[T](f: => ProtoConverter.ParsingResult[T]): Future[T] = f.fold(
    err =>
      Future.failed(
        Status.INVALID_ARGUMENT
          .withDescription(err.message)
          .asException()
      ),
    Future(_),
  )

  private def ensureValidO[P, T](
      field: String,
      value: Option[P],
      f: P => ProtoConverter.ParsingResult[T],
  ): Future[T] = (for {
    requiredValue <- ProtoConverter.required(field, value)
    convertedValue <- f(requiredValue)
  } yield convertedValue).fold(
    err =>
      Future.failed(
        Status.INVALID_ARGUMENT
          .withDescription(err.message)
          .asException()
      ),
    Future(_),
  )

  private def handleUserError(update: => EitherT[Future, String, Unit]): Future[Unit] =
    EitherTUtil.toFuture(
      update.leftMap(
        Status.INVALID_ARGUMENT
          .withDescription(_)
          .asRuntimeException()
      )
    )

}

trait HasPruningScheduler {
  protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler]

  implicit val ec: ExecutionContext
}
