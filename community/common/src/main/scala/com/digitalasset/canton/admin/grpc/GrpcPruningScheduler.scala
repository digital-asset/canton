// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.pruning.v30
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule, PruningScheduler}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

trait GrpcPruningScheduler {
  this: HasPruningScheduler & NamedLogging =>

  protected def ec: ExecutionContext
  @inline private implicit def ecInternal: ExecutionContext = ec

  def setSchedule(request: v30.SetScheduleRequest): Future[v30.SetScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      schedule <- convertRequiredF("schedule", request.schedule, PruningSchedule.fromProtoV30)
      _scheduleSuccessfullySet <- scheduler.setSchedule(schedule)
    } yield v30.SetScheduleResponse()
    fus.asGrpcFuture
  }

  def clearSchedule(
      @unused
      request: v30.ClearScheduleRequest
  ): Future[v30.ClearScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      _ <- scheduler.clearSchedule()
    } yield v30.ClearScheduleResponse()
    fus.asGrpcFuture
  }

  def setCron(request: v30.SetCronRequest): Future[v30.SetCronResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      cron <- convertFUS(Cron.fromProtoPrimitive(request.cron))
      _cronSuccessfullySet <- handleUserError(scheduler.updateCron(cron))
    } yield v30.SetCronResponse()
    fus.asGrpcFuture
  }

  def setMaxDuration(
      request: v30.SetMaxDurationRequest
  ): Future[v30.SetMaxDurationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      positiveDuration <- convertFUS(
        PositiveSeconds.fromProtoPrimitiveO("max_duration")(request.maxDuration)
      )
      _maxDurationSuccessfullySet <- handleUserError(scheduler.updateMaxDuration(positiveDuration))
    } yield v30.SetMaxDurationResponse()
    fus.asGrpcFuture
  }

  def setRetention(
      request: v30.SetRetentionRequest
  ): Future[v30.SetRetentionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      positiveDuration <- convertFUS(
        PositiveSeconds.fromProtoPrimitiveO("retention")(request.retention)
      )
      _retentionSuccessfullySet <- handleUserError(scheduler.updateRetention(positiveDuration))
    } yield v30.SetRetentionResponse()
    fus.asGrpcFuture
  }

  def getSchedule(
      @unused
      request: v30.GetScheduleRequest
  ): Future[v30.GetScheduleResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val fus = for {
      scheduler <- ensureScheduler
      scheduleWithRetention <- scheduler.getSchedule()
    } yield v30.GetScheduleResponse(scheduleWithRetention.map(_.toProtoV30))
    fus.asGrpcFuture
  }

  protected def convertF[T](f: ProtoConverter.ParsingResult[T])(implicit
      traceContext: TraceContext
  ): Future[T] = f
    .leftMap(err => ProtoDeserializationFailure.Wrap(err).asGrpcError)
    .fold(Future.failed, Future.successful)

  protected def convertFUS[T](f: ProtoConverter.ParsingResult[T])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[T] = f
    .leftMap(err => ProtoDeserializationFailure.Wrap(err).asGrpcError)
    .fold(FutureUnlessShutdown.failed, FutureUnlessShutdown.pure)

  protected def convertRequiredF[P, T](
      field: String,
      value: Option[P],
      f: P => ProtoConverter.ParsingResult[T],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[T] = convertFUS(
    ProtoConverter.required(field, value).flatMap(f)
  )

  protected def handleUserError(
      update: EitherT[FutureUnlessShutdown, String, Unit]
  ): FutureUnlessShutdown[Unit] =
    EitherTUtil.toFutureUnlessShutdown(
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
  ): FutureUnlessShutdown[PruningScheduler]
}
