// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.error.definitions.LedgerApiErrors.RequestValidation.NonHexOffset
import com.daml.error.{BaseError, ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PruningServiceErrorGroup
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.sync.{CantonSyncService, UpstreamOffsetConvert}
import com.digitalasset.canton.pruning.admin.v0
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule, PruningScheduler}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcPruningService(
    sync: CantonSyncService,
    scheduleAccessorBuilder: () => Option[PruningScheduler],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends PruningServiceGrpc.PruningService
    with NamedLogging {

  override def prune(request: PruneRequest): Future[PruneResponse] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val eithert = for {
        ledgerSyncOffset <- EitherT.fromEither[Future](
          UpstreamOffsetConvert
            .toLedgerSyncOffset(request.pruneUpTo)
            .leftMap(err => NonHexOffset.Error("prune_up_to", request.pruneUpTo, err))
        )
        _ <- sync.pruneInternally(ledgerSyncOffset).leftWiden[BaseError]
      } yield PruneResponse()

      EitherTUtil.toFuture(eithert.leftMap(err => err.code.asGrpcError(err)))
    }

  private lazy val maybeScheduleAccessor: Option[PruningScheduler] =
    scheduleAccessorBuilder()

  private def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler] =
    maybeScheduleAccessor match {
      case None =>
        Future.failed(
          PruningServiceError.PruningNotSupportedInCommunityEdition.Error().asGrpcError
        )
      case Some(scheduler) => Future.successful(scheduler)
    }

  override def setSchedule(request: v0.SetSchedule.Request): Future[v0.SetSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      schedule <- ensureValidO("schedule", request.schedule, PruningSchedule.fromProtoV0)
      _scheduleSuccessfullySet <- scheduler.setScheduleWithRetention(schedule)
    } yield v0.SetSchedule.Response()
  }

  override def clearSchedule(
      request: v0.ClearSchedule.Request
  ): Future[v0.ClearSchedule.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      _ <- scheduler.clearSchedule()
    } yield v0.ClearSchedule.Response()
  }

  override def updateCron(request: v0.UpdateCron.Request): Future[v0.UpdateCron.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      cron <- ensureValid(Cron.fromProtoPrimitive(request.cron))
      _cronSuccessfullySet <- handleUserError(scheduler.updateCron(cron))
    } yield v0.UpdateCron.Response()
  }

  override def updateMaxDuration(
      request: v0.UpdateMaxDuration.Request
  ): Future[v0.UpdateMaxDuration.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- ensureValid(
        PositiveSeconds
          .fromProtoPrimitiveO("max_duration")(request.maxDuration)
      )
      _maxDurationSuccessfullySet <- handleUserError(scheduler.updateMaxDuration(positiveDuration))
    } yield v0.UpdateMaxDuration.Response()
  }

  override def updateRetention(
      request: v0.UpdateRetention.Request
  ): Future[v0.UpdateRetention.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      scheduler <- ensureScheduler
      positiveDuration <- ensureValid(
        PositiveSeconds
          .fromProtoPrimitiveO("retention")(request.retention)
      )
      _retentionSuccessfullySet <- handleUserError(scheduler.updateRetention(positiveDuration))
    } yield v0.UpdateRetention.Response()
  }

  override def getSchedule(
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

sealed trait PruningServiceError extends CantonError
object PruningServiceError extends PruningServiceErrorGroup {

  @Explanation("""The supplied offset has an unexpected lengths.""")
  @Resolution(
    "Ensure the offset has originated from this participant and is 9 bytes in length."
  )
  object NonCantonOffset
      extends ErrorCode(id = "NON_CANTON_OFFSET", ErrorCategory.InvalidIndependentOfSystemState) {
    case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Offset length does not match ledger standard of 9 bytes"
        )
        with PruningServiceError
  }

  @Explanation("""Pruning is not supported in the Community Edition.""")
  @Resolution("Upgrade to the Enterprise Edition.")
  object PruningNotSupportedInCommunityEdition
      extends ErrorCode(
        id = "PRUNING_NOT_SUPPORTED_IN_COMMUNITY_EDITION",
        // TODO(#5990) According to the WriteParticipantPruningService, this should give the status code UNIMPLEMENTED. Introduce a new error category for that!
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Pruning is only supported in the Enterprise Edition"
        )
        with PruningServiceError
  }

  @Explanation(
    """Pruning is not possible at the specified offset at the current time."""
  )
  @Resolution(
    """Specify a lower offset or retry pruning after a while. Generally, you can only prune
       older events. In particular, the events must be older than the sum of mediator reaction timeout
       and participant timeout for every domain. And, you can only prune events that are older than the
       deduplication time configured for this participant.
       Therefore, if you observe this error, you either just prune older events or you adjust the settings
       for this participant.
       The error details field `safe_offset` contains the highest offset that can currently be pruned, if any.
      """
  )
  object UnsafeToPrune
      extends ErrorCode(id = "UNSAFE_TO_PRUNE", ErrorCategory.InvalidGivenCurrentSystemStateOther) {
    case class Error(_cause: String, reason: String, safe_offset: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Participant cannot prune at specified offset due to ${_cause}"
        )
        with PruningServiceError
  }

  @Explanation("""Pruning has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
      extends ErrorCode(
        id = "INTERNAL_PRUNING_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Internal error such as the inability to write to the database"
        )
        with PruningServiceError
  }

}
