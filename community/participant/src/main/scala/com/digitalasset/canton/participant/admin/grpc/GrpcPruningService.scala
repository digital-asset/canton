// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.error.definitions.LedgerApiErrors.RequestValidation.NonHexOffset
import com.daml.error.{BaseError, ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.admin.grpc.{GrpcPruningScheduler, HasPruningScheduler}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PruningServiceErrorGroup
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.sync.{CantonSyncService, UpstreamOffsetConvert}
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

class GrpcPruningService(
    sync: CantonSyncService,
    scheduleAccessorBuilder: () => Option[PruningScheduler],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    val ec: ExecutionContext
) extends PruningServiceGrpc.PruningService
    with HasPruningScheduler
    with GrpcPruningScheduler
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

  override protected def ensureScheduler(implicit
      traceContext: TraceContext
  ): Future[PruningScheduler] =
    maybeScheduleAccessor match {
      case None =>
        Future.failed(
          PruningServiceError.PruningNotSupportedInCommunityEdition.Error().asGrpcError
        )
      case Some(scheduler) => Future.successful(scheduler)
    }

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
