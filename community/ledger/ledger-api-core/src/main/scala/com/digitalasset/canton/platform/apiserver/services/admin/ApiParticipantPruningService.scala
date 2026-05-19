// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v2.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.daml.metrics.Tracked
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.ParticipantOffsetValidator
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexParticipantPruningService,
  IndexUpdateService,
  LedgerEndService,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.platform.apiserver.ApiException
import com.digitalasset.canton.scheduler.SafeToPruneCommitmentState
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.util.Thereafter.syntax.*
import io.grpc.protobuf.StatusProto
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    syncService: SyncService,
    metrics: LedgerApiServerMetrics,
    safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService
    with NamedLogging {

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def prune(request: PruneRequest): Future[PruneResponse] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(TraceContextGrpc.fromGrpcContext)

    checkPruningIsNotInProgress { () =>
      logger.info(s"Pruning up to ${request.pruneUpTo}.")
      (for {
        pruneUpTo <- validateRequest(request)

        // If write service pruning succeeds but ledger api server index pruning fails, the user can bring the
        // systems back in sync by reissuing the prune request at the currently specified or later offset.
        _ = logger.debug("Pruning write service")
        _ <- Tracked.future(
          metrics.services.pruning.pruneCommandStarted,
          metrics.services.pruning.pruneCommandCompleted,
          pruneSyncService(pruneUpTo),
        )(MetricsContext(("phase", "underlyingLedger")))

        _ = logger.debug("Getting incomplete reassignments")
        getIncompleteReassignmentOffsets = (offset: Offset) =>
          syncService
            .incompleteReassignmentOffsets(
              validAt = offset,
              stakeholders = Set.empty, // getting all incomplete reassignments
            )
            .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
        previousPrunedOffset <- readBackend.indexDbPrunedUpto
        incompleteReassignmentOffsets <-
          getIncompleteReassignmentOffsets(pruneUpTo)
        previousIncompleteReassignmentOffsets <-
          previousPrunedOffset
            .map(getIncompleteReassignmentOffsets)
            .getOrElse(Future.successful(Vector.empty))

        _ = logger.debug("Pruning Ledger API Server")
        pruneResponse <- Tracked.future(
          metrics.services.pruning.pruneCommandStarted,
          metrics.services.pruning.pruneCommandCompleted,
          pruneLedgerApiServerIndex(
            previousPrunedOffset,
            previousIncompleteReassignmentOffsets,
            pruneUpTo,
            incompleteReassignmentOffsets,
          ),
        )(MetricsContext(("phase", "ledgerApiServerIndex")))

      } yield pruneResponse)
        .thereafter(logger.logErrorsOnCall[PruneResponse](loggingContext.traceContext))
    }
  }

  private def validateRequest(
      request: PruneRequest
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): Future[Offset] =
    (for {
      _ <- checkOffsetIsSpecified(request.pruneUpTo)
      pruneUpTo <- ParticipantOffsetValidator.validatePositive(request.pruneUpTo, "prune_up_to")
    } yield pruneUpTo)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        checkOffsetIsBeforeLedgerEnd,
      )

  private def pruneSyncService(
      pruneUpTo: Offset
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = {
    import state.PruningResult.*
    logger.info(
      s"About to prune participant ledger up to ${pruneUpTo.unwrap} inclusively starting with the write service."
    )
    syncService
      .prune(pruneUpTo, safeToPruneCommitmentState)
      .flatMap {
        case NotPruned(status) =>
          Future.failed(new ApiException(StatusProto.toStatusRuntimeException(status)))
        case ParticipantPruned =>
          logger.info(s"Pruned participant ledger up to ${pruneUpTo.unwrap} inclusively.")
          Future.unit
      }
  }

  private def pruneLedgerApiServerIndex(
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpTo: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[PruneResponse] = {
    logger.info(s"About to prune ledger api server index to ${pruneUpTo.unwrap} inclusively.")
    readBackend
      .prune(
        previousPruneUpToInclusive = previousPruneUpToInclusive,
        previousIncompleteReassignmentOffsets = previousIncompleteReassignmentOffsets,
        pruneUpToInclusive = pruneUpTo,
        incompleteReassignmentOffsets = incompleteReassignmentOffsets,
      )
      .map { _ =>
        logger.info(s"Pruned ledger api server index up to ${pruneUpTo.unwrap} inclusively.")
        PruneResponse()
      }
  }

  private def checkOffsetIsSpecified(
      offset: Long
  )(implicit errorLogger: ErrorLoggingContext): Either[StatusRuntimeException, Unit] =
    Either.cond(
      offset != 0,
      (),
      invalidArgument("prune_up_to not specified or zero"),
    )

  private def checkOffsetIsBeforeLedgerEnd(
      pruneUpTo: Offset
  )(implicit
      errorLogger: ErrorLoggingContext
  ): Future[Offset] =
    for {
      ledgerEnd <- readBackend.currentLedgerEnd()
      _ <-
        if (Option(pruneUpTo) < ledgerEnd) Future.unit
        else
          Future.failed(
            RequestValidationErrors.OffsetOutOfRange
              .Reject(
                s"prune_up_to needs to be before ledger end $ledgerEnd"
              )
              .asGrpcError
          )
    } yield pruneUpTo

  // Fast-path check to reject early if pruning is already in progress.
  // The actual serialization guard lives in JdbcLedgerDao.ensurePruningIsNotInProgress.
  private def checkPruningIsNotInProgress[T](f: () => Future[T])(implicit
      errorLogger: ErrorLoggingContext
  ): Future[T] =
    if (!readBackend.isPruningInProgress) {
      f()
    } else {
      Future.failed(
        RequestValidationErrors.ParticipantPruningInProgress.Reject().asGrpcError
      )
    }
}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService with IndexUpdateService,
      syncService: SyncService,
      metrics: LedgerApiServerMetrics,
      safeToPruneCommitmentState: Option[SafeToPruneCommitmentState],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(
      readBackend,
      syncService,
      metrics,
      safeToPruneCommitmentState,
      loggerFactory,
    )

}
