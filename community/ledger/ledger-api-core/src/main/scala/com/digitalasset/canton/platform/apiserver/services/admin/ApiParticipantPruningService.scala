// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
  PruneResponse,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.{Metrics, Tracked}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.*
import com.digitalasset.canton.ledger.error.{DamlContextualizedErrorLogger, LedgerApiErrors}
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  IndexParticipantPruningService,
  LedgerEndService,
}
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.platform.ApiOffset.ApiOffsetConverter
import com.digitalasset.canton.platform.api.grpc.GrpcApiService
import com.digitalasset.canton.platform.apiserver.services.logging
import com.digitalasset.canton.platform.server.api.services.grpc.Logging.traceId
import com.digitalasset.canton.platform.server.api.{ApiException, ValidationLogger}
import io.grpc.protobuf.StatusProto
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

final class ApiParticipantPruningService private (
    readBackend: IndexParticipantPruningService with LedgerEndService,
    writeBackend: state.WriteParticipantPruningService,
    metrics: Metrics,
    telemetry: Telemetry,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends ParticipantPruningServiceGrpc.ParticipantPruningService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def bindService(): ServerServiceDefinition =
    ParticipantPruningServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def prune(request: PruneRequest): Future[PruneResponse] =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        val submissionIdOrErr = Ref.SubmissionId
          .fromString(
            if (request.submissionId.nonEmpty) request.submissionId else UUID.randomUUID().toString
          )
          .left
          .map(err =>
            invalidArgument(s"submission_id $err")(
              contextualizedErrorLogger(request.submissionId)
            )
          )

        submissionIdOrErr.fold(
          t => Future.failed(ValidationLogger.logFailure(request, t)),
          submissionId =>
            LoggingContext.withEnrichedLoggingContext(logging.submissionId(submissionId)) {
              implicit loggingContext =>
                logger.info(s"Pruning up to ${request.pruneUpTo}")
                (for {

                  pruneUpTo <- validateRequest(request)(
                    loggingContext,
                    contextualizedErrorLogger(submissionId),
                  )

                  // If write service pruning succeeds but ledger api server index pruning fails, the user can bring the
                  // systems back in sync by reissuing the prune request at the currently specified or later offset.
                  _ <- Tracked.future(
                    metrics.daml.services.pruning.pruneCommandStarted,
                    metrics.daml.services.pruning.pruneCommandCompleted,
                    pruneWriteService(pruneUpTo, submissionId, request.pruneAllDivulgedContracts),
                  )(MetricsContext(("phase", "underlyingLedger")))

                  pruneResponse <- Tracked.future(
                    metrics.daml.services.pruning.pruneCommandStarted,
                    metrics.daml.services.pruning.pruneCommandCompleted,
                    pruneLedgerApiServerIndex(
                      pruneUpTo,
                      request.pruneAllDivulgedContracts,
                    ),
                  )(MetricsContext(("phase", "ledgerApiServerIndex")))

                } yield pruneResponse).andThen(logger.logErrorsOnCall[PruneResponse])
            },
        )
    }

  private def validateRequest(
      request: PruneRequest
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Offset] = {
    (for {
      pruneUpToString <- checkOffsetIsSpecified(request.pruneUpTo)
      pruneUpTo <- checkOffsetIsHexadecimal(pruneUpToString)
    } yield (pruneUpTo, pruneUpToString))
      .fold(
        t => Future.failed(ValidationLogger.logFailure(request, t)),
        o => checkOffsetIsBeforeLedgerEnd(o._1, o._2),
      )
  }

  private def pruneWriteService(
      pruneUpTo: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    import state.PruningResult.*
    logger.info(
      s"About to prune participant ledger up to ${pruneUpTo.toApiString} inclusively starting with the write service"
    )
    writeBackend
      .prune(pruneUpTo, submissionId, pruneAllDivulgedContracts)
      .asScala
      .flatMap {
        case NotPruned(status) =>
          Future.failed(new ApiException(StatusProto.toStatusRuntimeException(status)))
        case ParticipantPruned =>
          logger.info(s"Pruned participant ledger up to ${pruneUpTo.toApiString} inclusively.")
          Future.successful(())
      }
  }

  private def pruneLedgerApiServerIndex(
      pruneUpTo: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[PruneResponse] = {
    logger.info(s"About to prune ledger api server index to ${pruneUpTo.toApiString} inclusively")
    readBackend
      .prune(pruneUpTo, pruneAllDivulgedContracts)
      .map { _ =>
        logger.info(s"Pruned ledger api server index up to ${pruneUpTo.toApiString} inclusively.")
        PruneResponse()
      }
  }

  private def checkOffsetIsSpecified(
      offset: String
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, String] =
    Either.cond(
      offset.nonEmpty,
      offset,
      invalidArgument("prune_up_to not specified"),
    )

  private def checkOffsetIsHexadecimal(
      pruneUpToString: String
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, Offset] =
    ApiOffset
      .tryFromString(pruneUpToString)
      .toEither
      .left
      .map(t =>
        LedgerApiErrors.RequestValidation.NonHexOffset
          .Error(
            fieldName = "prune_up_to",
            offsetValue = pruneUpToString,
            message =
              s"prune_up_to needs to be a hexadecimal string and not $pruneUpToString: ${t.getMessage}",
          )
          .asGrpcError
      )

  private def checkOffsetIsBeforeLedgerEnd(
      pruneUpToProto: Offset,
      pruneUpToString: String,
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[Offset] =
    for {
      ledgerEnd <- readBackend.currentLedgerEnd()
      _ <-
        // NOTE: This constraint should be relaxed to (pruneUpToString <= ledgerEnd.value)
        if (pruneUpToString < ledgerEnd.value) Future.successful(())
        else
          Future.failed(
            LedgerApiErrors.RequestValidation.OffsetOutOfRange
              .Reject(
                s"prune_up_to needs to be before ledger end ${ledgerEnd.value}"
              )
              .asGrpcError
          )
    } yield pruneUpToProto

  private def contextualizedErrorLogger(submissionId: String)(implicit
      loggingContext: LoggingContext
  ): ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))
}

object ApiParticipantPruningService {
  def createApiService(
      readBackend: IndexParticipantPruningService with LedgerEndService,
      writeBackend: state.WriteParticipantPruningService,
      metrics: Metrics,
      telemetry: Telemetry,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ParticipantPruningServiceGrpc.ParticipantPruningService with GrpcApiService =
    new ApiParticipantPruningService(readBackend, writeBackend, metrics, telemetry)

}
