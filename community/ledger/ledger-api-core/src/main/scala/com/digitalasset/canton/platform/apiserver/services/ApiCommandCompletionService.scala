// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.command_completion_service.*
import com.daml.logging.entries.LoggingEntries
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.validation.CompletionServiceRequestValidator
import com.digitalasset.canton.ledger.api.validation.CompletionServiceRequestValidator.GetCompletionsStreamRequest
import com.digitalasset.canton.ledger.participant.state.index.IndexCompletionsService
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class ApiCommandCompletionService(
    completionsService: IndexCompletionsService,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
) extends StreamingServiceLifecycleManagement
    with NamedLogging {

  def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(TraceContextGrpc.fromGrpcContext)
    registerStream(responseObserver) {
      implicit val errorLoggingContext: ErrorLoggingContext = ErrorLoggingContext(
        logger,
        loggingContextWithTrace.toPropertiesMap,
        loggingContextWithTrace.traceContext,
      )
      logger.debug(s"Received new completion request $request.")
      Source.single(completionsService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        CompletionServiceRequestValidator
          .validateGrpcCompletionStreamRequest(request)
          .flatMap(
            CompletionServiceRequestValidator
              .validateCompletionStreamRequest(_, ledgerEnd.map(_.lastOffset))
          )
          .fold(
            t =>
              Source.failed[CompletionStreamResponse](
                ValidationLogger.logFailureWithTrace(logger, request, t)
              ),
            request => {
              withEnrichedLoggingContext(
                logging.userId(request.userId),
                logging.partyStrings(request.parties),
                logging.offset(request.offset.fold(0L)(_.unwrap)),
              ) { implicit loggingContext =>
                logger.info(
                  s"Received request for completion subscription, ${loggingContext
                      .serializeFiltered("userId", "parties", "offset")}."
                )(loggingContext.traceContext)
              }
              streamCompletions(
                userId = Some(request.userId),
                parties = request.parties,
                offset = request.offset,
              )
            },
          )
      }
    }
  }

  /** Subscribe to command completion events. This streaming endpoint provides more flexibility in
    * filtering than the predecessor ``CompletionStream``.
    */
  def getCompletions(
      request: GetCompletionsRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(TraceContextGrpc.fromGrpcContext)
    registerStream(responseObserver) {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext(logger, loggingContextWithTrace)
      logger.debug(s"Received new GetCompletions request $request.")
      Source
        .single(completionsService.currentLedgerEnd())
        .flatMapConcat { ledgerEnd =>
          CompletionServiceRequestValidator
            .validateGetCompletionsRequest(request, ledgerEnd.map(_.lastOffset))
            .fold(
              t =>
                Source.failed[CompletionStreamResponse](
                  ValidationLogger.logFailureWithTrace(logger, request, t)
                ),
              { case GetCompletionsStreamRequest(parties, offset) =>
                withEnrichedLoggingContext(
                  logging.parties(parties),
                  logging.offset(offset.fold(0L)(_.unwrap)),
                ) { implicit loggingContext =>
                  logger.info(
                    s"Received request for completion subscription, ${loggingContext
                        .serializeFiltered("parties", "offset")}."
                  )(loggingContext.traceContext)
                }
                streamCompletions(
                  userId = None,
                  parties = parties,
                  offset = offset,
                )
              },
            )
        }
    }
  }

  private def streamCompletions(
      userId: Option[Ref.UserId],
      parties: Set[Ref.Party],
      offset: Option[Offset],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Source[CompletionStreamResponse, ?] =
    completionsService
      .getCompletions(
        offset,
        userId,
        parties,
      )
      .via(
        logger.enrichedDebugStream(
          "Responding with completions.",
          response =>
            response.completionResponse.completion match {
              case Some(completion) =>
                LoggingEntries(
                  "commandId" -> completion.commandId,
                  "statusCode" -> completion.status.map(_.code),
                )
              case None =>
                LoggingEntries()
            },
        )
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.lapi.streams.completions))

  /** Look up a completion by its transaction hash. This is only available for completions received
    * after upgrading to Canton version 3.5. If:
    *
    *   - there is no completion with this transaction hash,
    *   - or the completion is not visible to the user,
    *   - or the completion was populated before upgrading to Canton version 3.5,
    *   - or respective completions are all pruned,
    *
    * a COMPLETION_NOT_FOUND error will be raised.
    */
  def getCompletionByHash(
      req: ApiCommandCompletionService.InternalGetCompletionByHashRequest
  ): Future[GetCompletionByHashResponse] = {
    val parties = req.parties.map(Ref.Party.assertFromString)
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)(TraceContextGrpc.fromGrpcContext)
    implicit val errorLoggingContext: ErrorLoggingContext = ErrorLoggingContext(
      logger,
      loggingContextWithTrace.toPropertiesMap,
      loggingContextWithTrace.traceContext,
    )
    CompletionServiceRequestValidator
      .validateCompletionByHash(req.transactionHash)
      .fold(
        t =>
          Future.failed(
            ValidationLogger.logFailureWithTrace(logger, req.transactionHash, t)
          ),
        validHash => {
          logger.info(s"Received request for completion by hash.")
          completionsService
            .getCompletionByHash(validHash, parties)
            .thereafter(
              logger.logErrorsOnCall[GetCompletionByHashResponse](
                loggingContextWithTrace.traceContext
              )
            )
        },
      )
  }
}

object ApiCommandCompletionService {

  /** Internal request for the by-hash lookup. `parties` is backfilled by the authorization layer
    * from the caller's claims: empty means "no filter" (ReadAsAnyParty semantics), non-empty means
    * "restrict to these parties".
    */
  final case class InternalGetCompletionByHashRequest(
      transactionHash: ByteString,
      parties: Set[String],
  )

  object InternalGetCompletionByHashRequest {
    val partiesLens: Lens[InternalGetCompletionByHashRequest, Set[String]] =
      Lens[InternalGetCompletionByHashRequest, Set[String]](_.parties)((q, p) =>
        q.copy(parties = p)
      )
  }
}
