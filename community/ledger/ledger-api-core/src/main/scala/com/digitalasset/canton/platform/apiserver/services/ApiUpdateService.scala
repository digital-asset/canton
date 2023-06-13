// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_service.GetTransactionsRequest
import com.daml.ledger.api.v2.update_service.*
import com.daml.lf.ledger.EventId
import com.daml.logging.LoggingContext
import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.Metrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.TransactionId
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.validation.{
  PartyNameChecker,
  TransactionServiceRequestValidator,
  ValidationErrors,
}
import com.digitalasset.canton.ledger.api.{ValidationLogger, domain}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexTransactionsService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import scalaz.syntax.tag.*

import scala.concurrent.{ExecutionContext, Future}

final class ApiUpdateService(
    transactionsService: IndexTransactionsService,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
    mat: Materializer,
) extends UpdateServiceGrpc.UpdateService
    with StreamingServiceLifecycleManagement
    with NamedLogging {

  import ApiConversions.*

  // TODO(#13269) remove the contextualizedErrorLogger
  protected val contextualizedErrorLogger: ContextualizedErrorLogger =
    errorLoggingContext(
      TraceContext.empty
    )

  private val validator =
    new TransactionServiceRequestValidator(
      domain.LedgerId(""), // not used
      PartyNameChecker.AllowAllParties,
    )

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit =
    registerStream(responseObserver) {
      implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new update request $request.")
      Source.future(transactionsService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        val validation = validator.validate(
          GetTransactionsRequest(
            ledgerId = "",
            begin = request.beginExclusive.map(toV1),
            end = request.endInclusive.map(toV1),
            filter = request.filter.map(v2TxFilter => TransactionFilter(v2TxFilter.filtersByParty)),
            verbose = request.verbose,
          ),
          ledgerEnd,
        )

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req =>
            if (req.filter.filtersByParty.isEmpty) Source.empty
            else {
              LoggingContextWithTrace.withEnrichedLoggingContext(
                logging.startExclusive(req.startExclusive),
                logging.endInclusive(req.endInclusive),
                logging.filters(req.filter),
                logging.verbose(req.verbose),
              ) { implicit loggingContext =>
                logger.info(
                  s"Received request for updates, ${loggingContext
                      .serializeFiltered("startExclusive", "endInclusive", "filters", "verbose")}."
                )(loggingContext.traceContext)
              }
              logger.trace(s"Update request: $req.")
              transactionsService
                .transactions(req.startExclusive, req.endInclusive, req.filter, req.verbose)
                .via(logger.enrichedDebugStream("Responding with updates.", updatesLoggable))
                .via(logger.logErrorsOnStream)
                .via(StreamMetrics.countElements(metrics.daml.lapi.streams.updates))
            },
        )
      }
    }

  override def getUpdateTrees(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdateTreesResponse],
  ): Unit =
    registerStream(responseObserver) {
      implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
      implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new update trees request $request.")
      Source.future(transactionsService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        val validation = validator.validate(
          GetTransactionsRequest(
            ledgerId = "",
            begin = request.beginExclusive.map(toV1),
            end = request.endInclusive.map(toV1),
            filter = request.filter.map(v2TxFilter => TransactionFilter(v2TxFilter.filtersByParty)),
            verbose = request.verbose,
          ),
          ledgerEnd,
        )

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req =>
            if (req.filter.filtersByParty.isEmpty) Source.empty
            else {
              LoggingContextWithTrace.withEnrichedLoggingContext(
                logging.startExclusive(req.startExclusive),
                logging.endInclusive(req.endInclusive),
                logging.filters(req.filter),
                logging.verbose(req.verbose),
              ) { implicit loggingContext =>
                logger.info(
                  s"Received request for update trees, ${loggingContext
                      .serializeFiltered("startExclusive", "endInclusive", "filters", "verbose")}."
                )(loggingContext.traceContext)
              }
              logger.trace(s"Update tree request: $req.")
              transactionsService
                .transactionTrees(req.startExclusive, req.endInclusive, req.filter, req.verbose)
                .via(logger.enrichedDebugStream("Responding with update trees.", updatesLoggable))
                .via(logger.logErrorsOnStream)
                .via(StreamMetrics.countElements(metrics.daml.lapi.streams.updates))
            },
        )
      }
    }

  override def getTransactionTreeByEventId(
      req: GetTransactionByEventIdRequest
  ): Future[GetTransactionTreeResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    validator
      .validateTransactionByEventId(toV1(req))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace(loggingContextWithTrace.traceContext)(
              LoggingContext.enriched(
                logging.eventId(request.eventId),
                logging.parties(request.requestingParties),
              )(loggingContext)
            )
          logger.info(s"Received request for transaction tree by event ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction tree by event ID request: $request")(
            loggingContextWithTrace.traceContext
          )
          EventId
            .fromString(request.eventId.unwrap)
            .map { case EventId(transactionId, _) =>
              transactionsService
                .getTransactionTreeById(TransactionId(transactionId), request.requestingParties)(
                  loggingContextWithTrace
                )
                .flatMap {
                  case None =>
                    Future.failed(
                      LedgerApiErrors.RequestValidation.NotFound.Transaction
                        .Reject(transactionId)
                        .asGrpcError
                    )
                  case Some(transactionTree) =>
                    Future.successful(transactionTree)
                }
            }
            .getOrElse {
              Future.failed {
                ValidationErrors.invalidArgument(s"invalid eventId: ${request.eventId}")
              }
            }
            .andThen(
              logger
                .logErrorsOnCall[GetTransactionTreeResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getTransactionTreeById(
      req: GetTransactionByIdRequest
  ): Future[GetTransactionTreeResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    validator
      .validateTransactionById(toV1(req))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace(loggingContextWithTrace.traceContext)(
              LoggingContext.enriched(
                logging.transactionId(request.transactionId),
                logging.parties(request.requestingParties),
              )(loggingContext)
            )
          logger.info(s"Received request for transaction tree by ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction tree by ID request: $request")(
            loggingContextWithTrace.traceContext
          )
          transactionsService
            .getTransactionTreeById(request.transactionId, request.requestingParties)(
              loggingContextWithTrace
            )
            .flatMap {
              case None =>
                Future.failed(
                  LedgerApiErrors.RequestValidation.NotFound.Transaction
                    .Reject(request.transactionId.unwrap)
                    .asGrpcError
                )
              case Some(transactionTree) =>
                Future.successful(transactionTree)
            }
            .andThen(
              logger
                .logErrorsOnCall[GetTransactionTreeResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getTransactionByEventId(
      req: GetTransactionByEventIdRequest
  ): Future[GetTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    validator
      .validateTransactionByEventId(toV1(req))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace(loggingContextWithTrace.traceContext)(
              LoggingContext.enriched(
                logging.eventId(request.eventId),
                logging.parties(request.requestingParties),
              )(loggingContext)
            )
          logger.info(s"Received request for transaction by event ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction by event ID request: $request")(
            loggingContextWithTrace.traceContext
          )
          EventId
            .fromString(request.eventId.unwrap)
            .map { case EventId(transactionId, _) =>
              transactionsService
                .getTransactionById(TransactionId(transactionId), request.requestingParties)(
                  loggingContextWithTrace
                )
                .flatMap {
                  case None =>
                    Future.failed(
                      LedgerApiErrors.RequestValidation.NotFound.Transaction
                        .Reject(transactionId)
                        .asGrpcError
                    )
                  case Some(transaction) =>
                    Future.successful(transaction)
                }
            }
            .getOrElse {
              Future.failed {
                ValidationErrors.invalidArgument(s"invalid eventId: ${request.eventId}")
              }
            }
            .andThen(
              logger.logErrorsOnCall[GetTransactionResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getTransactionById(
      req: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    validator
      .validateTransactionById(toV1(req))
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace(loggingContextWithTrace.traceContext)(
              LoggingContext.enriched(
                logging.transactionId(request.transactionId),
                logging.parties(request.requestingParties),
              )(loggingContext)
            )
          logger.info(s"Received request for transaction by ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction by ID request: $request")(loggingContextWithTrace.traceContext)
          transactionsService
            .getTransactionById(request.transactionId, request.requestingParties)(
              loggingContextWithTrace
            )
            .flatMap {
              case None =>
                Future.failed(
                  LedgerApiErrors.RequestValidation.NotFound.Transaction
                    .Reject(request.transactionId.unwrap)
                    .asGrpcError
                )
              case Some(transaction) =>
                Future.successful(transaction)
            }
            .andThen(
              logger.logErrorsOnCall[GetTransactionResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  private def updatesLoggable(updates: GetUpdatesResponse): LoggingEntries =
    updates.update match {
      case GetUpdatesResponse.Update.Transaction(t) =>
        entityLoggable(t.commandId, t.updateId, t.workflowId, t.offset)
      case GetUpdatesResponse.Update.Reassignment(r) =>
        entityLoggable(r.commandId, r.updateId, r.workflowId, r.offset)
      case GetUpdatesResponse.Update.Empty =>
        LoggingEntries()
    }

  private def updatesLoggable(updates: GetUpdateTreesResponse): LoggingEntries =
    updates.update match {
      case GetUpdateTreesResponse.Update.TransactionTree(t) =>
        entityLoggable(t.commandId, t.updateId, t.workflowId, t.offset)
      case GetUpdateTreesResponse.Update.Reassignment(r) =>
        entityLoggable(r.commandId, r.updateId, r.workflowId, r.offset)
      case GetUpdateTreesResponse.Update.Empty =>
        LoggingEntries()
    }

  private def entityLoggable(
      commandId: String,
      transactionId: String,
      workflowId: String,
      offset: String,
  ): LoggingEntries =
    LoggingEntries(
      logging.commandId(commandId),
      logging.transactionId(transactionId),
      logging.workflowId(workflowId),
      logging.offset(offset),
    )
}
