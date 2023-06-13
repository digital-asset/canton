// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.*
import com.daml.metrics.Metrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{
  GrpcActiveContractsService,
  GrpcApiService,
  StreamingServiceLifecycleManagement,
}
import com.digitalasset.canton.ledger.api.validation.{FieldValidator, TransactionFilterValidator}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexActiveContractsService as ACSBackend
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiActiveContractsService private (
    backend: ACSBackend,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends ActiveContractsServiceGrpc.ActiveContractsService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  // TODO(#13269) remove the contextualizedErrorLogger
  protected val contextualizedErrorLogger: ContextualizedErrorLogger =
    errorLoggingContext(
      TraceContext.empty
    )

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = registerStream(responseObserver) {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    val result = for {
      filters <- TransactionFilterValidator.validate(request.getFilter)
      activeAtO <- FieldValidator.optionalString(request.activeAtOffset)(str =>
        ApiOffset.fromString(str).left.map { errorMsg =>
          LedgerApiErrors.RequestValidation.NonHexOffset
            .Error(
              fieldName = "active_at_offset",
              offsetValue = request.activeAtOffset,
              message = s"Reason: $errorMsg",
            )
            .asGrpcError
        }
      )
    } yield {
      withEnrichedLoggingContext(telemetry)(
        logging.filters(filters)
      ) { implicit loggingContext =>
        logger.info(
          s"Received request for active contracts: $request, ${loggingContext.serializeFiltered("filters")}."
        )
        backend.getActiveContracts(
          filter = filters,
          verbose = request.verbose,
          activeAtO = activeAtO,
        )
      }
    }
    result
      .fold(
        t =>
          Source.failed(
            ValidationLogger.logFailureWithTrace(logger, request, t)
          ),
        identity,
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.acs))
  }

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiActiveContractsService {

  def create(
      ledgerId: LedgerId,
      backend: ACSBackend,
      metrics: Metrics,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
  ): ActiveContractsService & GrpcApiService = {
    val service = new ApiActiveContractsService(
      backend = backend,
      metrics = metrics,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )
    new GrpcActiveContractsService(
      service = service,
      ledgerId = ledgerId,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, executionContext)
    }
  }
}
