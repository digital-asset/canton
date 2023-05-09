// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.*
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.canton.ledger.error.{DamlContextualizedErrorLogger, LedgerApiErrors}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexActiveContractsService as ACSBackend
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.platform.api.grpc.GrpcApiService
import com.digitalasset.canton.platform.server.api.ValidationLogger
import com.digitalasset.canton.platform.server.api.services.grpc.Logging.traceId
import com.digitalasset.canton.platform.server.api.services.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.platform.server.api.validation.{
  ActiveContractsServiceValidation,
  FieldValidations,
}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiActiveContractsService private (
    backend: ACSBackend,
    metrics: Metrics,
    telemetry: Telemetry,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends ActiveContractsServiceGrpc.ActiveContractsService
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = registerStream(responseObserver) {
    val result = for {
      filters <- TransactionFilterValidator.validate(request.getFilter)
      activeAtO <- FieldValidations.optionalString(request.activeAtOffset)(str =>
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
      withEnrichedLoggingContext(
        logging.filters(filters),
        traceId(telemetry.traceIdFromGrpcContext),
      ) { implicit loggingContext =>
        logger.info(s"Received request for active contracts: $request")
        backend.getActiveContracts(
          filter = filters,
          verbose = request.verbose,
          activeAtO = activeAtO,
        )
      }
    }
    result
      .fold(t => Source.failed(ValidationLogger.logFailure(request, t)), identity)
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
  )(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ActiveContractsService with GrpcApiService = {
    val service = new ApiActiveContractsService(
      backend = backend,
      metrics = metrics,
      telemetry = telemetry,
    )
    new ActiveContractsServiceValidation(
      service = service,
      ledgerId = ledgerId,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, executionContext)
    }
  }
}
