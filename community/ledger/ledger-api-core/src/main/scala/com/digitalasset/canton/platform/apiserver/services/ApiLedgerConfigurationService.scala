// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import akka.stream.Materializer
import com.daml.api.util.DurationConversion.*
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.ledger_configuration_service.*
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{
  GrpcApiService,
  GrpcLedgerConfigurationService,
  StreamingServiceLifecycleManagement,
}
import com.digitalasset.canton.ledger.error.DamlContextualizedErrorLogger
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigurationService
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiLedgerConfigurationService private (
    configurationService: IndexConfigurationService
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends LedgerConfigurationServiceGrpc.LedgerConfigurationService
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)
  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit = registerStream(responseObserver) {
    logger.info(s"Received request for configuration subscription: $request")
    configurationService
      .getLedgerConfiguration()
      .map(configuration =>
        GetLedgerConfigurationResponse(
          Some(
            LedgerConfiguration(
              Some(toProto(configuration.maxDeduplicationDuration))
            )
          )
        )
      )
      .via(logger.logErrorsOnStream)
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiLedgerConfigurationService {
  def create(
      ledgerId: LedgerId,
      configurationService: IndexConfigurationService,
  )(implicit
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): LedgerConfigurationServiceGrpc.LedgerConfigurationService with GrpcApiService = {
    new GrpcLedgerConfigurationService(
      service = new ApiLedgerConfigurationService(configurationService),
      ledgerId = ledgerId,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, executionContext)
    }
  }
}
