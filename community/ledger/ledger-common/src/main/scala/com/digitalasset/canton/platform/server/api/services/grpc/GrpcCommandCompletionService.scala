// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.server.api.services.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_completion_service.*
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.validation.CompletionServiceRequestValidator
import com.digitalasset.canton.platform.server.api.ValidationLogger
import com.digitalasset.canton.platform.server.api.services.domain.CommandCompletionService
import com.digitalasset.canton.platform.server.api.services.grpc.Logging.traceId
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandCompletionService(
    service: CommandCompletionService,
    validator: CompletionServiceRequestValidator,
    telemetry: Telemetry,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandCompletionServiceGrpc.CommandCompletionService
    with StreamingServiceLifecycleManagement {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)
  protected implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    withEnrichedLoggingContext(traceId(telemetry.traceIdFromGrpcContext)) {
      implicit loggingContext =>
        registerStream(responseObserver) {
          validator
            .validateGrpcCompletionStreamRequest(request)
            .fold(
              t => Source.failed[CompletionStreamResponse](ValidationLogger.logFailure(request, t)),
              service.completionStreamSource,
            )
        }
    }

  override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
    validator
      .validateCompletionEndRequest(request)
      .fold(
        t => Future.failed[CompletionEndResponse](ValidationLogger.logFailure(request, t)),
        _ =>
          service
            .getLedgerEnd()
            .map(abs =>
              CompletionEndResponse(Some(LedgerOffset(LedgerOffset.Value.Absolute(abs.value))))
            ),
      )

}