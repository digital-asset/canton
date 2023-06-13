// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.CompletionRequest
import com.digitalasset.canton.participant.protocol.v0.multidomain.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

private[participant] object CommandCompletionGrpcService {
  val ApiDataConversionParallelism = 1

  def bindableService(
      namedLoggerFactory: NamedLoggerFactory,
      grpcStream: GrpcStream,
      updateApiService: UpdateApiService,
  )(implicit
      transportExecutionContext: ExecutionContext
  ): BindableService = new BindableService with AutoCloseable {
    override def bindService(): ServerServiceDefinition =
      CommandCompletionServiceGrpc.bindService(
        serviceImpl = new CommandCompletionServiceGrpc.CommandCompletionService with NamedLogging {
          override val loggerFactory: NamedLoggerFactory = namedLoggerFactory

          override def completionStream(
              request: CompletionStreamRequest,
              responseObserver: StreamObserver[CompletionStreamResponse],
          ): Unit =
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              def convertedCompletions(
                  apiRequest: CompletionRequest
              ): Source[CompletionStreamResponse, NotUsed] =
                updateApiService
                  .completions(apiRequest)
                  .mapAsync(ApiDataConversionParallelism) {
                    case (offset, acceptOrReject, completionInfo) =>
                      Future {
                        GrpcConversion.toApiCompletion(offset, acceptOrReject, completionInfo)
                      }
                  }

              GrpcValidation
                .validateCompletionRequest(request)
                .fold(Source.failed, convertedCompletions)
                .pipe(grpcStream(responseObserver))
            }

          override def completionEnd(request: CompletionEndRequest): Future[CompletionEndResponse] =
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              for {
                ledgerEndO <- updateApiService.ledgerEnd()
              } yield CompletionEndResponse(Some(GrpcConversion.toApiLedgerOffset(ledgerEndO)))
            }

        },
        executionContext = transportExecutionContext,
      )

    override def close(): Unit = grpcStream.close()
  }
}
