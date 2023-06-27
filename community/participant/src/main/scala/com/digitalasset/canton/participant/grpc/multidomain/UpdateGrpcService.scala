// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByEventIdRequest,
  GetTransactionByIdRequest,
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.UpdateRequest
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

object UpdateGrpcService {
  val ApiDataConversionParallelism = 1

  def bindableService(
      namedLoggerFactory: NamedLoggerFactory,
      grpcStream: GrpcStream,
      updateApiService: UpdateApiService,
  )(implicit
      transportExecutionContext: ExecutionContext
  ): BindableService = new BindableService with AutoCloseable {
    override def bindService(): ServerServiceDefinition =
      UpdateServiceGrpc.bindService(
        serviceImpl = new UpdateServiceGrpc.UpdateService with NamedLogging {
          override def getUpdateTrees(
              request: GetUpdatesRequest,
              responseObserver: StreamObserver[GetUpdateTreesResponse],
          ): Unit =
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              def convertedUpdateTrees(
                  apiRequest: UpdateRequest
              ): Source[GetUpdateTreesResponse, NotUsed] =
                updateApiService
                  .updates(apiRequest)
                  .mapAsync(ApiDataConversionParallelism) { case (globalOffset, update) =>
                    GrpcConversion
                      .toApi(globalOffset, update)(apiRequest.filters)
                      .toFuture(error =>
                        new RuntimeException(
                          s"Failed converting $update to Grpc response because $error"
                        )
                      )
                  }

              GrpcValidation
                .validateUpdatesRequest(request)
                .fold(Source.failed, convertedUpdateTrees)
                .pipe(grpcStream(responseObserver))
            }

          override def getUpdates(
              request: GetUpdatesRequest,
              responseObserver: StreamObserver[GetUpdatesResponse],
          ): Unit = throw new UnsupportedOperationException("This endpoint is not implemented yet")

          override def getTransactionTreeByEventId(
              request: GetTransactionByEventIdRequest
          ): Future[GetTransactionTreeResponse] =
            throw new UnsupportedOperationException("This endpoint is not implemented yet")

          override def getTransactionTreeById(
              request: GetTransactionByIdRequest
          ): Future[GetTransactionTreeResponse] =
            throw new UnsupportedOperationException("This endpoint is not implemented yet")

          override def getTransactionByEventId(
              request: GetTransactionByEventIdRequest
          ): Future[GetTransactionResponse] =
            throw new UnsupportedOperationException("This endpoint is not implemented yet")

          override def getTransactionById(
              request: GetTransactionByIdRequest
          ): Future[GetTransactionResponse] =
            throw new UnsupportedOperationException("This endpoint is not implemented yet")

          override val loggerFactory: NamedLoggerFactory = namedLoggerFactory
        },
        executionContext = transportExecutionContext,
      )

    override def close(): Unit = grpcStream.close()
  }
}
