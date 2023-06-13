// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService
import com.digitalasset.canton.participant.ledger.api.multidomain.UpdateApiService.UpdateRequest
import com.digitalasset.canton.participant.protocol.v0.multidomain.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext
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
          override def getTreeUpdates(
              request: GetUpdatesRequest,
              responseObserver: StreamObserver[GetTreeUpdatesResponse],
          ): Unit =
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              def convertedUpdateTrees(
                  apiRequest: UpdateRequest
              ): Source[GetTreeUpdatesResponse, NotUsed] =
                updateApiService
                  .updates(apiRequest)
                  .mapAsync(ApiDataConversionParallelism) { case (globalOffset, update) =>
                    GrpcConversion
                      .toApi(globalOffset, update)(apiRequest.requestingParty)
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

          override def getLedgerEnd(
              request: GetLedgerEndRequest,
              responseObserver: StreamObserver[GetLedgerEndResponse],
          ): Unit = TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
            def convertedLedgerEnds(): Source[GetLedgerEndResponse, NotUsed] =
              updateApiService
                .ledgerEnds()
                .map { globalOffset =>
                  GetLedgerEndResponse(GrpcConversion.toAbsoluteOffset(globalOffset))
                }

            convertedLedgerEnds().pipe(grpcStream(responseObserver))
          }

          override val loggerFactory: NamedLoggerFactory = namedLoggerFactory
        },
        executionContext = transportExecutionContext,
      )

    override def close(): Unit = grpcStream.close()
  }
}
