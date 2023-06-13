// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import akka.stream.scaladsl.Source
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.StateApiService
import com.digitalasset.canton.participant.protocol.v0.multidomain.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherUtil.RichEither
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

object StateGrpcService {
  val ApiDataConversionParallelism = 1

  def bindableService(
      namedLoggerFactory: NamedLoggerFactory,
      grpcStream: GrpcStream,
      stateApiService: StateApiService,
  )(implicit
      transportExecutionContext: ExecutionContext
  ): BindableService = new BindableService with AutoCloseable {
    override def bindService(): ServerServiceDefinition =
      StateServiceGrpc.bindService(
        serviceImpl = new StateServiceGrpc.StateService with NamedLogging {
          override val loggerFactory: NamedLoggerFactory = namedLoggerFactory

          override def getActiveContracts(
              request: GetActiveContractsRequest,
              responseObserver: StreamObserver[GetActiveContractsResponse],
          ): Unit = {
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              val syncResponse: Future[GetActiveContractsResponse] = for {
                request <- Future.fromTry(
                  GrpcValidation.validateActiveContractsRequest(request)
                )
                response <- stateApiService.getActiveContracts(request)
                apiResponse <- GrpcConversion
                  .toApi(response)(request.filters.keys.toSet)
                  .toFuture(error =>
                    new RuntimeException(
                      s"failed converting $response to Grpc response because $error"
                    )
                  )
              } yield apiResponse

              // TODO(#11002) Implement pagination to populate the stream
              Source
                .future(syncResponse)
                .mapConcat(protoIncompleteResponse =>
                  protoIncompleteResponse.contractStateComponents
                    .map(contract =>
                      GetActiveContractsResponse(
                        offset = "",
                        contractStateComponents = Seq(contract),
                      )
                    )
                    .appended(
                      GetActiveContractsResponse(
                        offset = protoIncompleteResponse.offset,
                        contractStateComponents = Seq.empty,
                      )
                    )
                )
                .pipe(grpcStream(responseObserver))
            }
          }

          override def getConnectedDomains(
              request: GetConnectedDomainsRequest
          ): Future[GetConnectedDomainsResponse] = {
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              for {
                request <- Future.fromTry(GrpcValidation.validateConnectedDomainRequest(request))
                response <- stateApiService.getConnectedDomains(request)
                protoResponse = GrpcConversion.toApi(response)
              } yield protoResponse
            }
          }

          override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] =
            TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
              for {
                ledgerEnd <- stateApiService.ledgerEnd()
              } yield GetLedgerEndResponse(GrpcConversion.toAbsoluteOffset(ledgerEnd))
            }
        },
        executionContext = transportExecutionContext,
      )

    override def close(): Unit = grpcStream.close()
  }
}
