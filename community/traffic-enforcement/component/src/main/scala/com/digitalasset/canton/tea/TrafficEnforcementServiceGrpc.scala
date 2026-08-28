// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  PruneEventsRequest,
  PruneEventsResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

/** Grpc service implementing [[com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService]].
  */
class TrafficEnforcementServiceGrpc(
    service: TrafficEnforcementService,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TrafficService
    with GrpcApiService
    with NamedLogging {

  override def getAccount(
      request: GetAccountRequest
  ): Future[GetAccountResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherT(
      service
        .getAccount(request)
        .map(_.leftMap(_.asGrpcError))
    ).asGrpcResponse
  }

  override def updateAccount(
      request: UpdateAccountRequest
  ): Future[UpdateAccountResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherT(
      service
        .updateAccount(request)
        .map(_.leftMap(_.asGrpcError))
    ).asGrpcResponse
  }

  override def pruneEvents(request: PruneEventsRequest): Future[PruneEventsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherT(
      service
        .pruneEvents(request)
        .map(_.leftMap(_.asGrpcError))
    ).asGrpcResponse
  }

  override def bindService(): ServerServiceDefinition =
    TrafficServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()
}
