// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.traffic

import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

/** The [[com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService]] Ledger API backend.
  * Internally proxies requests to the traffic enforcement server via the
  * [[com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient]].
  */
class ApiTrafficService(
    client: RichTrafficServiceClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends TrafficService
    with GrpcApiService
    with NamedLogging {

  override def getAccount(request: GetAccountRequest): Future[GetAccountResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    client.getAccount(request).asGrpcFuture
  }

  override def updateAccount(request: UpdateAccountRequest): Future[UpdateAccountResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    client.updateAccount(request).asGrpcFuture
  }

  override def bindService(): ServerServiceDefinition =
    TrafficServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()
}
