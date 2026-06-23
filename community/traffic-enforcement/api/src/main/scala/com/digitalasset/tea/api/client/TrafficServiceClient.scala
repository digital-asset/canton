// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.tea.api.client

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficServiceStub
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.ManagedChannel

import scala.concurrent.Future

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class TrafficServiceClient(
    channel: ManagedChannel,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val stub: TrafficServiceStub =
    TrafficServiceGrpc.stub(channel)

  def getAccount(
      request: GetAccountRequest
  )(implicit traceContext: TraceContext): Future[GetAccountResponse] =
    tracingStub.getAccount(request)

  def updateAccount(
      request: UpdateAccountRequest
  )(implicit traceContext: TraceContext): Future[UpdateAccountResponse] =
    tracingStub.updateAccount(request)

  private def tracingStub(implicit
      traceContext: TraceContext
  ): TrafficServiceStub =
    stub
      .withInterceptors(TraceContextGrpc.clientInterceptor())
      .withOption(TraceContextGrpc.TraceContextOptionsKey, traceContext)
}
