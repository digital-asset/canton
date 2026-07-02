// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.tea.TrafficEnforcementService.{
  NotEnoughTraffic,
  TrafficEnforcementServiceError,
}
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.{ServerServiceDefinition, Status, StatusRuntimeException}

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
        .map(_.leftMap(handleError))
    ).asGrpcResponse
  }

  override def updateAccount(
      request: UpdateAccountRequest
  ): Future[UpdateAccountResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherT(
      service
        .updateAccount(request)
        .map(_.leftMap(handleError))
    ).asGrpcResponse
  }

  /** Maps a [[TrafficEnforcementServiceError]] onto the gRPC status returned to the caller. */
  private def handleError(
      error: TrafficEnforcementServiceError
  )(implicit traceContext: TraceContext): StatusRuntimeException =
    error match {
      case NotEnoughTraffic(account, balance, cost) =>
        logger.info(
          s"Rejecting traffic reservation for account $account: balance $balance is below cost $cost"
        )
        Status.RESOURCE_EXHAUSTED
          .withDescription(
            s"Not enough traffic for account $account: balance $balance is below cost $cost"
          )
          .asRuntimeException()
    }

  override def bindService(): ServerServiceDefinition =
    TrafficServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()
}
