// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.tea.v1.SubmissionAccountingServiceGrpc.SubmissionAccountingService
import com.digitalasset.canton.tea.v1.{
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
  SubmissionAccountingServiceGrpc,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

/** Grpc service implementing
  * [[com.digitalasset.canton.tea.v1.SubmissionAccountingServiceGrpc.SubmissionAccountingService]].
  */
class TeaSubmissionAccountingServiceGrpc(
    service: TrafficEnforcementService,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SubmissionAccountingService
    with GrpcApiService
    with NamedLogging {

  override def registerSubmission(
      request: RegisterSubmissionRequest
  ): Future[RegisterSubmissionResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherT(
      service
        .registerSubmission(request)
        .map(_.leftMap(_.asGrpcError))
    ).asGrpcResponse
  }

  override def bindService(): ServerServiceDefinition =
    SubmissionAccountingServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()
}
