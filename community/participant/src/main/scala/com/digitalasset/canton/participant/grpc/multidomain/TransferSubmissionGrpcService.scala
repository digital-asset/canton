// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.TransferSubmissionApiService
import com.digitalasset.canton.participant.protocol.v0.multidomain.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.protobuf.empty.Empty
import io.grpc.BindableService

import scala.concurrent.{ExecutionContext, Future}

object TransferSubmissionGrpcService {

  def bindableService(
      namedLoggerFactory: NamedLoggerFactory
  )(transferSubmissionApiService: TransferSubmissionApiService)(implicit
      transportExecutionContext: ExecutionContext
  ): BindableService = () =>
    TransferSubmissionServiceGrpc.bindService(
      serviceImpl = new TransferSubmissionServiceGrpc.TransferSubmissionService with NamedLogging {
        override def submit(request: SubmitRequest): Future[Empty] = {
          TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
            for {
              transferCommand <- Future.fromTry(
                GrpcValidation.validateTransferSubmitRequest(request)
              )
              _ <- transferSubmissionApiService.submit(transferCommand)
            } yield Empty()
          }
        }

        override val loggerFactory: NamedLoggerFactory = namedLoggerFactory
      },
      executionContext = transportExecutionContext,
    )
}
