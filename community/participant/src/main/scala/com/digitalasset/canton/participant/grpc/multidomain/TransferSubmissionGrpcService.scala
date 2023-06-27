// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.grpc.multidomain

import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.multidomain.TransferSubmissionApiService
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.BindableService

import scala.concurrent.{ExecutionContext, Future}

object TransferSubmissionGrpcService {

  def bindableService(
      namedLoggerFactory: NamedLoggerFactory
  )(transferSubmissionApiService: TransferSubmissionApiService)(implicit
      transportExecutionContext: ExecutionContext
  ): BindableService = () =>
    CommandSubmissionServiceGrpc.bindService(
      serviceImpl = new CommandSubmissionServiceGrpc.CommandSubmissionService with NamedLogging {
        override def submitReassignment(
            request: SubmitReassignmentRequest
        ): Future[SubmitReassignmentResponse] = {
          TraceContextGrpc.withGrpcTraceContext { implicit tc: TraceContext =>
            for {
              transferCommand <- Future.fromTry(
                GrpcValidation.validateTransferSubmitRequest(request)
              )
              _ <- transferSubmissionApiService.submit(transferCommand)
            } yield SubmitReassignmentResponse()
          }
        }

        override def submit(request: SubmitRequest): Future[SubmitResponse] =
          throw new UnsupportedOperationException(
            "This endpoint is not implemented yet; use command submission from v1 service to submit transactions"
          )

        override val loggerFactory: NamedLoggerFactory = namedLoggerFactory
      },
      executionContext = transportExecutionContext,
    )
}
