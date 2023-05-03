// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.Checkpoint as PbCheckpoint
import com.daml.ledger.api.v1.completion.Completion as PbCompletion
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.error.{
  CommonErrors,
  DamlContextualizedErrorLogger,
  LedgerApiErrors,
}
import com.digitalasset.canton.platform.apiserver.services.logging
import com.google.rpc.status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import scala.util.{Failure, Success, Try}

final case class CompletionResponse(checkpoint: Option[PbCheckpoint], completion: PbCompletion)

object CompletionResponse {
  def fromCompletion(
      completion: PbCompletion,
      checkpoint: Option[PbCheckpoint],
  ): Try[CompletionResponse] =
    completion.status
      .toRight(missingStatusError(completion))
      .toTry
      .flatMap {
        case status if status.code == 0 =>
          Success(CompletionResponse(checkpoint, completion))
        case nonZeroStatus =>
          Failure(
            StatusProto.toStatusRuntimeException(
              status.Status.toJavaProto(nonZeroStatus)
            )
          )
      }

  def timeout(commandId: String, submissionId: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Try[CompletionResponse] =
    Failure(
      CommonErrors.RequestTimeOut
        .Reject(
          s"Timed out while awaiting for a completion corresponding to a command submission with command-id=$commandId and submission-id=$submissionId.",
          definiteAnswer = false,
        )
        .asGrpcError
    )

  def duplicate(
      submissionId: String
  )(implicit loggingContext: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(
      LedgerApiErrors.ConsistencyErrors.DuplicateCommand
        .Reject(existingCommandSubmissionId = Some(submissionId))
        .asGrpcError
    )

  def closing(implicit loggingContext: ContextualizedErrorLogger): Try[CompletionResponse] =
    Failure(CommonErrors.ServerIsShuttingDown.Reject().asGrpcError)

  private def missingStatusError(completion: PbCompletion): StatusRuntimeException = {
    val loggingContext = LoggingContext(
      logging.submissionId(completion.submissionId),
      logging.commandId(completion.commandId),
      logging.actAsStrings(completion.actAs),
    )

    val errorLogger = DamlContextualizedErrorLogger.forClass(
      getClass,
      loggingContext,
      Some(completion.submissionId),
    )

    CommonErrors.ServiceInternalError
      .Generic(
        "Missing status in completion response",
        throwableO = None,
      )(errorLogger)
      .asGrpcError
  }
}
