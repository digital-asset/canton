// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.validation.{
  CommandsValidator,
  SubmitAndWaitRequestValidator,
}
import com.digitalasset.canton.ledger.api.{ProxyCloseable, SubmissionIdGenerator, ValidationLogger}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.google.protobuf.empty.Empty
import io.grpc.ServerServiceDefinition

import java.time.{Duration, Instant}
import scala.concurrent.{ExecutionContext, Future}

class GrpcCommandService(
    protected val service: CommandService & AutoCloseable,
    val ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    generateSubmissionId: SubmissionIdGenerator,
    explicitDisclosureUnsafeEnabled: Boolean,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends CommandService
    with GrpcApiService
    with ProxyCloseable
    with NamedLogging {

  private[this] val validator = new SubmitAndWaitRequestValidator(
    CommandsValidator(ledgerId, explicitDisclosureUnsafeEnabled)
  )

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    enrichRequestAndSubmit(request)(service.submitAndWait)

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransactionId)

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransaction)

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    enrichRequestAndSubmit(request)(service.submitAndWaitForTransactionTree)

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)

  private def enrichRequestAndSubmit[T](
      request: SubmitAndWaitRequest
  )(submit: SubmitAndWaitRequest => Future[T]): Future[T] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    validator
      .validate(
        requestWithSubmissionId,
        currentLedgerTime(),
        currentUtcTime(),
        maxDeduplicationDuration(),
      )(contextualizedErrorLogger(requestWithSubmissionId))
      .fold(
        t =>
          Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
        _ => submit(requestWithSubmissionId),
      )
  }

  private def generateSubmissionIdIfEmpty(request: SubmitAndWaitRequest): SubmitAndWaitRequest =
    if (request.commands.exists(_.submissionId.isEmpty)) {
      val commandsWithSubmissionId =
        request.commands.map(_.copy(submissionId = generateSubmissionId.generate()))
      request.copy(commands = commandsWithSubmissionId)
    } else {
      request
    }

  private def contextualizedErrorLogger(request: SubmitAndWaitRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ) =
    ErrorLoggingContext.fromOption(logger, loggingContext, request.commands.map(_.submissionId))
}
