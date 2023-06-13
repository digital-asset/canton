// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService as ApiCommandSubmissionService
import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest as ApiSubmitRequest,
}
import com.daml.metrics.{Metrics, Timed}
import com.daml.tracing.{SpanAttribute, Telemetry, TelemetryContext}
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.services.CommandSubmissionService
import com.digitalasset.canton.ledger.api.validation.{CommandsValidator, SubmitRequestValidator}
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

class GrpcCommandSubmissionService(
    override protected val service: CommandSubmissionService with AutoCloseable,
    ledgerId: LedgerId,
    currentLedgerTime: () => Instant,
    currentUtcTime: () => Instant,
    maxDeduplicationDuration: () => Option[Duration],
    submissionIdGenerator: SubmissionIdGenerator,
    metrics: Metrics,
    explicitDisclosureUnsafeEnabled: Boolean,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ApiCommandSubmissionService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  private val validator = new SubmitRequestValidator(
    CommandsValidator(ledgerId, explicitDisclosureUnsafeEnabled)
  )

  override def submit(request: ApiSubmitRequest): Future[Empty] = {
    implicit val telemetryContext: TelemetryContext =
      telemetry.contextFromGrpcThreadLocalContext()
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    request.commands.foreach { commands =>
      telemetryContext
        .setAttribute(SpanAttribute.ApplicationId, commands.applicationId)
        .setAttribute(SpanAttribute.CommandId, commands.commandId)
        .setAttribute(SpanAttribute.Submitter, commands.party)
        .setAttribute(SpanAttribute.WorkflowId, commands.workflowId)
    }
    val requestWithSubmissionId = generateSubmissionIdIfEmpty(request)
    val errorLogger: ContextualizedErrorLogger =
      ErrorLoggingContext.fromOption(
        logger,
        loggingContextWithTrace,
        requestWithSubmissionId.commands.map(_.submissionId),
      )
    Timed.timedAndTrackedFuture(
      metrics.daml.commands.submissions,
      metrics.daml.commands.submissionsRunning,
      Timed
        .value(
          metrics.daml.commands.validation,
          validator.validate(
            req = requestWithSubmissionId,
            currentLedgerTime = currentLedgerTime(),
            currentUtcTime = currentUtcTime(),
            maxDeduplicationDuration = maxDeduplicationDuration(),
            domainIdString = None,
          )(errorLogger),
        )
        .fold(
          t =>
            Future.failed(ValidationLogger.logFailureWithTrace(logger, requestWithSubmissionId, t)),
          service.submit(_).map(_ => Empty.defaultInstance),
        ),
    )
  }

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)

  private def generateSubmissionIdIfEmpty(request: ApiSubmitRequest): ApiSubmitRequest =
    if (request.commands.exists(_.submissionId.isEmpty))
      request.update(_.commands.submissionId := submissionIdGenerator.generate())
    else
      request
}
