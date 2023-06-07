// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.api.util.TimeProvider
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.command_service.*
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionByIdRequest,
  GetTransactionResponse,
}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, GrpcCommandService}
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.digitalasset.canton.ledger.error.{CommonErrors, DamlContextualizedErrorLogger}
import com.digitalasset.canton.platform.apiserver.configuration.LedgerConfigurationSubscription
import com.digitalasset.canton.platform.apiserver.services.ApiCommandService.*
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.google.protobuf.empty.Empty
import io.grpc.Context

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiCommandService private[services] (
    transactionServices: TransactionServices,
    submissionTracker: SubmissionTracker,
    submit: SubmitRequest => Future[Empty],
    defaultTrackingTimeout: Duration,
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends CommandServiceGrpc.CommandService
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val running = new AtomicBoolean(true)

  override def close(): Unit = {
    logger.info("Shutting down Command Service")
    running.set(false)
    submissionTracker.close()
  }

  override def submitAndWait(request: SubmitAndWaitRequest): Future[Empty] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).map { _ =>
        Empty.defaultInstance
      }
    }

  override def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionIdResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).map { response =>
        SubmitAndWaitForTransactionIdResponse.of(
          transactionId = response.completion.transactionId,
          completionOffset = offsetFromCheckpoint(response.checkpoint),
        )
      }
    }

  private def offsetFromCheckpoint(checkpoint: Option[Checkpoint]) =
    checkpoint.flatMap(_.offset).flatMap(_.value.absolute).getOrElse("")

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          ledgerId = request.getCommands.ledgerId,
          transactionId = resp.completion.transactionId,
          requestingParties = effectiveActAs.toList,
        )
        transactionServices
          .getFlatTransactionById(txRequest)
          .map(transactionResponse =>
            SubmitAndWaitForTransactionResponse
              .of(
                transactionResponse.transaction,
                transactionResponse.transaction.map(_.offset).getOrElse(""),
              )
          )
      }
    }

  override def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitForTransactionTreeResponse] =
    withCommandsLoggingContext(request.getCommands) { case (enrichedLoggingContext, errorLogger) =>
      submitAndWaitInternal(request)(enrichedLoggingContext, errorLogger).flatMap { resp =>
        val effectiveActAs = CommandsValidator.effectiveSubmitters(request.getCommands).actAs
        val txRequest = GetTransactionByIdRequest(
          ledgerId = request.getCommands.ledgerId,
          transactionId = resp.completion.transactionId,
          requestingParties = effectiveActAs.toList,
        )
        transactionServices
          .getTransactionById(txRequest)
          .map(resp =>
            SubmitAndWaitForTransactionTreeResponse
              .of(resp.transaction, resp.transaction.map(_.offset).getOrElse(""))
          )
      }
    }

  private def submitAndWaitInternal(
      request: SubmitAndWaitRequest
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[CompletionResponse] =
    if (running.get()) {
      val timeout = Option(Context.current().getDeadline)
        .map(deadline => Duration.ofNanos(deadline.timeRemaining(TimeUnit.NANOSECONDS)))
        .getOrElse(defaultTrackingTimeout)

      val commands = request.commands.getOrElse(
        throw new IllegalArgumentException("Missing commands field in request")
      )
      submissionTracker.track(commands, timeout, submit)
    } else
      Future.failed(CommonErrors.ServiceNotRunning.Reject("Command Service").asGrpcError)

  private def withCommandsLoggingContext[T](commands: Commands)(
      submitWithContext: (LoggingContext, ContextualizedErrorLogger) => Future[T]
  )(implicit loggingContext: LoggingContext): Future[T] =
    withEnrichedLoggingContext(
      logging.submissionId(commands.submissionId),
      logging.commandId(commands.commandId),
      logging.partyString(commands.party),
      logging.actAsStrings(commands.actAs),
      logging.readAsStrings(commands.readAs),
    ) { loggingContext =>
      submitWithContext(
        loggingContext,
        new DamlContextualizedErrorLogger(
          logger,
          loggingContext,
          Some(commands.submissionId),
        ),
      )
    }
}

private[apiserver] object ApiCommandService {

  def create(
      submissionTracker: SubmissionTracker,
      submit: SubmitRequest => Future[Empty],
      configuration: Configuration,
      transactionServices: TransactionServices,
      timeProvider: TimeProvider,
      ledgerConfigurationSubscription: LedgerConfigurationSubscription,
      explicitDisclosureUnsafeEnabled: Boolean,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): CommandServiceGrpc.CommandService with GrpcApiService =
    new GrpcCommandService(
      service = new ApiCommandService(
        transactionServices,
        submissionTracker,
        submit,
        configuration.defaultTrackingTimeout,
      ),
      ledgerId = configuration.ledgerId,
      currentLedgerTime = () => timeProvider.getCurrentTime,
      currentUtcTime = () => Instant.now,
      maxDeduplicationDuration = () =>
        ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
      generateSubmissionId = SubmissionIdGenerator.Random,
      explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
    )

  final case class Configuration(
      ledgerId: LedgerId,
      defaultTrackingTimeout: Duration,
  )

  final class TransactionServices(
      val getTransactionById: GetTransactionByIdRequest => Future[GetTransactionResponse],
      val getFlatTransactionById: GetTransactionByIdRequest => Future[GetFlatTransactionResponse],
  )
}
