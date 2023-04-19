// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.definitions.{CommonErrors, LedgerApiErrors}
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.{GlobalKey, ProcessedDisclosedContract, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.InstrumentedGraph
import com.daml.tracing.TelemetryContext
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.ledger.sandbox.bridge.{BridgeMetrics, LedgerBridge}
import com.digitalasset.canton.ledger.sandbox.domain.{Rejection, Submission}

import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

class BridgeWriteService(
    feedSink: Sink[(Offset, Update), NotUsed],
    submissionBufferSize: Int,
    ledgerBridge: LedgerBridge,
    bridgeMetrics: BridgeMetrics,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends WriteService
    with AutoCloseable {
  import BridgeWriteService.*

  private[this] val logger = ContextualizedLogger.get(getClass)

  override def close(): Unit = {
    logger.info("Shutting down BridgeWriteService.")
    queue.complete()
  }

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] = {
    implicit val errorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, submitterInfo.submissionId)
    submitterInfo.deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(deduplicationDuration) =>
        validateDeduplicationDurationAndSubmit(
          submitterInfo,
          transactionMeta,
          transaction,
          estimatedInterpretationCost,
          deduplicationDuration,
          processedDisclosedContracts,
        )
      case DeduplicationPeriod.DeduplicationOffset(_) =>
        CompletableFuture.completedFuture(
          SubmissionResult.SynchronousError(
            Rejection
              .OffsetDeduplicationPeriodUnsupported(submitterInfo.toCompletionInfo())
              .toStatus
          )
        )
    }
  }

  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: Ref.SubmissionId,
      config: Configuration,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.Config(
        maxRecordTime = maxRecordTime,
        submissionId = submissionId,
        config = config,
      )
    )

  override def currentHealth(): HealthStatus = Healthy

  override def allocateParty(
      hint: Option[Ref.Party],
      displayName: Option[String],
      submissionId: Ref.SubmissionId,
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.AllocateParty(
        hint = hint,
        displayName = displayName,
        submissionId = submissionId,
      )
    )

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      archives: List[Archive],
      sourceDescription: Option[String],
  )(implicit
      loggingContext: LoggingContext,
      telemetryContext: TelemetryContext,
  ): CompletionStage[SubmissionResult] =
    submit(
      Submission.UploadPackages(
        submissionId = submissionId,
        archives = archives,
        sourceDescription = sourceDescription,
      )
    )

  override def prune(
      pruneUpToInclusive: Offset,
      submissionId: Ref.SubmissionId,
      pruneAllDivulgedContracts: Boolean,
  ): CompletionStage[PruningResult] =
    CompletableFuture.completedFuture(
      PruningResult.ParticipantPruned
    )

  private val queue: BoundedSourceQueue[Submission] = {
    val (queue, queueSource) =
      InstrumentedGraph
        .queue[Submission](
          bufferSize = submissionBufferSize,
          capacityCounter = bridgeMetrics.BridgeInputQueue.conflictQueueCapacity,
          lengthCounter = bridgeMetrics.BridgeInputQueue.conflictQueueLength,
          delayTimer = bridgeMetrics.BridgeInputQueue.conflictQueueDelay,
        )
        .via(ledgerBridge.flow)
        .preMaterialize()

    queueSource.runWith(feedSink)
    logger.info(
      s"Write service initialized. Configuration: [submissionBufferSize: $submissionBufferSize]"
    )
    queue
  }

  private def submit(submission: Submission): CompletionStage[SubmissionResult] =
    toSubmissionResult(submission.submissionId, queue.offer(submission))

  private def validateDeduplicationDurationAndSubmit(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      estimatedInterpretationCost: Long,
      deduplicationDuration: Duration,
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  )(implicit errorLogger: ContextualizedErrorLogger): CompletionStage[SubmissionResult] = {
    val maxDeduplicationDuration = submitterInfo.ledgerConfiguration.maxDeduplicationDuration
    if (deduplicationDuration.compareTo(maxDeduplicationDuration) > 0)
      CompletableFuture.completedFuture(
        SubmissionResult.SynchronousError(
          Rejection
            .MaxDeduplicationDurationExceeded(
              deduplicationDuration,
              maxDeduplicationDuration,
              submitterInfo.toCompletionInfo(),
            )
            .toStatus
        )
      )
    else
      submit(
        Submission.Transaction(
          submitterInfo = submitterInfo,
          transactionMeta = transactionMeta,
          transaction = transaction,
          estimatedInterpretationCost = estimatedInterpretationCost,
          processedDisclosedContracts = processedDisclosedContracts,
        )
      )
  }
}

object BridgeWriteService {
  private[this] val logger = ContextualizedLogger.get(getClass)

  def toSubmissionResult(
      submissionId: Ref.SubmissionId,
      queueOfferResult: QueueOfferResult,
  )(implicit
      loggingContext: LoggingContext
  ): CompletableFuture[SubmissionResult] = {
    implicit val errorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, Some(submissionId))

    CompletableFuture.completedFuture(
      queueOfferResult match {
        case QueueOfferResult.Enqueued => SubmissionResult.Acknowledged
        case QueueOfferResult.Dropped =>
          SubmissionResult.SynchronousError(
            LedgerApiErrors.ParticipantBackpressure
              .Rejection("Sandbox-on-X ledger bridge submission buffer is full")
              .rpcStatus()
          )
        case QueueOfferResult.Failure(throwable) =>
          SubmissionResult.SynchronousError(
            LedgerApiErrors.InternalError
              .Generic(
                message = s"Failed to enqueue submission in the Sandbox-on-X ledger bridge",
                throwableO = Some(throwable),
              )
              .rpcStatus()
          )
        case QueueOfferResult.QueueClosed =>
          SubmissionResult.SynchronousError(
            CommonErrors.ServiceNotRunning
              .Reject("Sandbox-on-X ledger bridge")
              .rpcStatus()
          )
      }
    )
  }
}