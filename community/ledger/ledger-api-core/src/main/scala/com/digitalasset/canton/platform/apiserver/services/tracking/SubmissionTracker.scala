// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.digitalasset.canton.ledger.error.{
  CommonErrors,
  DamlContextualizedErrorLogger,
  LedgerApiErrors,
}
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.Submitters
import com.google.protobuf.empty.Empty

import java.time.Duration
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait SubmissionTracker extends AutoCloseable {
  def track(
      commands: Commands,
      timeout: Duration,
      submit: SubmitRequest => Future[Empty],
  )(implicit
      loggingContext: LoggingContext,
      errorLogger: ContextualizedErrorLogger,
  ): Future[CompletionResponse]

  /** [[com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse.completions]] do not have `act_as` populated,
    * hence submitters are propagated separately.
    * TODO(#12658): Use only the completion response once completions.act_as is populated.
    */
  def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit
}

object SubmissionTracker {
  type Submitters = Set[String]

  def owner(maxCommandsInFlight: Int, metrics: Metrics): ResourceOwner[SubmissionTracker] =
    for {
      cancellableTimeoutSupport <- CancellableTimeoutSupport.owner(
        "submission-tracker-timeout-timer"
      )
      tracker <- ResourceOwner.forCloseable(() =>
        new SubmissionTrackerImpl(cancellableTimeoutSupport, maxCommandsInFlight, metrics)
      )
    } yield tracker

  private[tracking] class SubmissionTrackerImpl(
      cancellableTimeoutSupport: CancellableTimeoutSupport,
      maxCommandsInFlight: Int,
      metrics: Metrics,
  ) extends SubmissionTracker {
    private[tracking] val pending =
      TrieMap.empty[SubmissionKey, Promise[CompletionResponse]]
    private val errorLogger = DamlContextualizedErrorLogger.forClass(getClass)

    // Set max-in-flight capacity
    metrics.daml.commands.maxInFlightCapacity.inc(maxCommandsInFlight.toLong)(MetricsContext.Empty)

    override def track(
        commands: Commands,
        timeout: Duration,
        submit: SubmitRequest => Future[Empty],
    )(implicit
        loggingContext: LoggingContext,
        errorLogger: ContextualizedErrorLogger,
    ): Future[CompletionResponse] =
      ensuringSubmissionIdPopulated(commands) {
        ensuringMaximumInFlight {
          val parties = CommandsValidator.effectiveActAs(commands)
          val submissionKey = SubmissionKey(
            commandId = commands.commandId,
            submissionId = commands.submissionId,
            applicationId = commands.applicationId,
            parties = parties,
          )

          val promise = Promise[CompletionResponse]()
          pending.putIfAbsent(submissionKey, promise) match {
            case Some(_) =>
              promise.complete(CompletionResponse.duplicate(submissionKey.submissionId))

            case None =>
              // Start the timeout timer before submit to ensure that the timer scheduling
              // happens before its cancellation (on submission failure OR onCompletion)
              val cancelTimeout = cancellableTimeoutSupport.scheduleOnce(
                duration = timeout,
                promise = promise,
                onTimeout =
                  CompletionResponse.timeout(submissionKey.commandId, submissionKey.submissionId),
              )

              submit(SubmitRequest(Some(commands)))
                .onComplete {
                  case Success(_) => // succeeded, nothing to do
                  case Failure(throwable) =>
                    // Submitting command failed, finishing entry with the very same error
                    promise.tryComplete(Failure(throwable))
                }(ExecutionContext.parasitic)

              promise.future.onComplete { _ =>
                // register timeout cancellation and removal from map
                cancelTimeout.close()
                pending.remove(submissionKey)
              }(ExecutionContext.parasitic)
          }
          promise.future
        }
      }

    override def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit = {
      val (completionStreamResponse, submitters) = completionResult
      completionStreamResponse.completions.foreach { completion =>
        attemptFinish(SubmissionKey.fromCompletion(completion, submitters))(
          CompletionResponse.fromCompletion(completion, completionStreamResponse.checkpoint)
        )
      }
    }

    override def close(): Unit = {
      pending.values.foreach(_.complete(CompletionResponse.closing(errorLogger)))
    }

    private def attemptFinish(submissionKey: SubmissionKey)(
        result: => Try[CompletionResponse]
    ): Unit =
      pending.get(submissionKey).foreach(_.complete(result))

    private def ensuringMaximumInFlight[T](
        f: => Future[T]
    )(implicit errorLogger: ContextualizedErrorLogger): Future[T] =
      if (pending.size < maxCommandsInFlight) {
        metrics.daml.commands.maxInFlightLength.inc()
        val ret = f
        ret.onComplete { _ =>
          metrics.daml.commands.maxInFlightLength.dec()
        }(ExecutionContext.parasitic)
        ret
      } else {
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Maximum number of commands in-flight reached")
            .asGrpcError
        )
      }

    private def ensuringSubmissionIdPopulated[T](commands: Commands)(f: => Future[T])(implicit
        errorLogger: ContextualizedErrorLogger
    ): Future[T] =
      // We need submissionId for tracking submissions
      if (commands.submissionId.isEmpty) {
        Future.failed(
          CommonErrors.ServiceInternalError
            .Generic("Missing submission id in submission tracker")
            .asGrpcError
        )
      } else {
        f
      }

    private[tracking] case class SubmissionKey(
        commandId: String,
        submissionId: String,
        applicationId: String,
        parties: Set[String],
    )

    private object SubmissionKey {
      def fromCompletion(
          completion: com.daml.ledger.api.v1.completion.Completion,
          submitters: Submitters,
      ): SubmissionKey =
        SubmissionKey(
          commandId = completion.commandId,
          submissionId = completion.submissionId,
          applicationId = completion.applicationId,
          parties = submitters,
        )
    }
  }
}
