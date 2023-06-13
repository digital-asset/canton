// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import com.digitalasset.canton.ledger.error.{CommonErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.Submitters
import com.digitalasset.canton.tracing.TraceContext
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
      errorLogger: ContextualizedErrorLogger
  ): Future[CompletionResponse]

  /** [[com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse.completions]] do not have `act_as` populated,
    * hence submitters are propagated separately.
    * TODO(#12658): Use only the completion response once completions.act_as is populated.
    */
  def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit
}

object SubmissionTracker {
  type Submitters = Set[String]

  def owner(maxCommandsInFlight: Int, metrics: Metrics, loggerFactory: NamedLoggerFactory)(implicit
      traceContext: TraceContext
  ): ResourceOwner[SubmissionTracker] =
    for {
      cancellableTimeoutSupport <- CancellableTimeoutSupport.owner(
        "submission-tracker-timeout-timer",
        loggerFactory,
      )
      tracker <- ResourceOwner.forCloseable(() =>
        new SubmissionTrackerImpl(
          cancellableTimeoutSupport,
          maxCommandsInFlight,
          metrics,
          loggerFactory,
        )
      )
    } yield tracker

  private[tracking] class SubmissionTrackerImpl(
      cancellableTimeoutSupport: CancellableTimeoutSupport,
      maxCommandsInFlight: Int,
      metrics: Metrics,
      val loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext)
      extends SubmissionTracker
      with NamedLogging {
    private[tracking] val pending =
      TrieMap.empty[SubmissionKey, Promise[CompletionResponse]]

    // Set max-in-flight capacity
    metrics.daml.commands.maxInFlightCapacity.inc(maxCommandsInFlight.toLong)(MetricsContext.Empty)

    // TODO(#13019) Replace parasitic with DirectExecutionContext
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    override def track(
        commands: Commands,
        timeout: Duration,
        submit: SubmitRequest => Future[Empty],
    )(implicit
        errorLogger: ContextualizedErrorLogger
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
              promise.complete(
                CompletionResponse.duplicate(submissionKey.submissionId)(errorLogger)
              )

            case None =>
              // Start the timeout timer before submit to ensure that the timer scheduling
              // happens before its cancellation (on submission failure OR onCompletion)
              val cancelTimeout = cancellableTimeoutSupport.scheduleOnce(
                duration = timeout,
                promise = promise,
                onTimeout =
                  CompletionResponse.timeout(submissionKey.commandId, submissionKey.submissionId)(
                    errorLogger
                  ),
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
        }(errorLogger)
      }(errorLogger)

    override def onCompletion(completionResult: (CompletionStreamResponse, Submitters)): Unit = {
      val (completionStreamResponse, submitters) = completionResult
      completionStreamResponse.completion.foreach { completion =>
        attemptFinish(SubmissionKey.fromCompletion(completion, submitters))(
          CompletionResponse.fromCompletion(completion, completionStreamResponse.checkpoint)
        )
      }
    }

    override def close(): Unit = {
      pending.values.foreach(_.complete(CompletionResponse.closing))
    }

    private def attemptFinish(submissionKey: SubmissionKey)(
        result: => Try[CompletionResponse]
    ): Unit =
      pending.get(submissionKey).foreach(_.complete(result))

    // TODO(#13019) Replace parasitic with DirectExecutionContext
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
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
            .Rejection("Maximum number of commands in-flight reached")(errorLogger)
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
            .Generic("Missing submission id in submission tracker")(errorLogger)
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
          completion: com.daml.ledger.api.v2.completion.Completion,
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
