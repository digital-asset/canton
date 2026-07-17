// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{
  ExternalCallKey,
  ParticipantTransactionView,
  ViewParticipantData,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.concurrent.ExecutionContext

/** Checks the external-call results recorded in the views of a transaction request, as one of the
  * parallel validation suites invoked from `TransactionProcessingSteps.doParallelChecks`.
  *
  * The check has two parts:
  *   - consistency: whether occurrences of the same external call recorded across the request agree
  *     on their output ([[ExternalCallConsistencyChecker]]),
  *   - re-validation: whether the (undisputed) recorded outputs agree with the extension service,
  *     re-executing each distinct call once ([[ExternalCallValidator]]).
  *
  * Every disagreement found by either part is alarmed. The outcome is a single per-request
  * `ExternalCallCheck.Result`: a disagreement from either part rejects the request on behalf of all
  * hosted confirming parties, and a recorded result that cannot be re-validated leads to an
  * abstention instead of an approval (see [[TransactionConfirmationResponsesFactory]]).
  * Disagreements among the recorded results within a single view's subtree are rejected earlier, as
  * a malformed view when the view is validated, independently of this check.
  *
  * @param externalCallValidator
  *   The validator used to re-run external calls against the extension service.
  * @param externalCallValidationParallelism
  *   Bounds the number of concurrent validator calls.
  */
class ExternalCallCheck(
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import ExternalCallCheck.*

  /** Checks consistency of the recorded external-call results and re-validates them against the
    * extension service.
    *
    * If no view records an external-call result -- in particular, always, on protocol versions
    * without external-call support -- the check short-circuits to `ExternalCallCheck.Passed`.
    *
    * @param requestId
    *   The request under validation, used to correlate logs and alarms.
    * @param views
    *   All views of the request received by this participant, by position.
    * @param runValidation
    *   Whether to re-validate recorded results. False for requests with malformed payloads, which
    *   are rejected wholesale: only the alarms are of interest there.
    */
  def check(
      requestId: RequestId,
      views: Map[ViewPosition, ParticipantTransactionView],
      runValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Result] = {
    val recordedResults = views.toSeq
      .sortBy { case (viewPosition, _) => viewPosition }(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { case (viewPosition, view) =>
        view.viewParticipantData.externalCallResults.map(viewPosition -> _)
      }

    if (recordedResults.isEmpty)
      FutureUnlessShutdown.pure(Passed)
    else {
      // Hosted parties are irrelevant for the request-level outcome: any visible disagreement
      // rejects the request, so only the party-independent visible inconsistencies are consulted.
      val consistency = ExternalCallConsistencyChecker.check(views, Set.empty)

      // Every visible inconsistency is alarmed: only the first one is propagated into the
      // rejection, so the confirmation-responses factory reports just that one.
      consistency.visibleInconsistencies.foreach(alarmDisagreement(requestId, _))

      // The checker sorts the inconsistencies, so the reported disagreement is deterministic.
      consistency.visibleInconsistencies.headOption match {
        case Some(inconsistency) =>
          FutureUnlessShutdown.pure(Rejected(inconsistency.description))
        case None if !runValidation =>
          FutureUnlessShutdown.pure(Passed)
        case None =>
          validateRecordedResults(requestId, recordedResults)
      }
    }
  }

  /** Re-validates the recorded results, one validator call per distinct semantic call
    * ([[com.digitalasset.canton.data.ExternalCallKey]]). At this point every key has a single
    * recorded output: a key with disagreeing outputs is a visible inconsistency and was rejected
    * before re-validation. Outcomes are examined in key order, so the result is deterministic.
    */
  private def validateRecordedResults(
      requestId: RequestId,
      recordedResults: Seq[(ViewPosition, ViewParticipantData.ViewExternalCallResult)],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Result] = {
    val occurrencesByKey = recordedResults
      .groupMap { case (_, result) => ExternalCallKey.fromResult(result.result) } {
        case (viewPosition, result) =>
          ExternalCallOccurrence(viewPosition, result.exerciseIndex, result.callIndex) ->
            result.result.output
      }
      .toSeq
      .sortBy { case (key, _) => key }

    MonadUtil
      .parTraverseWithLimit(externalCallValidationParallelism)(occurrencesByKey) {
        case (key, occurrencesAndOutputs) =>
          // All occurrences of the key record the same output (see the method contract).
          val recordedOutput = occurrencesAndOutputs.headOption
            .map { case (_, output) => output }
            .getOrElse(ErrorUtil.invalidState("a grouped key always has at least one occurrence"))
          externalCallValidator
            .validate(key, recordedOutput)
            .map(outcome => (key, occurrencesAndOutputs, recordedOutput, outcome))
      }
      .map { outcomes =>
        val mismatches = outcomes.collect {
          case (
                key,
                occurrencesAndOutputs,
                recordedOutput,
                validated: ExternalCallValidator.Mismatched,
              ) =>
            Inconsistency(
              key,
              Set(validated.computedOutput, recordedOutput),
              occurrencesAndOutputs.map { case (occurrence, _) => occurrence }.toSet,
            )
        }
        // Mirrors the visible-inconsistency alarms in check: every disagreement with the
        // extension service is suspicious, not only the first one.
        mismatches.foreach(alarmDisagreement(requestId, _))

        val rejectionO =
          mismatches.headOption.map(inconsistency => Rejected(inconsistency.description))
        val abstentionO = outcomes.collectFirst {
          case (_, _, _, ExternalCallValidator.UnableToValidate(reason)) =>
            CannotValidate(reason)
        }
        rejectionO.orElse(abstentionO).getOrElse(Passed)
      }
  }

  private def alarmDisagreement(requestId: RequestId, inconsistency: Inconsistency)(implicit
      traceContext: TraceContext
  ): Unit =
    ExternalCallValidationError.ExternalCallResultDisagreementAlarm
      .Warn(s"Observed inconsistent external call results: ${inconsistency.description}")
      .logWithContext(Map("requestId" -> requestId.toString))
}

object ExternalCallCheck {

  /** Per-request outcome of the external-call check, consumed by
    * `TransactionConfirmationResponsesFactory` as pure data.
    */
  sealed trait Result extends Product with Serializable

  /** All recorded results agree with each other and with the extension service (or there are none).
    */
  case object Passed extends Result

  /** Recorded results disagree with each other, or with the extension service. The request is
    * rejected on behalf of all hosted confirming parties.
    */
  final case class Rejected(description: String) extends Result

  /** A recorded result could not be re-validated (for example, no extension service is configured).
    * Views that would otherwise be approved are abstained from instead.
    */
  final case class CannotValidate(reason: String) extends Result
}
