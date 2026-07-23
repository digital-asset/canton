// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.functor.*
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
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.concurrent.ExecutionContext

/** Checks the external-call results recorded in the views of a transaction request, as one of the
  * parallel validation suites invoked from `TransactionProcessingSteps.doParallelChecks`. See
  * [[check]] for the semantics.
  *
  * @param participantId
  *   This participant, for deciding which checking parties it hosts.
  * @param externalCallValidator
  *   The validator used to re-run external calls against the extension service.
  * @param externalCallValidationParallelism
  *   Bounds the number of concurrent validator calls.
  */
class ExternalCallCheck(
    participantId: ParticipantId,
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import ExternalCallCheck.*

  /** Checks the recorded external-call results, in two parts:
    *   - consistency: whether occurrences of the same external call recorded across the request
    *     agree on their output ([[ExternalCallConsistencyChecker]]),
    *   - re-validation: whether the (undisputed) recorded outputs agree with the extension service,
    *     re-executing each distinct call once ([[ExternalCallValidator]]).
    *
    * The outcome is one result per view that records external-call results: a disagreement or
    * re-validation failure rejects exactly the views whose participant data records the affected
    * call, and a recorded result that cannot be re-validated abstains those views instead of
    * approving them (see [[TransactionConfirmationResponsesFactory]], which consults only the
    * result of the view it responds for). Views without external-call results have no entry in the
    * returned map and need no verdict; if no view records any result -- in particular, always, on
    * protocol versions without external-call support -- the map is empty. Disagreements among the
    * recorded results within a single view's subtree are rejected earlier, as a malformed view when
    * the view is validated, independently of this check.
    *
    * Re-validation is restricted to the calls this participant is responsible for: a call is
    * re-validated only if the participant hosts one of its checking parties as a confirming party
    * of a view recording the call. The checking parties of a call are confirming parties of the
    * recording view whose confirmation is required, so every call is re-validated by the
    * participants hosting its checking parties; re-validating it elsewhere would only invoke the
    * extension service needlessly. Keys whose visible occurrences disagree have no unambiguous
    * output and are not re-validated either, so a disagreement on one call does not mask a
    * re-validation failure of another.
    *
    * Every disagreement found by either part is alarmed, once per request.
    *
    * @param requestId
    *   The request under validation, used to correlate logs and alarms.
    * @param views
    *   All views of the request received by this participant, by position.
    * @param topologySnapshot
    *   The topology snapshot used for the request, for deciding which confirming parties this
    *   participant hosts. Must be the same snapshot the confirmation responses are computed with,
    *   so that the re-validation restriction and the responses cannot diverge.
    * @param runValidation
    *   Whether to re-validate recorded results. False for requests with malformed payloads, which
    *   are rejected wholesale: only the alarms are of interest there.
    */
  def check(
      requestId: RequestId,
      views: Map[ViewPosition, ParticipantTransactionView],
      topologySnapshot: TopologySnapshot,
      runValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Map[ViewPosition, Result]] = {
    val recordedResults = views.toSeq
      .sortBy { case (viewPosition, _) => viewPosition }(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { case (viewPosition, view) =>
        view.viewParticipantData.externalCallResults.map(viewPosition -> _)
      }

    if (recordedResults.isEmpty)
      FutureUnlessShutdown.pure(Map.empty)
    else {
      val inconsistencies = ExternalCallConsistencyChecker.check(views)

      // Every visible inconsistency is alarmed here, once per request; the per-view results
      // reject each affected view with the first (in the checker's deterministic order)
      // inconsistency among the calls recorded in that view.
      inconsistencies.foreach(alarmDisagreement(requestId, _))

      val disagreeingKeys = inconsistencies.map(_.key).toSet
      val validatableResults = recordedResults.filter { case (_, result) =>
        !disagreeingKeys.contains(ExternalCallKey.fromResult(result.result))
      }

      val revalidationF =
        if (!runValidation || validatableResults.isEmpty)
          FutureUnlessShutdown.pure(Seq.empty[(ExternalCallKey, KeyOutcome)])
        else
          keysToRevalidate(views, validatableResults, topologySnapshot).flatMap { gatedKeys =>
            // The gate selects the keys to re-validate; a resulting mismatch still rejects
            // every view recording the key, also where the gate did not pass.
            validateRecordedResults(
              requestId,
              validatableResults.filter { case (_, result) =>
                gatedKeys.contains(ExternalCallKey.fromResult(result.result))
              },
            )
          }

      revalidationF.map(keyOutcomes =>
        assemblePerViewResults(recordedResults, inconsistencies, keyOutcomes)
      )
    }
  }

  /** The keys this participant is responsible for re-validating: those with at least one occurrence
    * whose checking parties include a party this participant hosts as a confirming party of the
    * recording view. Hosting is decided with a single topology lookup over all candidate parties.
    */
  private def keysToRevalidate(
      views: Map[ViewPosition, ParticipantTransactionView],
      recordedResults: Seq[(ViewPosition, ViewParticipantData.ViewExternalCallResult)],
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Set[ExternalCallKey]] = {
    val resultsWithCandidateParties = recordedResults.map { case (viewPosition, result) =>
      val confirmers = views(viewPosition).viewCommonData.viewConfirmationParameters.confirmers
      (result, result.checkingParties.intersect(confirmers))
    }
    val allCandidateParties = resultsWithCandidateParties.flatMap { case (_, candidates) =>
      candidates
    }.toSet

    topologySnapshot.canConfirm(participantId, allCandidateParties).map { hostedParties =>
      resultsWithCandidateParties.collect {
        case (result, candidates) if candidates.exists(hostedParties) =>
          ExternalCallKey.fromResult(result.result)
      }.toSet
    }
  }

  /** Combines the per-key outcomes into one result per view that records external-call results.
    * Within a view, a disagreement takes precedence over a re-validation mismatch, which takes
    * precedence over an unvalidatable result; each verdict describes a call recorded in that view.
    * Outcomes are examined in key order, so every result is deterministic.
    */
  private def assemblePerViewResults(
      recordedResults: Seq[(ViewPosition, ViewParticipantData.ViewExternalCallResult)],
      inconsistencies: Seq[Inconsistency],
      keyOutcomes: Seq[(ExternalCallKey, KeyOutcome)],
  ): Map[ViewPosition, Result] = {
    val keysByView: Map[ViewPosition, Set[ExternalCallKey]] =
      recordedResults
        .groupMap { case (viewPosition, _) => viewPosition } { case (_, result) =>
          ExternalCallKey.fromResult(result.result)
        }
        .view
        .mapValues(_.toSet)
        .toMap

    val mismatches = keyOutcomes.collect { case (_, KeyOutcome.Mismatch(inconsistency)) =>
      inconsistency
    }
    val unvalidatable = keyOutcomes.collect { case (key, KeyOutcome.Unvalidatable(reason)) =>
      key -> reason
    }

    keysByView.fmap { viewKeys =>
      inconsistencies
        .find(inconsistency => viewKeys.contains(inconsistency.key))
        .orElse(mismatches.find(mismatch => viewKeys.contains(mismatch.key)))
        .map(inconsistency => Rejected(inconsistency.description))
        .orElse(unvalidatable.collectFirst {
          case (key, reason) if viewKeys.contains(key) => CannotValidate(reason)
        })
        .getOrElse(Passed)
    }
  }

  /** Re-validates the recorded results, one validator call per distinct semantic call
    * ([[com.digitalasset.canton.data.ExternalCallKey]]), returning the outcome per key in key
    * order. At this point every key has a single recorded output: keys with disagreeing outputs are
    * excluded from re-validation by the caller.
    */
  private def validateRecordedResults(
      requestId: RequestId,
      recordedResults: Seq[(ViewPosition, ViewParticipantData.ViewExternalCallResult)],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Seq[(ExternalCallKey, KeyOutcome)]] = {
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
        outcomes.map { case (key, occurrencesAndOutputs, recordedOutput, validated) =>
          val outcome = validated match {
            case mismatched: ExternalCallValidator.Mismatched =>
              val inconsistency = Inconsistency(
                key,
                Set(mismatched.computedOutput, recordedOutput),
                occurrencesAndOutputs.map { case (occurrence, _) => occurrence }.toSet,
              )
              // Mirrors the visible-inconsistency alarms in check: every disagreement with the
              // extension service is suspicious and alarmed once per request.
              alarmDisagreement(requestId, inconsistency)
              KeyOutcome.Mismatch(inconsistency)
            case ExternalCallValidator.UnableToValidate(reason) =>
              KeyOutcome.Unvalidatable(reason)
            case ExternalCallValidator.Matched =>
              KeyOutcome.Ok
          }
          key -> outcome
        }
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

  /** Per-view outcome of the external-call check, consumed by
    * `TransactionConfirmationResponsesFactory` as pure data when responding for that view.
    */
  sealed trait Result extends Product with Serializable

  /** The results recorded in the view agree with all visible occurrences of the same calls and with
    * the extension service.
    */
  case object Passed extends Result

  /** A result recorded in the view disagrees with another visible occurrence of the same call, or
    * with the extension service. The view is rejected; the description names a call recorded in
    * this view.
    */
  final case class Rejected(description: String) extends Result

  /** A result recorded in the view could not be re-validated (for example, no extension service is
    * configured). The view is abstained from instead of approved.
    */
  final case class CannotValidate(reason: String) extends Result

  /** Re-validation outcome of a single distinct call. */
  private sealed trait KeyOutcome extends Product with Serializable
  private object KeyOutcome {
    case object Ok extends KeyOutcome
    final case class Mismatch(inconsistency: Inconsistency) extends KeyOutcome
    final case class Unvalidatable(reason: String) extends KeyOutcome
  }
}
