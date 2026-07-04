// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{ParticipantTransactionView, ViewParticipantData, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.ExecutionContext

/** Checks and re-validates the external-call results of a transaction request, as one of the
  * parallel validation suites invoked from `TransactionProcessingSteps.doParallelChecks`.
  *
  * External-call verdicts derive from three sources:
  *   - disagreements between the results recorded across the views of the transaction, computed by
  *     [[ExternalCallConsistencyChecker]] (see
  *     [[ExternalCallResponseRouter.Result.recordedConsistency]]),
  *   - disagreements within the replay data of a single reinterpretation, that is, of a view
  *     together with its subviews, surfaced as
  *     [[com.digitalasset.canton.participant.util.DAMLe.ExternalCallRecordedResultDisagreement]]
  *     model-conformance errors (see [[ExternalCallResponseRouter.Result.inconsistenciesForView]]),
  *   - re-validation of undisputed recorded results against the extension service (see
  *     `validationOccurrences` and `validateExternalCalls`).
  *
  * Intended data flow for a request: `doParallelChecks` invokes [[check]] without awaiting it, so
  * that it runs concurrently with the model conformance and internal consistency checks (following
  * the pattern of `ModelConformanceChecker.check`); the returned
  * [[ExternalCallResponseRouter.Result]] travels to `TransactionConfirmationResponsesFactory`
  * through the transaction validation result, and the factory translates it -- without further
  * validation work -- into per-view, per-party verdicts: rejections for the disagreements a hosted
  * confirming party checks, rejections and abstentions from re-validation on views that would
  * otherwise be approved, and the view's own verdict for the remaining parties.
  *
  * Stateless helpers that do not need the validator live on the companion object.
  *
  * @param participantId
  *   The participant running the validation, used to resolve the hosted confirming parties.
  * @param externalCallValidator
  *   The validator used to re-run external calls during validation.
  * @param externalCallValidationParallelism
  *   Bounds the number of concurrent validator calls in `validateExternalCalls`.
  */
class ExternalCallResponseRouter(
    participantId: ParticipantId,
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import ExternalCallResponseRouter.*

  /** Checks the external-call results recorded in the received views and re-validates them against
    * the extension service. The check consists of:
    *   - resolving the confirming parties of every view that this participant hosts,
    *   - checking the consistency of the recorded results across all views
    *     ([[ExternalCallConsistencyChecker.check]]) and alarming on every visible disagreement,
    *     including those that affect no hosted party,
    *   - re-validating the recorded results that a hosted confirming party checks and is not
    *     already rejecting, where the selected occurrences agree on the output
    *     (`validationOccurrences`, `validateExternalCalls`). The network I/O is kicked off without
    *     awaiting the model-conformance result, so it can run concurrently with the
    *     reinterpretation,
    *   - routing the replay disagreements surfaced by the model-conformance errors to the hosted
    *     confirming parties that check a matching occurrence
    *     (`recordedExternalCallDisagreementInconsistencies`).
    *
    * The returned [[ExternalCallResponseRouter.Result]] completes once all of the above have; it is
    * pure data and translated into verdicts by `TransactionConfirmationResponsesFactory`.
    *
    * If no view records an external-call result -- in particular, always, on protocol versions
    * without external-call support -- the check short-circuits to
    * [[ExternalCallResponseRouter.Result.empty]] without topology lookups: replay disagreements
    * carry outputs recorded in the views, so none can exist without recorded results either.
    *
    * @param requestId
    *   The request under validation, used to correlate logs and alarms.
    * @param views
    *   All views of the request received by this participant, by position.
    * @param topologySnapshot
    *   Used to resolve which confirming parties of each view this participant hosts.
    * @param conformanceErrorsF
    *   The non-abort errors of the model conformance check; awaited only for the replay-routing
    *   step, after the re-validation has been kicked off.
    * @param runValidation
    *   Whether to re-validate undisputed recorded results. False for requests with malformed
    *   payloads, which are rejected wholesale: only the alarms are of interest there.
    */
  def check(
      requestId: RequestId,
      views: Map[ViewPosition, ParticipantTransactionView],
      topologySnapshot: TopologySnapshot,
      conformanceErrorsF: FutureUnlessShutdown[Seq[ModelConformanceChecker.Error]],
      runValidation: Boolean,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Result] = {
    val hasAnyVisibleExternalCallResults =
      views.valuesIterator.exists(_.viewParticipantData.externalCallResults.nonEmpty)

    if (!hasAnyVisibleExternalCallResults)
      FutureUnlessShutdown.pure(Result.empty)
    else {
      val viewsWithHostedPartiesF =
        views.toSeq
          .sortBy { case (viewPosition, _) => viewPosition }(
            ViewPosition.orderViewPosition.toOrdering
          )
          .parTraverse { case (viewPosition, view) =>
            topologySnapshot
              .canConfirm(
                participantId,
                view.viewCommonData.viewConfirmationParameters.confirmers,
              )
              .map(hostedConfirmingParties =>
                ViewWithHostedParties(viewPosition, view, hostedConfirmingParties)
              )
          }

      for {
        viewsWithHostedParties <- viewsWithHostedPartiesF
        recordedConsistency = {
          val allHostedConfirmingParties =
            viewsWithHostedParties.flatMap(_.hostedConfirmingParties).toSet
          ExternalCallConsistencyChecker.check(views, allHostedConfirmingParties)
        }
        _ = reportVisibleRecordedDisagreementAlarms(requestId, recordedConsistency)
        // Kick off the re-validation before awaiting the model-conformance errors, so that the
        // network I/O runs concurrently with the reinterpretation.
        validationRoutesF =
          if (runValidation)
            validateExternalCalls(
              validationOccurrences(viewsWithHostedParties, recordedConsistency)
            )
          else FutureUnlessShutdown.pure(ExternalCallValidationRoutes.empty)
        conformanceErrors <- conformanceErrorsF
        replayDisagreementInconsistencies = recordedExternalCallDisagreementInconsistencies(
          conformanceErrors.flatMap(externalCallRecordedResultDisagreement),
          viewsWithHostedParties,
        )
        validationRoutes <- validationRoutesF
      } yield Result(recordedConsistency, replayDisagreementInconsistencies, validationRoutes)
    }
  }

  /** Selects the recorded external-call results to re-validate against the extension service.
    *
    * Every recorded result of every view is selected for the hosted confirming parties that check
    * it, excluding the parties that already reject the view because they check a disagreement among
    * the recorded results. Results with no such party yield no occurrence.
    *
    * Whether a view will be approved is not known at selection time (re-validation runs
    * concurrently with the other validation suites), so results of views that end up rejected may
    * be re-validated needlessly; the factory folds re-validation outcomes only into views that
    * would otherwise be approved.
    */
  private[validation] def validationOccurrences(
      viewsWithHostedParties: Seq[ViewWithHostedParties],
      recordedConsistency: ExternalCallConsistencyChecker.Result,
  ): Seq[ExternalCallValidationOccurrence] =
    viewsWithHostedParties
      .sortBy(_.viewPosition)(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { viewWithHostedParties =>
        val viewPosition = viewWithHostedParties.viewPosition
        val hostedConfirmingParties = viewWithHostedParties.hostedConfirmingParties

        val alreadyRejectedParties =
          recordedConsistency.hostedInconsistencies.collect {
            case (party, inconsistencies)
                if hostedConfirmingParties(party) &&
                  inconsistencies.exists(_.occurrences.exists(_.viewPosition == viewPosition)) =>
              party
          }.toSet

        viewWithHostedParties.view.viewParticipantData.externalCallResults.flatMap { result =>
          val affectedParties =
            result.checkingParties.intersect(hostedConfirmingParties) -- alreadyRejectedParties
          Option.when(affectedParties.nonEmpty)(
            ExternalCallValidationOccurrence(
              viewPosition,
              result,
              affectedParties,
            )
          )
        }
      }

  /** Re-validates the selected occurrences against the extension service and routes the verdicts to
    * parties.
    *
    * Occurrences are grouped by their semantic call
    * ([[com.digitalasset.canton.participant.util.DAMLe.ExternalCallKey]]). Only keys whose selected
    * occurrences all record the same output are re-validated: once per key, with the number of
    * concurrent validator calls bounded by `externalCallValidationParallelism`. Keys with
    * disagreeing recorded outputs are not re-validated; those disagreements are covered by the
    * consistency check (per-party rejections where a hosted party checks conflicting occurrences,
    * and visible-disagreement alarms in every case). Since the selection spans all views of the
    * request (not only those that end up approved), this skip is independent of the per-view
    * verdicts. It is however scoped to the selected occurrences: a conflicting occurrence that no
    * hosted, non-rejecting confirming party checks does not participate in the aggregation, so the
    * remaining side of such a disagreement is still re-validated for the parties that check it,
    * while the dropped side stays covered by the consistency check as above.
    *
    * Verdict routing, applied to every hosted checking party of every occurrence of the key:
    * [[ExternalCallValidator.Mismatched]] yields a rejection (an
    * [[ExternalCallConsistencyChecker.Inconsistency]] carrying the recorded and the computed
    * output), [[ExternalCallValidator.UnableToValidate]] yields an abstention with the given
    * reason, and [[ExternalCallValidator.Matched]] yields nothing. The per-party sequences of the
    * returned [[ExternalCallValidationRoutes]] are deduplicated and sorted, so the routes are
    * deterministic.
    */
  private[validation] def validateExternalCalls(
      occurrences: Seq[ExternalCallValidationOccurrence]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[ExternalCallValidationRoutes] = {
    val keyedOccurrences =
      occurrences.map(occurrence =>
        KeyedValidationOccurrence(
          DAMLe.ExternalCallKey.fromResult(occurrence.result.result),
          occurrence,
        )
      )

    val outputByKey =
      keyedOccurrences
        .groupMap(_.key)(_.output)
        .view
        .mapValues(_.toSet)
        .toMap

    val keysWithSingleOutput =
      outputByKey.toSeq
        .flatMap { case (key, outputs) =>
          outputs.toList match {
            case recordedOutput :: Nil => Some(key -> recordedOutput)
            case _ => None
          }
        }
        .sortBy { case (key, _) => key }

    MonadUtil
      .parTraverseWithLimit(externalCallValidationParallelism)(keysWithSingleOutput) {
        case (key, recordedOutput) =>
          externalCallValidator.validate(key, recordedOutput).map(key -> _)
      }
      .map { validationResults =>
        val resultsByKey = validationResults.toMap
        ExternalCallValidationRoutes(
          rejects = rejectsFrom(keyedOccurrences, resultsByKey),
          abstains = abstainsFrom(keyedOccurrences, resultsByKey),
        )
      }
  }

  /** Emits one [[ExternalCallValidationError.ExternalCallResultDisagreementAlarm]] per disagreement
    * among the recorded results visible to this participant, independently of whether a hosted
    * party is affected.
    */
  private def reportVisibleRecordedDisagreementAlarms(
      requestId: RequestId,
      recordedConsistency: ExternalCallConsistencyChecker.Result,
  )(implicit traceContext: TraceContext): Unit =
    recordedConsistency.visibleInconsistencies.foreach { inconsistency =>
      ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        .Warn(s"Observed inconsistent external call results: ${inconsistency.description}")
        .logWithContext(Map("requestId" -> requestId.toString))
    }
}

object ExternalCallResponseRouter {

  /** A received view together with the confirming parties of the view that this participant hosts.
    */
  private[validation] final case class ViewWithHostedParties(
      viewPosition: ViewPosition,
      view: ParticipantTransactionView,
      hostedConfirmingParties: Set[LfPartyId],
  )

  /** A recorded external-call result selected for re-validation, together with the hosted checking
    * parties whose approval of the view depends on it.
    */
  private[validation] final case class ExternalCallValidationOccurrence(
      viewPosition: ViewPosition,
      result: ViewParticipantData.ViewExternalCallResult,
      hostedCheckingParties: Set[LfPartyId],
  )

  /** An abstention from confirming a view because a recorded external-call result could not be
    * re-validated.
    */
  final case class ExternalCallValidationAbstain(
      viewPosition: ViewPosition,
      reason: String,
  )

  /** Per-party outcome of re-validating recorded external-call results: `rejects` holds the
    * re-validation mismatches (as inconsistencies carrying the recorded and the computed output),
    * `abstains` holds the per-view abstentions where a result could not be re-validated.
    */
  final case class ExternalCallValidationRoutes(
      rejects: Map[LfPartyId, Seq[Inconsistency]],
      abstains: Map[LfPartyId, Seq[ExternalCallValidationAbstain]],
  ) {

    /** Yields for every party in `nonRejectingConfirmingParties` an inconsistency in
      * `rejects(party)` that has the given `viewPosition` (provided it exists). Picks the smallest
      * inconsistency per party by `ExternalCallConsistencyChecker.orderInconsistency` so that the
      * result is deterministic.
      *
      * @param nonRejectingConfirmingParties
      *   The hosted confirming parties that are not already rejecting for this view (rejections for
      *   disagreements take precedence over re-validation outcomes, and at most one external-call
      *   rejection is routed per party and view).
      */
    def rejectsForView(
        viewPosition: ViewPosition,
        nonRejectingConfirmingParties: Set[LfPartyId],
    ): Seq[(LfPartyId, Inconsistency)] =
      rejects.toSeq.sortBy { case (party, _) => party }.flatMap {
        case (party, inconsistencies) if nonRejectingConfirmingParties(party) =>
          inconsistencies
            .filter(_.occurrences.exists(_.viewPosition == viewPosition))
            .minByOption(identity)(ExternalCallConsistencyChecker.orderInconsistency)
            .map(party -> _)
        case _ => None
      }

    /** Yields for every party in `nonRejectingConfirmingParties` the abstain reason in
      * `abstains(party)` recorded for the given `viewPosition` (provided it exists). Picks the
      * smallest reason per party so that the result is deterministic.
      *
      * @param nonRejectingConfirmingParties
      *   The hosted confirming parties that are not already rejecting for this view.
      */
    def abstainsForView(
        viewPosition: ViewPosition,
        nonRejectingConfirmingParties: Set[LfPartyId],
    ): Seq[(LfPartyId, String)] =
      abstains.toSeq.sortBy { case (party, _) => party }.flatMap {
        case (party, abstains) if nonRejectingConfirmingParties(party) =>
          abstains
            .filter(_.viewPosition == viewPosition)
            .minByOption(_.reason)
            .map(abstain => party -> abstain.reason)
        case _ => None
      }
  }

  object ExternalCallValidationRoutes {
    val empty: ExternalCallValidationRoutes = ExternalCallValidationRoutes(Map.empty, Map.empty)
  }

  /** Precomputed external-call validation outcome of a request, produced by `check` and consumed by
    * `TransactionConfirmationResponsesFactory` as pure data.
    *
    * Aggregates the two disagreement sources of a request, namely the results recorded across the
    * received views (checked by [[ExternalCallConsistencyChecker]]) and the disagreements raised
    * during replay reinterpretation, together with the outcome of re-validating the undisputed
    * results against the extension service.
    */
  final case class Result(
      recordedConsistency: ExternalCallConsistencyChecker.Result,
      replayDisagreementInconsistencies: Map[
        LfPartyId,
        Seq[(DAMLe.ExternalCallRecordedResultDisagreement, Inconsistency)],
      ],
      validationRoutes: ExternalCallValidationRoutes,
  ) {

    private lazy val routableReplayDisagreements
        : Set[DAMLe.ExternalCallRecordedResultDisagreement] =
      replayDisagreementInconsistencies.valuesIterator
        .flatMap(_.iterator)
        .map { case (disagreement, _) => disagreement }
        .toSet

    /** Whether `error` is an external-call replay disagreement for which some hosted confirming
      * party checks a matching occurrence. Such an error is expected to suppress the generic
      * (malformed) model-conformance rejection of the request; in its place, the hosted confirming
      * parties that check a matching occurrence reject per view with the disagreement inconsistency
      * provided by [[inconsistenciesForView]]. Note that the routed rejections cover at most as
      * many parties as the suppressed blanket rejection, and in general fewer: views without a
      * matching occurrence and confirming parties that do not check the result are not rejected
      * through this path.
      */
    def isRoutableModelConformanceError(error: ModelConformanceChecker.Error): Boolean =
      externalCallRecordedResultDisagreement(error).exists(routableReplayDisagreements)

    /** Yields for every party in `hostedConfirmingParties` at most one inconsistency among the
      * disagreeing external-call results that the party checks in the given view: from the results
      * recorded across views where the party is affected there, otherwise from the replay
      * disagreements. Parties in the result are expected to reject the view with the returned
      * inconsistency.
      */
    def inconsistenciesForView(
        viewPosition: ViewPosition,
        hostedConfirmingParties: Set[LfPartyId],
    ): Seq[(LfPartyId, Inconsistency)] = {
      val recordedResultInconsistencies =
        recordedConsistency.hostedInconsistencies.toSeq.flatMap {
          case (party, inconsistencies) if hostedConfirmingParties(party) =>
            inconsistencies
              .find(_.occurrences.exists(_.viewPosition == viewPosition))
              .map(party -> _)
          case _ => None
        }

      val partiesWithRecordedResultInconsistencies =
        recordedResultInconsistencies.map(_._1).toSet

      val recordedReplayInconsistencies =
        replayDisagreementInconsistencies.toSeq.flatMap {
          case (party, disagreementInconsistencies)
              if hostedConfirmingParties(party) &&
                !partiesWithRecordedResultInconsistencies(party) =>
            disagreementInconsistencies
              .find(_._2.occurrences.exists(_.viewPosition == viewPosition))
              .map { case (_, inconsistency) => party -> inconsistency }
          case _ => None
        }

      recordedResultInconsistencies ++
        recordedReplayInconsistencies
    }
  }

  object Result {
    val empty: Result = Result(
      ExternalCallConsistencyChecker.Result.empty,
      Map.empty,
      ExternalCallValidationRoutes.empty,
    )
  }

  private val orderRecordedResultDisagreement
      : Ordering[DAMLe.ExternalCallRecordedResultDisagreement] =
    Ordering
      .by[DAMLe.ExternalCallRecordedResultDisagreement, DAMLe.ExternalCallKey](_.key)
      .orElseBy(_.outputs)(ExternalCallConsistencyChecker.orderOutputSets)

  private val orderExternalCallValidationAbstain: Ordering[ExternalCallValidationAbstain] =
    Ordering
      .by[ExternalCallValidationAbstain, ViewPosition](_.viewPosition)(
        ViewPosition.orderViewPosition.toOrdering
      )
      .orElseBy(_.reason)

  private def externalCallRecordedResultDisagreement(
      error: ModelConformanceChecker.Error
  ): Option[DAMLe.ExternalCallRecordedResultDisagreement] = error match {
    case ModelConformanceChecker.DAMLeError(
          disagreement: DAMLe.ExternalCallRecordedResultDisagreement,
          _,
        ) =>
      Some(disagreement)
    case _ => None
  }

  private def recordedExternalCallDisagreementInconsistencies(
      disagreements: Seq[DAMLe.ExternalCallRecordedResultDisagreement],
      viewsWithHostedParties: Seq[ViewWithHostedParties],
  ): Map[LfPartyId, Seq[(DAMLe.ExternalCallRecordedResultDisagreement, Inconsistency)]] = {
    val orderedDisagreements =
      disagreements.distinct.sorted(orderRecordedResultDisagreement)

    val orderedViews =
      viewsWithHostedParties.sortBy(_.viewPosition)(ViewPosition.orderViewPosition.toOrdering)

    val routed =
      orderedDisagreements.flatMap { disagreement =>
        occurrencesByParty(disagreement, orderedViews).toSeq.map { case (party, occurrences) =>
          party -> (disagreement -> Inconsistency(
            disagreement.key,
            disagreement.outputs,
            occurrences,
          ))
        }
      }

    routed.groupMap(_._1)(_._2)
  }

  /** For a single recorded disagreement, the occurrences of its external call that each hosted
    * confirming party can see across all views. A party sees an occurrence when it is a checking
    * party of a matching external-call result in a view it confirms.
    */
  private def occurrencesByParty(
      disagreement: DAMLe.ExternalCallRecordedResultDisagreement,
      orderedViews: Seq[ViewWithHostedParties],
  ): Map[LfPartyId, Set[ExternalCallOccurrence]] =
    orderedViews
      .flatMap { viewWithHostedParties =>
        viewWithHostedParties.view.viewParticipantData.externalCallResults
          .filter(result => matchesDisagreement(disagreement, result))
          .flatMap { result =>
            val occurrence = ExternalCallOccurrence(
              viewWithHostedParties.viewPosition,
              result.exerciseIndex,
              result.callIndex,
            )
            result.checkingParties
              .intersect(viewWithHostedParties.hostedConfirmingParties)
              .toSeq
              .map(_ -> occurrence)
          }
      }
      .groupMap(_._1)(_._2)
      .view
      .mapValues(_.toSet)
      .toMap

  /** Whether `result` records the same external call (and one of the disagreeing outputs) as
    * `disagreement`.
    */
  private def matchesDisagreement(
      disagreement: DAMLe.ExternalCallRecordedResultDisagreement,
      result: ViewParticipantData.ViewExternalCallResult,
  ): Boolean =
    DAMLe.ExternalCallKey.fromResult(result.result) == disagreement.key &&
      disagreement.outputs(result.result.output)

  /** An external-call occurrence paired with the key it re-validates under. */
  private final case class KeyedValidationOccurrence(
      key: DAMLe.ExternalCallKey,
      occurrence: ExternalCallValidationOccurrence,
  ) {
    def output: Bytes = occurrence.result.result.output
    def hostedCheckingParties: Set[LfPartyId] = occurrence.hostedCheckingParties
    def externalCallOccurrence: ExternalCallOccurrence = ExternalCallOccurrence(
      occurrence.viewPosition,
      occurrence.result.exerciseIndex,
      occurrence.result.callIndex,
    )
  }

  private final case class RejectRow(
      party: LfPartyId,
      key: DAMLe.ExternalCallKey,
      outputs: Set[Bytes],
      occurrence: ExternalCallOccurrence,
  )

  private final case class AbstainRow(
      party: LfPartyId,
      abstain: ExternalCallValidationAbstain,
  )

  /** Per-party rejections for occurrences whose re-validation produced a mismatching output. */
  private def rejectsFrom(
      keyedOccurrences: Seq[KeyedValidationOccurrence],
      resultsByKey: Map[DAMLe.ExternalCallKey, ExternalCallValidator.Result],
  ): Map[LfPartyId, Seq[Inconsistency]] = {
    val rejectRows = keyedOccurrences.flatMap { keyedOccurrence =>
      resultsByKey.get(keyedOccurrence.key).toList.flatMap {
        case ExternalCallValidator.Mismatched(computedOutput, recordedOutput) =>
          val outputs = Set(computedOutput, recordedOutput)
          keyedOccurrence.hostedCheckingParties.toSeq.sorted.map(party =>
            RejectRow(
              party,
              keyedOccurrence.key,
              outputs,
              keyedOccurrence.externalCallOccurrence,
            )
          )

        case _ =>
          Seq.empty
      }
    }

    rejectRows
      .groupMap(row => (row.party, row.key, row.outputs))(_.occurrence)
      .toSeq
      .groupMap { case ((party, _, _), _) => party } { case ((_, key, outputs), occurrences) =>
        Inconsistency(key, outputs, occurrences.toSet)
      }
      .view
      .mapValues(_.sorted(ExternalCallConsistencyChecker.orderInconsistency))
      .toMap
  }

  /** Per-party abstentions for occurrences whose re-validation could not be performed. */
  private def abstainsFrom(
      keyedOccurrences: Seq[KeyedValidationOccurrence],
      resultsByKey: Map[DAMLe.ExternalCallKey, ExternalCallValidator.Result],
  ): Map[LfPartyId, Seq[ExternalCallValidationAbstain]] = {
    val abstainRows = keyedOccurrences.flatMap { keyedOccurrence =>
      resultsByKey.get(keyedOccurrence.key).toList.flatMap {
        case ExternalCallValidator.UnableToValidate(reason) =>
          val abstain = ExternalCallValidationAbstain(
            keyedOccurrence.occurrence.viewPosition,
            reason,
          )
          keyedOccurrence.hostedCheckingParties.toSeq.sorted
            .map(party => AbstainRow(party, abstain))

        case _ =>
          Seq.empty
      }
    }

    abstainRows.distinct
      .groupMap(_.party)(_.abstain)
      .view
      .mapValues(_.sorted(orderExternalCallValidationAbstain))
      .toMap
  }
}
