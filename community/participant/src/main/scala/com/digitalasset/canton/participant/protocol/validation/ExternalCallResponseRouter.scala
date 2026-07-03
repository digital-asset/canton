// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{ViewParticipantData, ViewPosition}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Bytes

import scala.concurrent.ExecutionContext

/** A received view together with the confirming parties of the view that this participant hosts.
  */
private[validation] final case class ViewWithHostedParties(
    viewPosition: ViewPosition,
    validationResult: ViewValidationResult,
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
private[validation] final case class ExternalCallValidationAbstain(
    viewPosition: ViewPosition,
    reason: String,
)

/** Per-party outcome of re-validating recorded external-call results: `rejects` holds the
  * re-validation mismatches (as inconsistencies carrying the recorded and the computed output),
  * `abstains` holds the per-view abstentions where a result could not be re-validated.
  */
private[validation] final case class ExternalCallValidationRoutes(
    rejects: Map[LfPartyId, Seq[Inconsistency]],
    abstains: Map[LfPartyId, Seq[ExternalCallValidationAbstain]],
) {

  /** Yields for every party in `hostedConfirmingParties` an inconsistency in `rejects(party)` that
    * has the given `viewPosition` (provided it exists). Picks the smallest inconsistency per party
    * by [[ExternalCallConsistencyChecker.orderInconsistency]] so that the result is deterministic.
    */
  def rejectsForView(
      viewPosition: ViewPosition,
      hostedConfirmingParties: Set[LfPartyId],
  ): Seq[(LfPartyId, Inconsistency)] =
    rejects.toSeq.sortBy { case (party, _) => party }.flatMap {
      case (party, inconsistencies) if hostedConfirmingParties(party) =>
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

/** Per-request external-call routing state.
  *
  * Aggregates the two disagreement sources of a request, namely the results recorded across the
  * received views (checked by [[ExternalCallConsistencyChecker]]) and the disagreements raised
  * during replay reinterpretation, and computes which hosted confirming parties must reject because
  * of them (or which model-conformance rejections they suppress and partially replace, see
  * [[isRoutableModelConformanceError]]).
  */
private[validation] final class ExternalCallRoutingContext(
    recordedResultDisagreements: Seq[DAMLe.ExternalCallRecordedResultDisagreement],
    viewsWithHostedParties: Seq[ViewWithHostedParties],
    viewValidationResults: Map[ViewPosition, ViewValidationResult],
) {
  private lazy val hasAnyVisibleExternalCallResults: Boolean =
    viewValidationResults.valuesIterator.exists { viewValidationResult =>
      val viewParticipantData = viewValidationResult.view.viewParticipantData
      viewParticipantData.externalCallResults.nonEmpty
    }

  private lazy val recordedExternalCallDisagreementInconsistencies =
    ExternalCallResponseRouter.recordedExternalCallDisagreementInconsistencies(
      recordedResultDisagreements,
      viewsWithHostedParties,
    )

  private lazy val routableRecordedExternalCallDisagreements =
    recordedExternalCallDisagreementInconsistencies.valuesIterator
      .flatMap(_.iterator)
      .map(_._1)
      .toSet

  /** Consistency of the external-call results recorded across all received views, checked for the
    * union of the hosted confirming parties of all views. See
    * [[ExternalCallConsistencyChecker.Result]].
    */
  lazy val recordedConsistencyResult: ExternalCallConsistencyChecker.Result =
    if (hasAnyVisibleExternalCallResults) {
      val allHostedConfirmingParties =
        viewsWithHostedParties.flatMap(_.hostedConfirmingParties).toSet
      ExternalCallConsistencyChecker.check(
        viewValidationResults,
        allHostedConfirmingParties,
      )
    } else ExternalCallConsistencyChecker.Result.empty

  /** Whether `error` is an external-call replay disagreement for which some hosted confirming party
    * checks a matching occurrence. Such an error is expected to suppress the generic (malformed)
    * model-conformance rejection of the request; in its place, the hosted confirming parties that
    * check a matching occurrence reject per view with the disagreement inconsistency provided by
    * [[inconsistenciesForView]]. Note that the routed rejections cover at most as many parties as
    * the suppressed blanket rejection, and in general fewer: views without a matching occurrence
    * and confirming parties that do not check the result are not rejected through this path.
    */
  def isRoutableModelConformanceError(error: ModelConformanceChecker.Error): Boolean =
    ExternalCallResponseRouter
      .externalCallRecordedResultDisagreement(error)
      .exists(routableRecordedExternalCallDisagreements)

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
      recordedConsistencyResult.inconsistencies.toSeq.flatMap {
        case (party, inconsistencies) if hostedConfirmingParties(party) =>
          inconsistencies
            .find(_.occurrences.exists(_.viewPosition == viewPosition))
            .map(party -> _)
        case _ => None
      }

    val partiesWithRecordedResultInconsistencies =
      recordedResultInconsistencies.map(_._1).toSet

    val recordedReplayInconsistencies =
      recordedExternalCallDisagreementInconsistencies.toSeq.flatMap {
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

/** Routes external-call validation outcomes to per-view, per-party local verdicts.
  *
  * External-call verdicts derive from three sources:
  *   - disagreements between the results recorded across the views of the transaction, computed by
  *     [[ExternalCallConsistencyChecker]] (see
  *     [[ExternalCallRoutingContext.recordedConsistencyResult]]),
  *   - disagreements within the replay data of a single reinterpretation, that is, of a view
  *     together with its subviews, surfaced as
  *     [[com.digitalasset.canton.participant.util.DAMLe.ExternalCallRecordedResultDisagreement]]
  *     model-conformance errors (see [[ExternalCallRoutingContext.inconsistenciesForView]]),
  *   - re-validation of undisputed recorded results against the extension service (see
  *     [[validationOccurrences]] and [[validateExternalCalls]]).
  *
  * Intended data flow for a request: construct an [[ExternalCallRoutingContext]] from the view
  * validation results and the reinterpretation disagreements; alarm on all visible recorded
  * disagreements, including those that affect no hosted party
  * ([[reportVisibleRecordedDisagreementAlarms]]); obtain the per-party disagreement rejections of
  * every view ([[ExternalCallRoutingContext.inconsistenciesForView]]); for the views that would
  * otherwise be approved, select the candidate occurrences for re-validation
  * ([[validationOccurrences]]) and re-run them against the extension service
  * ([[validateExternalCalls]]). The resulting [[ExternalCallValidationRoutes]] are folded into the
  * per-view verdicts via [[ExternalCallValidationRoutes.rejectsForView]] and
  * [[ExternalCallValidationRoutes.abstainsForView]], with rejections taking precedence:
  * [[ExternalCallValidationRoutes.abstainsForView]] is fed only the parties that are not already
  * rejecting. Net effect per hosted confirming party: it rejects the disagreements and mismatches
  * among the occurrences it checks, abstains where re-validation was not possible, and approves
  * otherwise.
  *
  * Stateless helpers that do not need the validator live on the companion object.
  *
  * @param externalCallValidator
  *   The validator used to re-run external calls during validation.
  * @param externalCallValidationParallelism
  *   Bounds the number of concurrent validator calls in [[validateExternalCalls]].
  */
private[validation] class ExternalCallResponseRouter(
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import ExternalCallResponseRouter.*

  /** Selects the recorded external-call results that a view is expected to re-validate against the
    * extension service before its hosted parties approve it.
    *
    * For every view in `approvingViewPositions` (mapping the views that would otherwise be approved
    * to their approving hosted parties), every recorded result of the view is selected for the
    * parties that check it and would approve, excluding the parties that already reject the view
    * because of a disagreement ([[ExternalCallRoutingContext.inconsistenciesForView]]). Results
    * with no such party yield no occurrence.
    */
  def validationOccurrences(
      viewsWithHostedParties: Seq[ViewWithHostedParties],
      approvingViewPositions: Map[ViewPosition, Set[LfPartyId]],
      externalCallRouting: ExternalCallRoutingContext,
  ): Seq[ExternalCallValidationOccurrence] =
    viewsWithHostedParties
      .sortBy(_.viewPosition)(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { viewWithHostedParties =>
        approvingViewPositions.get(viewWithHostedParties.viewPosition).toList.flatMap {
          approvingParties =>
            val viewParticipantData =
              viewWithHostedParties.validationResult.view.viewParticipantData
            if (viewParticipantData.externalCallResults.isEmpty) Seq.empty
            else {
              val alreadyRejectedParties =
                externalCallRouting
                  .inconsistenciesForView(viewWithHostedParties.viewPosition, approvingParties)
                  .map(_._1)
                  .toSet

              viewParticipantData.externalCallResults.flatMap { result =>
                val affectedParties =
                  result.checkingParties.intersect(approvingParties) -- alreadyRejectedParties
                Option.when(affectedParties.nonEmpty)(
                  ExternalCallValidationOccurrence(
                    viewWithHostedParties.viewPosition,
                    result,
                    affectedParties,
                  )
                )
              }
            }
        }
      }

  /** Re-validates the selected occurrences against the extension service and routes the verdicts to
    * parties.
    *
    * Occurrences are grouped by their semantic call
    * ([[com.digitalasset.canton.participant.util.DAMLe.ExternalCallKey]]). Only keys whose selected
    * occurrences all record the same output are re-validated: once per key, with the number of
    * concurrent validator calls bounded by `externalCallValidationParallelism`. Keys with
    * disagreeing recorded outputs are not re-validated; those disagreements are covered by
    * [[ExternalCallRoutingContext]] (per-party rejections where a hosted party checks conflicting
    * occurrences, visible-disagreement alarms otherwise).
    *
    * Verdict routing, applied to every hosted checking party of every occurrence of the key:
    * [[ExternalCallValidator.Mismatched]] yields a rejection (an [[Inconsistency]] carrying the
    * recorded and the computed output), [[ExternalCallValidator.UnableToValidate]] yields an
    * abstention with the given reason, and [[ExternalCallValidator.Matched]] yields nothing. The
    * per-party sequences of the returned [[ExternalCallValidationRoutes]] are deduplicated and
    * sorted, so the routes are deterministic.
    */
  def validateExternalCalls(
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
  def reportVisibleRecordedDisagreementAlarms(
      requestId: RequestId,
      recordedConsistencyResult: ExternalCallConsistencyChecker.Result,
  )(implicit traceContext: TraceContext): Unit =
    recordedConsistencyResult.visibleInconsistencies.foreach { inconsistency =>
      ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        .Warn(s"Observed inconsistent external call results: ${inconsistency.description}")
        .logWithContext(Map("requestId" -> requestId.toString))
    }
}

private[validation] object ExternalCallResponseRouter {

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

  def externalCallRecordedResultDisagreement(
      error: ModelConformanceChecker.Error
  ): Option[DAMLe.ExternalCallRecordedResultDisagreement] = error match {
    case ModelConformanceChecker.DAMLeError(
          disagreement: DAMLe.ExternalCallRecordedResultDisagreement,
          _,
        ) =>
      Some(disagreement)
    case _ => None
  }

  def recordedExternalCallDisagreementInconsistencies(
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
      .flatMap { view =>
        view.validationResult.view.viewParticipantData.externalCallResults
          .filter(result => matchesDisagreement(disagreement, result))
          .flatMap { result =>
            val occurrence = ExternalCallOccurrence(
              view.viewPosition,
              result.exerciseIndex,
              result.callIndex,
            )
            result.checkingParties
              .intersect(view.hostedConfirmingParties)
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
