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

private[validation] final case class ViewWithHostedParties(
    viewPosition: ViewPosition,
    validationResult: ViewValidationResult,
    hostedConfirmingParties: Set[LfPartyId],
)

private[validation] final case class ExternalCallValidationOccurrence(
    viewPosition: ViewPosition,
    result: ViewParticipantData.ViewExternalCallResult,
    hostedCheckingParties: Set[LfPartyId],
)

private[validation] final case class ExternalCallValidationAbstain(
    viewPosition: ViewPosition,
    reason: String,
)

private[validation] final case class ExternalCallValidationRoutes(
    rejects: Map[LfPartyId, Seq[Inconsistency]],
    abstains: Map[LfPartyId, Seq[ExternalCallValidationAbstain]],
) {
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

  def abstainsForView(
      viewPosition: ViewPosition,
      hostedConfirmingParties: Set[LfPartyId],
      rejectedParties: Set[LfPartyId],
  ): Seq[(LfPartyId, String)] =
    abstains.toSeq.sortBy { case (party, _) => party }.flatMap {
      case (party, abstains) if hostedConfirmingParties(party) && !rejectedParties(party) =>
        abstains
          .filter(_.viewPosition == viewPosition)
          .minByOption(_.reason)
          .map(abstain => party -> abstain.reason)
      case _ => None
    }
}

/** Per-request external-call routing state.
  *
  * Computes which hosted confirming parties must reject (or whose model-conformance rejections are
  * superseded) because of external-call result disagreements, both for results visible across views
  * and for disagreements surfaced during replay reinterpretation.
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

  private lazy val recordedReplayDisagreementInconsistencies =
    ExternalCallResponseRouter.recordedExternalCallDisagreementInconsistencies(
      recordedResultDisagreements,
      viewsWithHostedParties,
    )

  private lazy val routableRecordedExternalCallDisagreements =
    recordedReplayDisagreementInconsistencies.valuesIterator
      .flatMap(_.iterator)
      .map(_._1)
      .toSet

  lazy val recordedConsistencyResult: ExternalCallConsistencyChecker.Result =
    if (hasAnyVisibleExternalCallResults) {
      val allHostedConfirmingParties =
        viewsWithHostedParties.flatMap(_.hostedConfirmingParties).toSet
      ExternalCallConsistencyChecker.check(
        viewValidationResults,
        allHostedConfirmingParties,
      )
    } else ExternalCallConsistencyChecker.Result.empty

  def isRoutableModelConformanceError(error: ModelConformanceChecker.Error): Boolean =
    ExternalCallResponseRouter
      .externalCallRecordedResultDisagreement(error)
      .exists(routableRecordedExternalCallDisagreements)

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
      recordedReplayDisagreementInconsistencies.toSeq.flatMap {
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

/** Routes external-call result disagreements to per-view, per-party local verdicts.
  *
  * Holds the external-call validator used to re-run calls during validation. Stateless helpers that
  * do not need the validator live on the companion object.
  */
private[validation] class ExternalCallResponseRouter(
    externalCallValidator: ExternalCallValidator,
    externalCallValidationParallelism: PositiveInt,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  import ExternalCallResponseRouter.*
  import com.digitalasset.canton.util.ShowUtil.*

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

  def reportVisibleRecordedDisagreementAlarms(
      requestId: RequestId,
      recordedConsistencyResult: ExternalCallConsistencyChecker.Result,
  )(implicit traceContext: TraceContext): Unit =
    recordedConsistencyResult.visibleInconsistencies.foreach { inconsistency =>
      val details = inconsistency.description.limit(maxExternalCallDisagreementDetailsLength)
      ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        .Warn(s"Observed inconsistent external call results: $details")
        .logWithContext(Map("requestId" -> requestId.toString))
    }
}

private[validation] object ExternalCallResponseRouter {

  val maxExternalCallDisagreementDetailsLength = 1024

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
