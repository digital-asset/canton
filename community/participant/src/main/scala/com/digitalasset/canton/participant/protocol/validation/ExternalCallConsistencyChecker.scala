// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

/** Checks consistency of external call results across a transaction on a per-party basis.
  *
  * Two external calls are considered "equal" if they have the same (extensionId, functionId,
  * configHash, inputHex). For each party P that is a signatory of contracts being exercised,
  * this checker verifies that all equal external calls visible to P return the same output.
  *
  * This is required because the same external call can appear multiple times in a transaction
  * with different signatory sets. Each party must independently verify consistency of all
  * calls they are responsible for validating.
  *
  * @see [[https://github.com/digital-asset/canton/discussions/XXX Design Discussion]]
  */
object ExternalCallConsistencyChecker {

  /** Equality key for external calls.
    *
    * Two external calls with the same key are considered "the same call" and must return
    * identical results for consistency.
    */
  final case class ExternalCallKey(
      extensionId: String,
      functionId: String,
      configHash: String,
      inputHex: String,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ExternalCallKey] = prettyOfClass(
      param("extensionId", _.extensionId.unquoted),
      param("functionId", _.functionId.unquoted),
      param("configHash", _.configHash.unquoted),
      param("inputHex", _.inputHex.unquoted),
    )
  }

  /** An external call occurrence with its execution context.
    *
    * @param key The equality key identifying this call
    * @param outputHex The recorded output of this call
    * @param callIndex The index of this call within the exercise node
    * @param viewPosition The position of the view containing this call
    * @param signatories The signatories of the contract being exercised (the validating parties)
    */
  final case class ExternalCallWithContext(
      key: ExternalCallKey,
      outputHex: String,
      callIndex: Int,
      viewPosition: ViewPosition,
      signatories: Set[LfPartyId],
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ExternalCallWithContext] = prettyOfClass(
      param("key", _.key),
      param("outputHex", _.outputHex.unquoted),
      param("callIndex", _.callIndex),
      param("viewPosition", _.viewPosition),
      param("signatories", _.signatories),
    )
  }

  /** Result of consistency check for a single party. */
  sealed trait PartyConsistencyResult extends PrettyPrinting

  /** The party sees consistent external call results. */
  case object Consistent extends PartyConsistencyResult {
    override protected def pretty: Pretty[Consistent.type] = prettyOfObject[Consistent.type]
  }

  /** The party sees inconsistent external call results.
    *
    * @param key The external call key that has inconsistent results
    * @param outputs The different output values observed
    * @param viewPositions The view positions where the inconsistency was detected
    */
  final case class Inconsistent(
      key: ExternalCallKey,
      outputs: Set[String],
      viewPositions: Set[ViewPosition],
  ) extends PartyConsistencyResult {
    override protected def pretty: Pretty[Inconsistent] = prettyOfClass(
      param("key", _.key),
      param("outputs", _.outputs.mkString("[", ", ", "]").unquoted),
      param("viewPositions", _.viewPositions),
    )
  }

  /** Aggregated consistency results for all checked parties. */
  final case class ExternalCallConsistencyResults(
      results: Map[LfPartyId, PartyConsistencyResult]
  ) extends PrettyPrinting {

    /** Returns parties that have inconsistent results. */
    def inconsistentParties: Set[LfPartyId] = results.collect {
      case (party, _: Inconsistent) => party
    }.toSet

    /** Returns true if all parties have consistent results. */
    def allConsistent: Boolean = results.values.forall(_ == Consistent)

    override protected def pretty: Pretty[ExternalCallConsistencyResults] = prettyOfClass(
      param("results", _.results)
    )
  }

  object ExternalCallConsistencyResults {
    val empty: ExternalCallConsistencyResults = ExternalCallConsistencyResults(Map.empty)
  }
}

class ExternalCallConsistencyChecker {
  import ExternalCallConsistencyChecker.*

  /** Collects all external calls with their signatory context from view validation results.
    *
    * For each view containing an exercise action with external call results, extracts the calls
    * along with the signatories of the contract being exercised.
    *
    * @param viewValidationResults The validation results for all views in the transaction
    * @return All external calls with their signatory contexts
    */
  def collectExternalCalls(
      viewValidationResults: Map[ViewPosition, ViewValidationResult]
  ): Seq[ExternalCallWithContext] = {
    viewValidationResults.toSeq.flatMap { case (viewPosition, viewResult) =>
      val viewParticipantData = viewResult.view.viewParticipantData
      val actionDescription = viewParticipantData.actionDescription

      actionDescription match {
        case exercise: ExerciseActionDescription if exercise.externalCallResults.nonEmpty =>
          // Get signatories from the contract being exercised
          val signatories = viewParticipantData.coreInputs
            .get(exercise.inputContractId)
            .map(_.contract.signatories)
            .getOrElse(Set.empty)

          exercise.externalCallResults.toSeq.map { result =>
            ExternalCallWithContext(
              key = ExternalCallKey(
                extensionId = result.extensionId,
                functionId = result.functionId,
                configHash = result.configHash,
                inputHex = result.inputHex,
              ),
              outputHex = result.outputHex,
              callIndex = result.callIndex,
              viewPosition = viewPosition,
              signatories = signatories,
            )
          }
        case _ => Seq.empty
      }
    }
  }

  /** Checks consistency of external calls for each hosted confirming party.
    *
    * For each party in `hostedConfirmingParties`:
    * 1. Filters to external calls where this party is a signatory
    * 2. Groups those calls by their equality key
    * 3. For each group, verifies all outputs are identical
    * 4. Returns Inconsistent if any group has differing outputs
    *
    * @param allCalls All external calls collected from the transaction
    * @param hostedConfirmingParties The confirming parties hosted by this participant
    * @return Per-party consistency results
    */
  def checkConsistency(
      allCalls: Seq[ExternalCallWithContext],
      hostedConfirmingParties: Set[LfPartyId],
  ): ExternalCallConsistencyResults = {
    if (allCalls.isEmpty || hostedConfirmingParties.isEmpty) {
      ExternalCallConsistencyResults.empty
    } else {
      val results: Map[LfPartyId, PartyConsistencyResult] = hostedConfirmingParties.map { party =>
        // 1. Filter calls where this party is a signatory
        val callsForParty = allCalls.filter(_.signatories.contains(party))

        val result: PartyConsistencyResult = if (callsForParty.isEmpty) {
          Consistent
        } else {
          // 2. Group by equality key
          val groupedByKey = callsForParty.groupBy(_.key)

          // 3. For each group, check all outputs are identical
          val inconsistency: Option[Inconsistent] = groupedByKey.collectFirst {
            case (key, calls) if calls.map(_.outputHex).toSet.sizeIs > 1 =>
              Inconsistent(
                key = key,
                outputs = calls.map(_.outputHex).toSet,
                viewPositions = calls.map(_.viewPosition).toSet,
              )
          }

          // 4. Return result for this party
          inconsistency.getOrElse[PartyConsistencyResult](Consistent)
        }
        party -> result
      }.toMap

      ExternalCallConsistencyResults(results)
    }
  }
}
