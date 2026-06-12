// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.ExternalCallPayloadDescription.{
  byteCount,
  hexPayloadSize,
}
import com.digitalasset.daml.lf.data.Bytes

import scala.collection.mutable

/** Checks whether external-call results visible to hosted confirming parties agree.
  *
  * External-call results are replay data that participate in validation. If two visible occurrences
  * of the same external call record different outputs, a hosted confirming party must reject the
  * transaction locally instead of approving an ambiguous result.
  */
object ExternalCallConsistencyChecker {

  final case class ExternalCallOccurrence(
      viewPosition: ViewPosition,
      exerciseIndex: NonNegativeInt,
      callIndex: NonNegativeInt,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[ExternalCallOccurrence] = prettyOfClass(
      param("viewPosition", _.viewPosition),
      param("exerciseIndex", _.exerciseIndex),
      param("callIndex", _.callIndex),
    )
  }

  private val orderExternalCallOccurrence: Ordering[ExternalCallOccurrence] =
    Ordering
      .by[ExternalCallOccurrence, ViewPosition](_.viewPosition)(
        ViewPosition.orderViewPosition.toOrdering
      )
      .orElseBy(_.exerciseIndex.unwrap)
      .orElseBy(_.callIndex.unwrap)

  final case class Inconsistency(
      key: DAMLe.ExternalCallKey,
      outputs: Set[Bytes],
      occurrences: Set[ExternalCallOccurrence],
  ) extends PrettyPrinting {
    def description: String = toString

    private def outputByteSizes: Seq[Int] = outputs.toSeq.map(byteCount).sorted

    private def orderedOccurrences: Seq[ExternalCallOccurrence] =
      occurrences.toSeq.sorted(orderExternalCallOccurrence)

    override protected def pretty: Pretty[Inconsistency] =
      prettyOfClass(
        param("extensionId", _.key.extensionId.unquoted),
        param("functionId", _.key.functionId.unquoted),
        param("config", inconsistency => hexPayloadSize(inconsistency.key.config).unquoted),
        param("input", inconsistency => hexPayloadSize(inconsistency.key.input).unquoted),
        param("outputByteSizes", _.outputByteSizes),
        param("occurrences", _.orderedOccurrences),
      )
  }

  final case class Result(inconsistencies: Map[LfPartyId, Seq[Inconsistency]])
      extends PrettyPrinting {
    def inconsistentParties: Set[LfPartyId] = inconsistencies.keySet

    override protected def pretty: Pretty[Result] = prettyOfClass(
      param("inconsistencies", _.inconsistencies)
    )
  }

  object Result {
    val empty: Result = Result(Map.empty)
  }

  /** Returns per-party inconsistencies for hosted confirming parties that can see disagreeing
    * outputs for the same external call.
    */
  def check(
      viewValidationResults: Map[ViewPosition, ViewValidationResult],
      hostedConfirmingParties: Set[LfPartyId],
  ): Result =
    if (hostedConfirmingParties.isEmpty) Result.empty
    else {
      val externalCallViewResults =
        viewValidationResults.iterator
          .flatMap { case (viewPosition, viewValidationResult) =>
            val viewParticipantData = viewValidationResult.view.viewParticipantData
            if (
              viewParticipantData.supportsExternalCallResults &&
              viewParticipantData.externalCallResults.nonEmpty
            )
              Iterator.single(viewPosition -> viewParticipantData.externalCallResults)
            else Iterator.empty
          }
          .toSeq
          .sortBy(_._1)(ViewPosition.orderViewPosition.toOrdering)

      if (externalCallViewResults.isEmpty) Result.empty
      else {
        val outputsByPartyAndKey =
          mutable.LinkedHashMap.empty[
            LfPartyId,
            mutable.Map[
              DAMLe.ExternalCallKey,
              mutable.Map[Bytes, mutable.Set[ExternalCallOccurrence]],
            ],
          ]

        externalCallViewResults.foreach { case (viewPosition, externalCallResults) =>
          externalCallResults.foreach { externalCallResult =>
            val affectedHostedParties =
              externalCallResult.checkingParties.intersect(hostedConfirmingParties)
            if (affectedHostedParties.nonEmpty) {
              val key = DAMLe.ExternalCallKey.fromResult(externalCallResult.result)
              val output = externalCallResult.result.output
              val occurrence = ExternalCallOccurrence(
                viewPosition,
                externalCallResult.exerciseIndex,
                externalCallResult.callIndex,
              )

              affectedHostedParties.foreach { party =>
                val outputsByKey =
                  outputsByPartyAndKey.getOrElseUpdate(party, mutable.LinkedHashMap.empty)
                val occurrencesByOutput =
                  outputsByKey.getOrElseUpdate(key, mutable.LinkedHashMap.empty)
                val occurrences =
                  occurrencesByOutput.getOrElseUpdate(output, mutable.Set.empty)
                occurrences.add(occurrence).discard
              }
            }
          }
        }

        val inconsistencies = outputsByPartyAndKey.iterator.flatMap { case (party, outputsByKey) =>
          val partyInconsistencies = outputsByKey.iterator.collect {
            case (key, occurrencesByOutput) if occurrencesByOutput.sizeCompare(1) > 0 =>
              Inconsistency(
                key,
                occurrencesByOutput.keySet.toSet,
                occurrencesByOutput.valuesIterator.flatMap(_.iterator).toSet,
              )
          }.toSeq

          Option.when(partyInconsistencies.nonEmpty)(party -> partyInconsistencies)
        }.toMap

        Result(inconsistencies)
      }
    }
}
