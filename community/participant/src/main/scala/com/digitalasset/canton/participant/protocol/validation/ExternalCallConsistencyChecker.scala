// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Order
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.ExternalCallPayloadDescription.{
  byteCount,
  hexPayloadSize,
}
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

/** Checks whether visible external-call results agree.
  *
  * External-call results are replay data that participate in validation. If two visible occurrences
  * of the same external call record different outputs, a hosted confirming party must reject the
  * transaction locally instead of approving an ambiguous result. Visible disagreements are also
  * retained independently of hosted-party routing so callers can report suspicious recorded data.
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

  final case class Result(
      inconsistencies: Map[LfPartyId, Seq[Inconsistency]],
      visibleInconsistencies: Seq[Inconsistency],
  ) extends PrettyPrinting {
    def inconsistentParties: Set[LfPartyId] = inconsistencies.keySet

    override protected def pretty: Pretty[Result] = prettyOfClass(
      param("inconsistencies", _.inconsistencies),
      param("visibleInconsistencies", _.visibleInconsistencies),
    )
  }

  object Result {
    val empty: Result = Result(Map.empty, Seq.empty)
  }

  private final case class VisibleExternalCallOccurrence(
      key: DAMLe.ExternalCallKey,
      output: Bytes,
      occurrence: ExternalCallOccurrence,
      checkingParties: Set[LfPartyId],
  )

  private implicit val orderBytes: Order[Bytes] =
    Order.by[Bytes, ByteString](_.toByteString)(ByteStringUtil.orderByteString)

  private def orderedOutputs(outputs: Set[Bytes]): List[Bytes] =
    outputs.toList.sorted(orderBytes.toOrdering)

  private[validation] val orderOutputSets: Ordering[Set[Bytes]] =
    Order.by[Set[Bytes], List[Bytes]](orderedOutputs).toOrdering

  private[validation] val orderInconsistency: Ordering[Inconsistency] = {
    import scala.math.Ordering.Implicits.seqOrdering
    implicit val occurrenceOrdering: Ordering[ExternalCallOccurrence] = orderExternalCallOccurrence
    Ordering
      .by[Inconsistency, DAMLe.ExternalCallKey](_.key)
      .orElseBy(_.outputs)(orderOutputSets)
      .orElseBy(_.occurrences.toSeq.sorted)
  }

  private def inconsistencyFor(
      key: DAMLe.ExternalCallKey,
      outputsAndOccurrences: Seq[(Bytes, ExternalCallOccurrence)],
  ): Option[Inconsistency] = {
    val occurrencesByOutput = outputsAndOccurrences.groupMap(_._1)(_._2)
    Option.when(occurrencesByOutput.sizeCompare(1) > 0)(
      Inconsistency(
        key,
        occurrencesByOutput.keySet,
        occurrencesByOutput.valuesIterator.flatten.toSet,
      )
    )
  }

  private def visibleOccurrences(
      viewValidationResults: Map[ViewPosition, ViewValidationResult]
  ): Seq[VisibleExternalCallOccurrence] =
    viewValidationResults.toSeq
      .sortBy(_._1)(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { case (viewPosition, viewValidationResult) =>
        val viewParticipantData = viewValidationResult.view.viewParticipantData
        if (
          viewParticipantData.supportsExternalCallResults &&
          viewParticipantData.externalCallResults.nonEmpty
        ) {
          viewParticipantData.externalCallResults.map { externalCallResult =>
            val occurrence = ExternalCallOccurrence(
              viewPosition,
              externalCallResult.exerciseIndex,
              externalCallResult.callIndex,
            )
            VisibleExternalCallOccurrence(
              DAMLe.ExternalCallKey.fromResult(externalCallResult.result),
              externalCallResult.result.output,
              occurrence,
              externalCallResult.checkingParties,
            )
          }
        } else Seq.empty
      }

  private def visibleInconsistencies(
      occurrences: Seq[VisibleExternalCallOccurrence]
  ): Seq[Inconsistency] =
    occurrences
      .groupMap(_.key)(occurrence => occurrence.output -> occurrence.occurrence)
      .toSeq
      .flatMap { case (key, outputsAndOccurrences) =>
        inconsistencyFor(key, outputsAndOccurrences)
      }
      .sorted(orderInconsistency)

  private def hostedInconsistencies(
      occurrences: Seq[VisibleExternalCallOccurrence],
      hostedConfirmingParties: Set[LfPartyId],
  ): Map[LfPartyId, Seq[Inconsistency]] =
    occurrences
      .flatMap { occurrence =>
        occurrence.checkingParties.intersect(hostedConfirmingParties).toSeq.map { party =>
          (party, occurrence.key) -> (occurrence.output -> occurrence.occurrence)
        }
      }
      .groupMap(_._1)(_._2)
      .toSeq
      .flatMap { case ((party, key), outputsAndOccurrences) =>
        inconsistencyFor(key, outputsAndOccurrences).map(party -> _)
      }
      .groupMap(_._1)(_._2)
      .view
      .mapValues(_.sorted(orderInconsistency))
      .toMap

  /** Returns per-party inconsistencies for hosted confirming parties that can see disagreeing
    * outputs for the same external call. Also records visible disagreements that should be alarmed
    * even if this participant does not host an affected checking party.
    */
  def check(
      viewValidationResults: Map[ViewPosition, ViewValidationResult],
      hostedConfirmingParties: Set[LfPartyId],
  ): Result = {
    val occurrences = visibleOccurrences(viewValidationResults)
    if (occurrences.isEmpty) Result.empty
    else {
      val visible = visibleInconsistencies(occurrences)
      Result(
        inconsistencies = hostedInconsistencies(occurrences, hostedConfirmingParties),
        visibleInconsistencies = visible,
      )
    }
  }
}
