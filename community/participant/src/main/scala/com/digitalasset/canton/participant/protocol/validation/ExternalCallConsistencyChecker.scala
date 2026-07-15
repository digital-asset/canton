// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Order
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.ExternalCallPayloadDescription.{byteCount, hexPayloadSize}
import com.digitalasset.canton.data.{ExternalCallKey, ParticipantTransactionView, ViewPosition}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

/** Checks whether visible external-call results agree.
  *
  * An external-call result is ''visible'' to this participant if it is recorded in one of the
  * (unblinded) views that this participant has received for validation. External-call results are
  * replay data that participate in validation. If two visible occurrences of the same external call
  * record different outputs, the recorded data is ambiguous: the transaction must be rejected
  * rather than approved, and the disagreement is reported as suspicious recorded data.
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
      key: ExternalCallKey,
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

  /** An external-call result recorded in a view that this participant has received. */
  private final case class VisibleExternalCallOccurrence(
      key: ExternalCallKey,
      output: Bytes,
      occurrence: ExternalCallOccurrence,
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
      .by[Inconsistency, ExternalCallKey](_.key)
      .orElseBy(_.outputs)(orderOutputSets)
      .orElseBy(_.occurrences.toSeq.sorted)
  }

  private def inconsistencyFor(
      key: ExternalCallKey,
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
      views: Map[ViewPosition, ParticipantTransactionView]
  ): Seq[VisibleExternalCallOccurrence] =
    views.toSeq
      .sortBy(_._1)(ViewPosition.orderViewPosition.toOrdering)
      .flatMap { case (viewPosition, view) =>
        view.viewParticipantData.externalCallResults.map { externalCallResult =>
          val occurrence = ExternalCallOccurrence(
            viewPosition,
            externalCallResult.exerciseIndex,
            externalCallResult.callIndex,
          )
          VisibleExternalCallOccurrence(
            ExternalCallKey.fromResult(externalCallResult.result),
            externalCallResult.result.output,
            occurrence,
          )
        }
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

  /** Returns the disagreements across all occurrences visible to this participant, sorted
    * deterministically (by key, then outputs, then occurrences).
    */
  def check(views: Map[ViewPosition, ParticipantTransactionView]): Seq[Inconsistency] =
    visibleInconsistencies(visibleOccurrences(views))
}
