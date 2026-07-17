// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Order
import com.digitalasset.canton.LfPartyId
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
  * record different outputs, a hosted confirming party must reject the transaction locally instead
  * of approving an ambiguous result. Visible disagreements are also retained independently of
  * hosted-party routing so callers can report suspicious recorded data.
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

  /** Outcome of [[check]].
    *
    * @param hostedInconsistencies
    *   Inconsistencies grouped by hosted confirming party, restricted for each party to the
    *   occurrences that the party is responsible for checking. Grouped by party because
    *   confirmation responses are issued on behalf of individual confirming parties: each hosted
    *   confirming party must reject exactly the disagreements among its own occurrences.
    * @param visibleInconsistencies
    *   Inconsistencies across all occurrences visible to this participant, irrespective of which
    *   parties it hosts. Used to alarm on suspicious recorded data even if this participant hosts
    *   no affected checking party.
    */
  final case class Result(
      hostedInconsistencies: Map[LfPartyId, Seq[Inconsistency]],
      visibleInconsistencies: Seq[Inconsistency],
  ) extends PrettyPrinting {
    def inconsistentParties: Set[LfPartyId] = hostedInconsistencies.keySet

    override protected def pretty: Pretty[Result] = prettyOfClass(
      param("hostedInconsistencies", _.hostedInconsistencies),
      param("visibleInconsistencies", _.visibleInconsistencies),
    )
  }

  object Result {
    val empty: Result = Result(Map.empty, Seq.empty)
  }

  /** An external-call result recorded in a view that this participant has received.
    *
    * @param checkingParties
    *   The node-level confirming parties responsible for checking this result: the signatories and
    *   acting parties of the exercise node that recorded the call, as determined at
    *   transaction-tree construction. See
    *   [[com.digitalasset.canton.data.ViewParticipantData.ViewExternalCallResult]].
    */
  private final case class VisibleExternalCallOccurrence(
      key: ExternalCallKey,
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
            externalCallResult.checkingParties,
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

  /** Returns per-party inconsistencies for hosted confirming parties that check disagreeing outputs
    * for the same external call, as well as the disagreements across all visible occurrences. See
    * [[Result]] for how the two differ.
    */
  def check(
      views: Map[ViewPosition, ParticipantTransactionView],
      hostedConfirmingParties: Set[LfPartyId],
  ): Result = {
    val occurrences = visibleOccurrences(views)
    Result(
      hostedInconsistencies = hostedInconsistencies(occurrences, hostedConfirmingParties),
      visibleInconsistencies = visibleInconsistencies(occurrences),
    )
  }
}
