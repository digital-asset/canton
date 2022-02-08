// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either._
import com.digitalasset.canton._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.RequestCounter.GenesisRequestCounter
import com.digitalasset.canton.participant.protocol.ProcessingStartingPoints.InvalidStartingPointsException
import com.digitalasset.canton.participant.{LocalOffset, RequestCounter}
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.util.NoCopy

/** Summarizes the counters and timestamps where request processing or replay can start
  *
  * @param nextRequestCounter The request counter for the next request to be replayed or processed.
  * @param nextSequencerCounter The sequencer counter for the next event to be replayed or processed.
  * @param prenextTimestamp A strict lower bound on the timestamp for the `nextSequencerCounter`.
  *                         The bound must be tight, i.e., if a sequenced event has sequencer counter lower than
  *                         `nextSequencerCounter` or request counter lower than `nextRequestCounter`,
  *                         then the timestamp of the event must be less than or equal to `prenextTimestamp`.
  *
  *                         No sequenced event has both a higher timestamp than `prenextTimestamp`
  *                         and a lower sequencer counter than `nextSequencerCounter`.
  *                         No request has both a higher timestamp than `prenextTimestamp`
  *                         and a lower request counter than `nextRequestCounter`.
  */
case class MessageProcessingStartingPoint(
    nextRequestCounter: RequestCounter,
    nextSequencerCounter: SequencerCounter,
    prenextTimestamp: CantonTimestamp,
) extends PrettyPrinting {

  override def pretty: Pretty[MessageProcessingStartingPoint] = prettyOfClass(
    param("next request counter", _.nextRequestCounter),
    param("next sequencer counter", _.nextSequencerCounter),
    param("prenext timestamp", _.prenextTimestamp),
  )
}

object MessageProcessingStartingPoint {
  def default: MessageProcessingStartingPoint =
    MessageProcessingStartingPoint(
      GenesisRequestCounter,
      GenesisSequencerCounter,
      CantonTimestamp.MinValue,
    )
}

/** Starting points for processing on a [[com.digitalasset.canton.participant.sync.SyncDomain]].
  * The `cleanReplay` should be no later than the `processing` (in all components).
  *
  * @param cleanReplay The starting point for replaying clean requests
  * @param processing                     The starting point for processing requests.
  *                                       It refers to the first request that is not known to be clean.
  *                                       The [[MessageProcessingStartingPoint.prenextTimestamp]] be the timestamp of a sequenced event
  *                                       or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]].
  * @param eventPublishingNextLocalOffset The next local offset that may be published to the
  *                                       [[com.digitalasset.canton.participant.store.MultiDomainEventLog]]
  * @param rewoundSequencerCounterPrehead The point to which the sequencer counter prehead needs to be reset as part of the recovery clean-up.
  *                                       This is the minimum of the following:
  *                                       * The last event before the [[processing]] event
  *                                       * The clean sequencer counter prehead at the beginning of crash recovery.
  * @throws ProcessingStartingPoints.InvalidStartingPointsException if `cleanReplay` is after (in any component) `processing`
  */
case class ProcessingStartingPoints private (
    cleanReplay: MessageProcessingStartingPoint,
    processing: MessageProcessingStartingPoint,
    eventPublishingNextLocalOffset: LocalOffset,
    rewoundSequencerCounterPrehead: Option[CursorPrehead[SequencerCounter]],
) extends NoCopy
    with PrettyPrinting {

  if (cleanReplay.prenextTimestamp > processing.prenextTimestamp)
    throw InvalidStartingPointsException(
      s"Clean replay pre-next timestamp ${cleanReplay.prenextTimestamp} is after processing pre-next timestamp ${processing.prenextTimestamp}"
    )
  if (cleanReplay.nextRequestCounter > processing.nextRequestCounter)
    throw InvalidStartingPointsException(
      s"Clean replay next request counter ${cleanReplay.nextRequestCounter} is after processing next request counter ${processing.nextRequestCounter}"
    )
  if (cleanReplay.nextSequencerCounter > processing.nextSequencerCounter)
    throw InvalidStartingPointsException(
      s"Clean replay next sequencer counter ${cleanReplay.nextSequencerCounter} is after processing next sequencer counter ${processing.nextSequencerCounter}"
    )

  /** No events should have been published at or after the point where processing starts again
    * because [[com.digitalasset.canton.participant.protocol.AbstractMessageProcessor.terminateRequest]]
    * ticks the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]] only after
    * the clean request prehead has advanced in the [[RequestJournal]]. So this method normally returns `true`.
    *
    * Some tests reset the clean request prehead and thus violate this invariant.
    * In such a case, this method returns `false.`
    */
  def processingAfterPublished: Boolean =
    processing.nextRequestCounter >= eventPublishingNextLocalOffset

  override def pretty: Pretty[ProcessingStartingPoints] = prettyOfClass(
    param("clean replay", _.cleanReplay),
    param("processing", _.processing),
    param("event publishing next local offset", _.eventPublishingNextLocalOffset),
    param("rewound sequencer counter prehead", _.rewoundSequencerCounterPrehead),
  )
}

object ProcessingStartingPoints {
  case class InvalidStartingPointsException(message: String) extends RuntimeException(message)

  private[this] def apply(
      cleanReplay: MessageProcessingStartingPoint,
      processing: MessageProcessingStartingPoint,
      eventPublishingNextLocalOffset: LocalOffset,
      rewoundSequencerCounterPrehead: Option[CursorPrehead[SequencerCounter]],
  ): ProcessingStartingPoints =
    throw new UnsupportedOperationException("Use the factory methods instead")

  def tryCreate(
      cleanReplay: MessageProcessingStartingPoint,
      processing: MessageProcessingStartingPoint,
      eventPublishingNextLocalOffset: LocalOffset,
      rewoundSequencerCounterPrehead: Option[CursorPrehead[SequencerCounter]],
  ): ProcessingStartingPoints =
    new ProcessingStartingPoints(
      cleanReplay,
      processing,
      eventPublishingNextLocalOffset,
      rewoundSequencerCounterPrehead,
    )

  def create(
      cleanReplay: MessageProcessingStartingPoint,
      processing: MessageProcessingStartingPoint,
      eventPublishingNextLocalOffset: LocalOffset,
      rewoundSequencerCounterPrehead: Option[CursorPrehead[SequencerCounter]],
  ): Either[String, ProcessingStartingPoints] =
    Either
      .catchOnly[InvalidStartingPointsException](
        tryCreate(
          cleanReplay,
          processing,
          eventPublishingNextLocalOffset,
          rewoundSequencerCounterPrehead,
        )
      )
      .leftMap(_.message)

  def default: ProcessingStartingPoints =
    new ProcessingStartingPoints(
      MessageProcessingStartingPoint.default,
      MessageProcessingStartingPoint.default,
      GenesisRequestCounter,
      None,
    )
}
