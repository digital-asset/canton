// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SequencerAggregator.{
  SequencerAggregatorError,
  SequencerId,
}
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}
import scala.collection.mutable
import scala.concurrent.{Future, Promise, blocking}

class SequencerAggregator(
    cryptoPureApi: CryptoPureApi,
    eventInboxSize: PositiveInt,
    val loggerFactory: NamedLoggerFactory,
    expectedSequencers: NonEmpty[Set[SequencerAggregator.SequencerId]] =
      NonEmpty.mk(Set, SequencerAggregator.DefaultSequencerId),
) extends NamedLogging {

  private val expectedSequencersSize: Int = expectedSequencers.size

  private case class SequencerMessageData(
      eventBySequencer: Map[SequencerId, OrdinarySerializedEvent],
      promise: Promise[Either[SequencerAggregatorError, SequencerId]],
  )

  /** Queue containing received and not yet handled events.
    * Used for batched processing.
    */
  private val receivedEvents: BlockingQueue[OrdinarySerializedEvent] =
    new ArrayBlockingQueue[OrdinarySerializedEvent](eventInboxSize.unwrap)

  private val sequenceData = mutable.TreeMap.empty[CantonTimestamp, SequencerMessageData]

  def eventQueue: BlockingQueue[OrdinarySerializedEvent] = receivedEvents

  private def hash(message: OrdinarySerializedEvent) =
    SignedContent.hashContent(
      cryptoPureApi,
      message.signedEvent.content,
      HashPurpose.SequencedEventSignature,
    )

  @VisibleForTesting
  def combine(
      messages: NonEmpty[Seq[OrdinarySerializedEvent]]
  ): Either[SequencerAggregatorError, OrdinarySerializedEvent] = {
    val message: OrdinarySerializedEvent = messages.head1
    val expectedMessageHash = hash(message)
    val hashes: NonEmpty[Set[Hash]] = messages.map(hash).toSet
    val timestampsOfSigningKey = messages.map(_.signedEvent.timestampOfSigningKey).toSet
    for {
      _ <- Either.cond(
        hashes.forall(_ == expectedMessageHash),
        (),
        SequencerAggregatorError.NotTheSameContentHash(hashes),
      )
      expectedTimestampOfSigningKey = message.signedEvent.timestampOfSigningKey
      _ <- Either.cond(
        messages.forall(_.signedEvent.timestampOfSigningKey == expectedTimestampOfSigningKey),
        (),
        SequencerAggregatorError.NotTheSameTimestampOfSigningKey(timestampsOfSigningKey),
      )
    } yield {
      val combinedSignatures: NonEmpty[Seq[Signature]] = messages.flatMap(_.signedEvent.signatures)

      val potentiallyNonEmptyTraceContext = messages
        .find(_.traceContext != TraceContext.empty)
        .map(_.traceContext)
        .getOrElse(message.traceContext)

      message.copy(signedEvent = message.signedEvent.copy(signatures = combinedSignatures))(
        potentiallyNonEmptyTraceContext
      )
    }
  }

  private def addEventToQueue(event: OrdinarySerializedEvent): Unit = {
    implicit val traceContext: TraceContext = event.traceContext
    logger.debug(
      show"Storing event in the event inbox.\n${event.signedEvent.content}"
    )
    if (!receivedEvents.offer(event)) {
      logger.debug(
        s"Event inbox is full. Blocking sequenced event with timestamp ${event.timestamp}."
      )
      blocking {
        receivedEvents.put(event)
      }
      logger.debug(
        s"Unblocked sequenced event with timestamp ${event.timestamp}."
      )
    }
  }

  private def addEventToQueue(
      nextData: SequencerMessageData
  ): Either[SequencerAggregatorError, Unit] = {
    val messages =
      expectedSequencers.map(nextData.eventBySequencer).toSeq
    combine(messages).map(addEventToQueue)
  }

  def combineAndMergeEvent(
      sequencerId: SequencerId,
      message: OrdinarySerializedEvent,
  ): Future[Either[SequencerAggregatorError, SequencerId]] = {
    if (!expectedSequencers.contains(sequencerId)) {
      throw new IllegalArgumentException(s"Unexpected sequencerId: $sequencerId")
    }
    blocking {
      this.synchronized {

        val sequencerMessageData = updatedSequencerMessageData(sequencerId, message)

        sequenceData.put(message.timestamp, sequencerMessageData): Unit

        val (nextMinimumTimestamp, nextData) =
          sequenceData.headOption.getOrElse(
            (message.timestamp, sequencerMessageData)
          ) // returns min message.timestamp

        if (nextData.eventBySequencer.sizeCompare(expectedSequencersSize) == 0) {
          sequenceData.remove(nextMinimumTimestamp): Unit

          nextData.promise.success(addEventToQueue(nextData).map(_ => sequencerId)): Unit
        }

        sequencerMessageData.promise.future
      }
    }
  }

  private def updatedSequencerMessageData(
      sequencerId: SequencerId,
      message: OrdinarySerializedEvent,
  ): SequencerMessageData = {
    val data =
      sequenceData.getOrElse(
        message.timestamp,
        SequencerMessageData(Map(), Promise[Either[SequencerAggregatorError, SequencerId]]()),
      )
    data.copy(eventBySequencer = data.eventBySequencer.updated(sequencerId, message))
  }

}
object SequencerAggregator {
  type SequencerId = String
  val DefaultSequencerId = "DefaultSequencerId"
  sealed trait SequencerAggregatorError extends Product with Serializable with PrettyPrinting
  object SequencerAggregatorError {
    final case class NotTheSameContentHash(hashes: NonEmpty[Set[Hash]])
        extends SequencerAggregatorError {
      override def pretty: Pretty[NotTheSameContentHash] =
        prettyOfClass(param("hashes", _.hashes))
    }
    final case class NotTheSameTimestampOfSigningKey(
        timestamps: NonEmpty[Set[Option[CantonTimestamp]]]
    ) extends SequencerAggregatorError {
      override def pretty: Pretty[NotTheSameTimestampOfSigningKey] =
        prettyOfClass(param("timestamps", _.timestamps))
    }
  }
}