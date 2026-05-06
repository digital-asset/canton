// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.{Chain, EitherT}
import cats.syntax.either.*
import com.digitalasset.canton.crypto.{
  Signature,
  SyncCryptoApi,
  SyncCryptoClient,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.sequencing.protocol.{AggregationBySender, AggregationRule}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

/** Stores the state of an in-flight aggregation of submission requests.
  *
  * Since the [[com.digitalasset.canton.sequencing.protocol.AggregationId]] computationally
  * identifies the envelope contents, their recipients, and the
  * [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.topologyTimestamp]], we do not
  * need to maintain these data as part of the in-flight tracking. Instead, we can derive them from
  * the submission request that makes the aggregation reach its threshold.
  *
  * @param aggregatedSenders
  *   The senders whose submission request have already been aggregated with the timestamp of their
  *   aggregated submission request and the signatures on the envelopes.
  * @param maxSequencingTimestamp
  *   The max sequencing timestamp of the aggregatable submission requests. The aggregation will
  *   stop being in-flight when this timestamp has elapsed
  * @param rule
  *   The aggregation rule describing the eligible members and the threshold to reach
  * @param cachedDeliveredAt
  *   computed delivery timestamp of the inflight aggregation, cached to avoid recomputation.
  */
final case class InFlightAggregation(
    aggregatedSenders: SortedMap[Member, AggregationBySender],
    maxSequencingTimestamp: CantonTimestamp,
    rule: AggregationRule,
    cachedDeliveredAt: Option[Option[CantonTimestamp]] = None,
) extends PrettyPrinting
    with HasLoggerName {
  import InFlightAggregation.*

  def tryIsDeliveredAt: Option[CantonTimestamp] = cachedDeliveredAt.getOrElse(
    throw new IllegalStateException(
      "Inflight aggregation must precompute deliveredAt before using this method"
    )
  )

  /** The aggregated signatures on the closed envelopes in the aggregatable submission request, in
    * the same order as the envelopes are in the batch.
    *
    * The signatures for each envelope are ordered by the sender who produced them, rather than the
    * order in which the senders' submission requests were sequenced. This avoids leaking the order
    * of internal sequencing, as the signatures themselves anyway leak the sender through the
    * signing key's fingerprint.
    */
  def aggregatedSignatures: Seq[Seq[Signature]] =
    aggregatedSenders.values.map(_.signatures).transpose.map(_.flatten.toSeq).toSeq

  /** Recompute aggregation delivery times (used after restart)
    *
    * @param sequencingTimestamp
    *   the timestamp at which we are recovering the in-flight aggregation. Just used for
    *   consistency checking as we must not have any signature with a sequencing timestamp after
    *   this timestamp.
    * @param latestSequencerEventTimestamp
    *   the timestamp of the last event ticking the topology client
    */
  def prepareForNewAggregation(
      sequencingTimestamp: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      syncCryptoClient: SynchronizerCryptoClient,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[InFlightAggregation] =
    // if we haven't yet computed the delivered at, we will do it now
    if (cachedDeliveredAt.isEmpty) {
      val ret =
        // we can assume here that no signature was added to the aggregation after it previously got delivered.
        // otherwise, tryAggregate would have returned AlreadyDelivered
        // this means that we can evaluate the valid signatures at the latest sequencing timestamp
        aggregatedSenders.values
          .map(_.sequencingTimestamp)
          .maxOption
          .map { maxSequencingTime =>
            ErrorUtil.requireState(
              maxSequencingTime <= sequencingTimestamp,
              s"Cannot prepare in-flight aggregation for new aggregation at sequencing timestamp $sequencingTimestamp because it contains signatures with sequencing timestamp up to $maxSequencingTime",
            )
            // We need to be careful with the snapshot we are using for computing the delivered at and use the
            // standard method for finding the right snapshot in the sequencer, respecting the events that
            // this client has seen.
            SyncCryptoClient
              .getSnapshotForTimestamp(
                syncCryptoClient,
                maxSequencingTime,
                latestSequencerEventTimestamp,
              )
              .flatMap { snapshot =>
                rule.input.computeDeliveredAt(aggregatedSenders, snapshot)
              }
          }
          .getOrElse(FutureUnlessShutdown.pure(None))
      ret.map {
        case Some((deliveredAt, cleanedSignatures)) =>
          copy(cachedDeliveredAt = Some(Some(deliveredAt)), aggregatedSenders = cleanedSignatures)
        case None => copy(cachedDeliveredAt = Some(None))
      }
    } else {
      FutureUnlessShutdown.pure(this)
    }

  def tryAggregate(
      aggregatedSender: AggregatedSender,
      syncCryptoApi: SyncCryptoApi,
  )(implicit
      loggingContext: NamedLoggingContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, InFlightAggregationError, InFlightAggregation] = {
    val sender = aggregatedSender.sender
    val timestamp = aggregatedSender.aggregation.sequencingTimestamp
    ErrorUtil.requireState(
      timestamp <= maxSequencingTimestamp,
      s"Cannot aggregate submission by $sender with sequencing timestamp $timestamp after the max sequencing time at $maxSequencingTimestamp",
    )
    for {
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        tryIsDeliveredAt.toLeft(()).leftMap(AlreadyDelivered.apply)
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        aggregatedSenders
          .get(sender)
          .toLeft(())
          .leftMap(aggregationBySender =>
            AggregationStuffing(sender, aggregationBySender.sequencingTimestamp)
          )
      )
      newAggregatedSenders = aggregatedSenders + (sender -> aggregatedSender.aggregation)
      deliveredAtAndCleanedSignatures <- EitherT.right(
        rule.input.computeDeliveredAt(newAggregatedSenders, syncCryptoApi)
      )
    } yield {
      deliveredAtAndCleanedSignatures match {
        case Some((deliveredAt, cleanedSignatures)) =>
          copy(
            aggregatedSenders = cleanedSignatures,
            cachedDeliveredAt = Some(Some(deliveredAt)),
          )
        case None =>
          copy(
            aggregatedSenders = newAggregatedSenders,
            cachedDeliveredAt = Some(None),
          )
      }

    }

  }

  def extendWithValidButMaybeDuplicateAggregation(
      validAggregateSender: AggregatedSender
  ): InFlightAggregation = {
    val sender = validAggregateSender.sender
    val timestamp = validAggregateSender.aggregation.sequencingTimestamp
    require(
      timestamp <= maxSequencingTimestamp,
      s"Cannot aggregate submission by $sender with sequencing timestamp $timestamp after the max sequencing time at $maxSequencingTimestamp",
    )
    aggregatedSenders.get(sender) match {
      case Some(current) =>
        require(
          current == validAggregateSender.aggregation,
          s"aggregation for $sender already exists but is different than the new one",
        )
      case _ =>
    }
    InFlightAggregation.tryCreate(
      aggregatedSenders = aggregatedSenders + (sender -> validAggregateSender.aggregation),
      maxSequencingTimestamp = maxSequencingTimestamp,
      rule = rule,
    )
  }

  def asUpdate: InFlightAggregationUpdate = InFlightAggregationUpdate(
    Some(FreshInFlightAggregation(maxSequencingTimestamp, rule)),
    Chain(
      aggregatedSenders
        .map { case (sender, aggregationBySender) =>
          AggregatedSender(sender, aggregationBySender)
        }
        .to(Seq)*
    ),
  )

  /** Returns whether the in-flight aggregation has expired before or at the given timestamp. An
    * expired in-flight aggregation is no longer needed and can be removed.
    */
  def expired(timestamp: CantonTimestamp): Boolean = timestamp >= maxSequencingTimestamp

  /** Undoes all changes to the in-flight aggregation state that happened after the given timestamp.
    *
    * @return
    *   [[scala.None$]] if the aggregation is not in-flight at the given timestamp. An aggregation
    *   in in-flight from the first [[aggregatedSenders]]' timestamp to the
    *   [[maxSequencingTimestamp]].
    */
  def project(timestamp: CantonTimestamp): Option[InFlightAggregation] =
    for {
      _ <- Option.when(maxSequencingTimestamp > timestamp)(())
      projectedSenders = aggregatedSenders.filter {
        case (_sender, AggregationBySender(sequencingTimestamp, _signatures)) =>
          sequencingTimestamp <= timestamp
      }
      _ <- Option.when(projectedSenders.nonEmpty)(())
    } yield {
      new InFlightAggregation(
        projectedSenders,
        maxSequencingTimestamp,
        rule,
        cachedDeliveredAt = None,
      )
    }

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    param("aggregated senders", _.aggregatedSenders),
    param("max sequencing time", _.maxSequencingTimestamp),
    paramIfNonEmpty("sequencing timestamp", _.cachedDeliveredAt.flatten),
    param("rule", _.rule),
  )

  /** @throws java.lang.IllegalStateException if the class invariant does not hold */
  def checkInvariant()(implicit loggingContext: NamedLoggingContext): Unit =
    rule.input
      .checkInvariant(aggregatedSenders, maxSequencingTimestamp)
      .valueOr(err => ErrorUtil.invalidState(err))
}

object InFlightAggregation {

  def create(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  ): Either[String, InFlightAggregation] =
    rule.input.checkInvariant(aggregatedSenders, maxSequencingTimestamp).map { _ =>
      val sortedSenders = SortedMap.from(aggregatedSenders)
      new InFlightAggregation(
        sortedSenders,
        maxSequencingTimestamp,
        rule,
        cachedDeliveredAt = None,
      )
    }

  def tryCreate(
      aggregatedSenders: Map[Member, AggregationBySender],
      maxSequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  ): InFlightAggregation =
    create(aggregatedSenders, maxSequencingTimestamp, rule)
      .valueOr(err => throw new IllegalArgumentException(err))

  @VisibleForTesting
  def apply(
      rule: AggregationRule,
      maxSequencingTimestamp: CantonTimestamp,
      aggregatedSenders: (Member, AggregationBySender)*
  ): InFlightAggregation =
    InFlightAggregation.tryCreate(
      aggregatedSenders = Map.from(aggregatedSenders),
      maxSequencingTimestamp,
      rule,
    )

  def initial(fresh: FreshInFlightAggregation): InFlightAggregation =
    InFlightAggregation(
      aggregatedSenders = SortedMap.empty,
      maxSequencingTimestamp = fresh.maxSequencingTimestamp,
      rule = fresh.rule,
      cachedDeliveredAt = None,
    )

  sealed trait InFlightAggregationError extends Product with Serializable

  /** The aggregatable submission was already delivered at the given timestamp. */
  final case class AlreadyDelivered(deliveredAt: CantonTimestamp) extends InFlightAggregationError

  /** The given sender has already contributed its aggregatable submission request, which was
    * sequenced at the given timestamp
    */
  final case class AggregationStuffing(sender: Member, sequencingTimestamp: CantonTimestamp)
      extends InFlightAggregationError

}
