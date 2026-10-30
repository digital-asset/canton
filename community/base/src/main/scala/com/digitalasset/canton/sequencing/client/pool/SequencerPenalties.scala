// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeDouble, PositiveDouble, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap

/** Penalty modifier for sequencers. */
trait SequencerPenaltyModifier {

  /** Increase the penalty of the associated sequencer by `amount`, which must be positive. */
  def increase(amount: PositiveInt, reason: String)(implicit traceContext: TraceContext): Unit
}

object SequencerPenaltyModifier {

  /** A modifier that discards all penalty increases. */
  val noOp: SequencerPenaltyModifier = new SequencerPenaltyModifier {
    override def increase(amount: PositiveInt, reason: String)(implicit
        traceContext: TraceContext
    ): Unit = ()
  }
}

/** Keeps a penalty per sequencer. All penalties start at 0, and a higher penalty makes a sequencer
  * less preferred.
  *
  * Penalties are increased by connections through [[SequencerPenaltyModifier.increase]], and decay
  * exponentially through [[decay]]. To make sure that a sequencer whose penalty has been increased
  * eventually ties again with sequencers whose penalty was never increased, decayed penalties below
  * the configured threshold are set to exactly 0.
  *
  * All operations are thread-safe and lock-free.
  */
final class SequencerPenalties(protected override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {
  private val penalties = TrieMap[SequencerId, NonNegativeDouble]()

  @VisibleForTesting
  private[client] def snapshot: Map[SequencerId, NonNegativeDouble] =
    penalties.readOnlySnapshot().toMap

  def increase(sequencerId: SequencerId, amount: PositiveInt, reason: String)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      s"Increasing penalty of sequencer \"${sequencerId.uid.identifier}\" by $amount ($reason)"
    )
    penalties
      .updateWith(sequencerId) {
        // `create` should never fail because Double overflows to Infinity, not a negative value,
        // and this cannot happen even with PositiveInt.MaxValue (it remains finite).
        case Some(penalty) =>
          val newPenalty = NonNegativeDouble
            .create(penalty.value + amount.value)
            .valueOr(err => ErrorUtil.invalidState(s"Illegal penalty value: $err"))
          Some(newPenalty)
        case None =>
          val newPenalty =
            NonNegativeDouble
              .create(amount.value.toDouble)
              .valueOr(err => ErrorUtil.invalidState(s"Illegal penalty value: $err"))
          Some(newPenalty)
      }
      .discard
  }

  /** Return the current penalty of the given sequencer, or 0 if it has none. */
  def get(sequencerId: SequencerId): NonNegativeDouble =
    penalties.getOrElse(sequencerId, NonNegativeDouble.zero)

  /** Remove the penalties of the given sequencers.
    *
    * NOTE: The operation is not atomic with respect to other updates, but interleaving calls does
    * not compromise the integrity of the penalties.
    */
  def remove(sequencerIds: Iterable[SequencerId]): Unit =
    penalties.subtractAll(sequencerIds)

  /** Multiply the penalties of all the sequencers by `factor`, removing penalties that fall below
    * `zeroThreshold` (thereby resetting them to 0).
    *
    * NOTE: The operation is not atomic with respect to other updates, but interleaving calls does
    * not compromise the integrity of the penalties.
    */
  def decay(factor: NonNegativeDouble, zeroThreshold: PositiveDouble): Unit =
    penalties.keySet.foreach { sequencerId =>
      penalties
        .updateWith(sequencerId) {
          case Some(penalty) =>
            val decayed = penalty * factor
            if (decayed < zeroThreshold) None else Some(decayed)
          case None => None
        }
        .discard
    }
}
