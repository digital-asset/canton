// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.time

import cats.data.EitherT
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.synchronizer.sequencer.config.LsuSequencingBoundsOverride
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Used to specify time bounds around LSU.
  * @param upgradeTime
  *   Upgrade time from the previous synchronizer
  * @param lowerBoundSequencingTimeExclusive
  *   Strict lower bound on sequencing times. Defined as the highest effective time of the topology
  *   store on the predecessor.
  *
  * The following inequality holds: LSUAnnouncement.effective <= lowerBoundSequencingTimeExclusive <
  * upgradeTime
  *
  * Context on the two timestamps:
  *   - lowerBoundSequencingTimeExclusive acts as:
  *     - strict lower bound for every thing that happens on the sequencer:
  *       - topology snapshots
  *       - subscriptions
  *       - watermark
  *     - strict lower bound for messages that can be delivered to synchronizer nodes.
  *   - upgradeTime serves as a strict lower bound for messages that can be delivered to participant
  *     nodes.
  */
final case class LsuSequencingBounds private (
    lowerBoundSequencingTimeExclusive: CantonTimestamp,
    upgradeTime: CantonTimestamp,
) {
  require(
    lowerBoundSequencingTimeExclusive <= upgradeTime,
    s"lowerBoundSequencingTimeExclusive should be <= upgradeTime but found $lowerBoundSequencingTimeExclusive and $upgradeTime respectively",
  )
}

object LsuSequencingBounds {

  /** Create an [[LsuSequencingBounds]] from the config.
    */
  def create(
      lsuSequencingBoundsOverride: LsuSequencingBoundsOverride,
      store: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, LsuSequencingBounds] = {

    val LsuSequencingBoundsOverride(lowerBoundSequencingTimeExclusive, upgradeTime) =
      lsuSequencingBoundsOverride

    for {
      upgradeTimeFromStoreO <- EitherT.liftF(store.findUpgradeTimeFromPredecessor())
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        upgradeTimeFromStoreO.isEmpty,
        "LsuSequencingBoundsOverride cannot be set if an LSU announcement exists in the topology store",
      )

      lsuSequencingBounds <- EitherT.cond[FutureUnlessShutdown](
        lowerBoundSequencingTimeExclusive <= upgradeTime,
        LsuSequencingBounds(lowerBoundSequencingTimeExclusive, upgradeTime),
        s"lowerBoundSequencingTimeExclusive should be <= upgradeTime but found $lowerBoundSequencingTimeExclusive and $upgradeTime",
      )
    } yield lsuSequencingBounds
  }

  @VisibleForTesting
  // Bypass all checks. Only for testing
  def unsafeCreate(
      lowerBoundSequencingTimeExclusive: CantonTimestamp,
      upgradeTime: CantonTimestamp,
  ): LsuSequencingBounds = LsuSequencingBounds(lowerBoundSequencingTimeExclusive, upgradeTime)

  /** Create an [[LsuSequencingBounds]] from a topology store. Returns None if there are no relevant
    * LSU announcement. An announcement is considered relevant if the successor psid matches the
    * psid of the topology store.
    */
  def create(
      store: TopologyStore[SynchronizerStore]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[LsuSequencingBounds]] = {
    implicit val traceContext = errorLoggingContext.traceContext

    for {
      upgradeTimeO <- store.findUpgradeTimeFromPredecessor()

      lsuSequencingBounds <- upgradeTimeO
        .traverse { upgradeTime =>
          store
            .maxTimestamp(sequencedTime = SequencedTime(upgradeTime), includeRejected = true)
            .map {
              case Some((_, latestEffectiveTime)) =>
                /*
                Note: the require in the LsuSequencingBounds might throw if the invariant is not respected.
                This is fine because it means that something went terribly wrong: a topology change is effective after upgrade time.
                 */
                LsuSequencingBounds(
                  upgradeTime = upgradeTime,
                  lowerBoundSequencingTimeExclusive = latestEffectiveTime.value,
                ).some
              case None =>
                ErrorUtil.invalidState(
                  "Unable to find max timestamp of the topology store although there is an LsuAnnouncement in the store"
                )
            }
        }
        .map(_.flatten)
    } yield lsuSequencingBounds
  }
}
