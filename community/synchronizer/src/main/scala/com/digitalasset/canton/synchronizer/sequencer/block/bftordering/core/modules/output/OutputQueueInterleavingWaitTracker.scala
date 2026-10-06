// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputQueueInterleavingWaitTracker.InterleavingWaitTracking
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.CompleteBlockData
import com.google.common.annotations.VisibleForTesting

import java.time.{Duration, Instant}
import scala.collection.mutable

/** Attributes the output Peano queue extraction "wait" to the leaders whose lower-numbered blocks
  * ("holes") delayed extraction, and emits the resulting per-leader (and per-block total)
  * interleaving-wait metrics.
  *
  * ISS orders each epoch's blocks in interleaved segments (one per leader), so a slow segment
  * leaves holes that hold back already-ordered higher blocks; summing the resulting per-leader
  * series shows how much of the output-stage latency is due to segment interleaving.
  *
  * The queue head advances one hole at a time: filling a hole pops the contiguous run above it up
  * to the next hole, and the just-closed interval is owned by whichever leader ordered that head
  * hole. Because a leader owns a whole interleaved segment (many holes scattered through the
  * epoch), the head-blocker recurs, so a single waiting block is typically delayed by many
  * successive intervals, several of which may belong to the same leader. Its wait on a given leader
  * is therefore the SUM of that leader's recurring blocking intervals over the block's lifetime.
  *
  * We obtain that sum with a single subtraction by keeping a cumulative per-leader total of
  * head-blocked time and snapshotting it when a block arrives: current total minus arrival snapshot
  * sums exactly the intervals that closed while the block waited, however many and whoever they
  * interleaved with. This keeps both advancing the head and emitting a block's wait O(number of
  * leaders): each block pulls its total once at extraction rather than every closing interval
  * pushing its duration onto every still-waiting block. That naive push is quadratic in the backlog
  * depth `B`: an epoch's worth of already-ordered blocks sits buffered while the queue fills one
  * hole at a time, so the number of closing intervals is `~B` and each touches `~B` waiting blocks
  * (`O(B^2)`).
  *
  * A block usually arrives partway through an already-open head-blocked interval, so the fraction
  * of that interval that elapsed before its arrival must not count as wait time. We correct for
  * this exactly without any interval-keyed bookkeeping: each block records the pre-arrival fraction
  * of the interval it arrived into, and when that interval closes (the next head advance, whose
  * hole leader owns it) we set the fraction into the block's snapshot for that leader. Each block
  * is patched exactly once, so the total work stays linear and the only per-block state (the
  * snapshot) is released when the block is extracted, needing no epoch-boundary cleanup.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[output] final class OutputQueueInterleavingWaitTracker(
    metrics: BftOrderingMetrics
)(implicit mc: MetricsContext) {

  private val stageLatency = metrics.performance.orderingStageLatency
  private val stageLatencyLabels = stageLatency.labels

  // Cumulative wall-clock time each leader has spent as the queue head-blocker, summed over closed intervals (a
  //  running total whose difference over a block's wait yields that leader's recurring blocking intervals). Reset
  //  whenever the queue fully drains (no tracked blocks remain), so it only ever holds leaders active since the
  //  last drain rather than every leader ever observed, keeping it bounded under validator churn.
  //  In a healthy node the queue drains routinely (it empties whenever extraction catches up to the head, i.e.
  //  once no ordered block is waiting behind a hole), so these totals accumulate only over the span since the last
  //  drain, not the whole node uptime.
  //  Overflow is a non-issue: a leader blocks the head only over disjoint windows, so its total can't exceed node
  //  uptime, far below `Duration`'s ~292-billion-year `long`-seconds range (and the drain reset shrinks it anyway).
  private val blockedTimeByLeader = mutable.Map[BftNodeId, Duration]()

  // Monotonic time source for interleaving-wait intervals: measured against `System.nanoTime`, whose deltas are
  //  monotonic, so a backward wall-clock adjustment (e.g. NTP stepping the clock back) cannot move time backward
  //  and fabricate positive waits via negative interval/snapshot deltas. Anchored at construction so the derived
  //  instants stay small and positive; only their differences (`Duration.between`) are ever used.
  private val monotonicBaseNanos: Long = System.nanoTime()
  private def monotonicNow(): Instant =
    Instant.EPOCH.plusNanos(System.nanoTime() - monotonicBaseNanos)

  // Start of the currently open head-blocked interval (the last head advance, or queue init).
  private var openIntervalStart: Instant = monotonicNow()
  // Blocks that arrived during the currently open interval, patched with its hole leader when it closes and then
  //  cleared. Bounded by the current backlog and released every head advance.
  private val blocksArrivedInOpenInterval = mutable.ArrayBuffer[BlockNumber]()
  // Per still-waiting block: arrival snapshot used to reconstruct its per-leader wait at extraction in O(leaders).
  private val perBlockTracking = mutable.Map[BlockNumber, InterleavingWaitTracking]()
  // Mirrors `metrics.performance.enabled`; a change triggers a reset so we never attribute wall-clock time that
  //  spanned a disabled period.
  private var enabledMirror: Boolean = metrics.performance.enabled

  // Monotonic time source for interleaving-wait timestamps only; overridable in tests to make attribution
  //  deterministic. Not used for anything observable to the network. Its deltas must be non-decreasing (see
  //  `monotonicNow`) so backward wall-clock adjustments can't fabricate waits.
  @VisibleForTesting
  private[output] var now: () => Instant = () => monotonicNow()

  private def enabled: Boolean = metrics.performance.enabled

  /** Anchors the open interval to now (called at queue init) so the first extracted block isn't
    * wrongly charged wall-clock time elapsed before the queue existed as wait time.
    */
  def anchorOpenInterval(): Unit =
    openIntervalStart = now()

  /** Snapshots the cumulative per-leader head-blocked totals at a block's arrival, so we can later
    * attribute its extraction wait to the leaders of the holes that delayed it. Must be called only
    * for blocks that arrived strictly above the current head hole (i.e. blocks actually buffered
    * behind a lower missing block): a head-filling block never waited in this queue, so tracking it
    * would charge it the small positive gap between its arrival snapshot and the `now` sampled in
    * [[attributeAndEmit]], spuriously attributing interleaving wait to a fully in-order block.
    * Skipped when performance metrics are disabled to avoid the snapshot allocation.
    */
  def onBlockBufferedAboveHead(blockNumber: BlockNumber): Unit =
    if (enabled)
      perBlockTracking
        .getOrElseUpdate(
          blockNumber, {
            blocksArrivedInOpenInterval += blockNumber
            InterleavingWaitTracking(
              Duration.between(openIntervalStart, now()),
              mutable.Map.from(blockedTimeByLeader),
            )
          },
        )
        .discard

  /** Reacts to a possible enable/disable transition of performance metrics: resets all bookkeeping
    * synchronously so no interval ever spans a disabled period and the first post-enable snapshot
    * is taken against a clean state.
    */
  def onMetricsEnabledMaybeChanged(): Unit =
    if (metrics.performance.enabled != enabledMirror)
      reset()

  /** Clears all interleaving-wait bookkeeping and re-bases the open interval to now. */
  private def reset(): Unit = {
    blockedTimeByLeader.clear()
    blocksArrivedInOpenInterval.clear()
    perBlockTracking.clear()
    openIntervalStart = now()
    enabledMirror = metrics.performance.enabled
  }

  /** Attributes the output Peano queue extraction wait to the leaders that ordered the delaying
    * holes, and emits the per-leader wait of each extracted block.
    *
    * The open head-blocked interval `[openIntervalStart, now]` measures how long the queue head has
    * been an empty hole. It is closed and its whole length credited to the hole's leader
    * (`filledHoleLeader`) as soon as that hole is filled, i.e. when an insertion supplies the
    * missing head block, EVEN IF the subsequent extraction is still held back by the epoch gate, as
    * the head-empty time is effectively interleaving wait regardless of whether extraction can
    * immediately proceed. A fresh (initially uncredited) interval then starts, covering any further
    * "head present but epoch-gated" wait, which is topology wait.
    *
    * Each extracted block's per-leader wait is the difference between the current cumulative totals
    * and the block's arrival snapshot (O(leaders)). Wait times for blocks that arrived during the
    * just-closed interval are considered only from their arrival, not from the interval start: we
    * set their pre-arrival fraction into their snapshot for the hole's leader (each block is
    * patched exactly once, when its arrival interval closes). This decomposes each block's
    * extraction wait into non-overlapping per-leader intervals whose sum equals the block's wait,
    * so the sum of the emitted series equals the total interleaving wait, broken down by
    * leader/segment.
    *
    * When the interval is instead closed by an epoch release draining blocks that were present all
    * along but held back by the `blockEpochNumber <= processingEpoch` gate (`filledHoleLeader` is
    * `None`), the elapsed time is topology/epoch-transition wait: we close the interval without
    * crediting any leader (so it is attributed to nobody), which also makes those extracted blocks
    * emit zero interleaving wait for this interval.
    *
    * When performance metrics are disabled, all of this is bypassed (only the open-interval start
    * is kept fresh so a later enable doesn't observe a spuriously long first interval); the
    * bookkeeping is reset synchronously on every enable/disable transition via
    * [[onMetricsEnabledMaybeChanged]].
    *
    * The two triggers are independent (hence the `||` guard): an insertion can fill the head hole
    * without extracting anything (`filledHoleLeader.isDefined`, empty `extractedBlocks`, when the
    * new head block is still epoch-gated), and an epoch release can extract previously-gated blocks
    * without filling any hole (non-empty `extractedBlocks`, `filledHoleLeader` `None`); they may
    * also both hold (a head-filling insertion that immediately extracts) or both be absent (a block
    * merely buffered above the head), in which case there is nothing to do.
    */
  def attributeAndEmit(
      extractedBlocks: Seq[CompleteBlockData],
      filledHoleLeader: Option[BftNodeId],
  ): Unit =
    if (!enabled) {
      // Keep the open-interval start fresh (on any head advance, i.e. an extraction or a filled hole) so a later
      //  enable doesn't observe an incorrectly long first interval.
      if (extractedBlocks.nonEmpty || filledHoleLeader.isDefined)
        openIntervalStart = now()
    } else if (extractedBlocks.nonEmpty || filledHoleLeader.isDefined) {
      val sampledNow = now()

      // Close the open head-blocked interval. Credit its whole length (and add buffered blocks' pre-arrival
      //  fractions) to the hole's leader only if a real head hole was just filled; otherwise (epoch release) this
      //  is topology wait, which we attribute to nobody.
      filledHoleLeader.foreach { holeLeader =>
        val blockedDuration = Duration.between(openIntervalStart, sampledNow)
        if (!blockedDuration.isNegative && !blockedDuration.isZero)
          blockedTimeByLeader
            .updateWith(holeLeader)(total =>
              Some(total.getOrElse(Duration.ZERO).plus(blockedDuration))
            )
            .discard

        // Add the pre-arrival fraction of the just-closed interval into the snapshot (for the hole's leader) of
        //  every block that arrived into it, so wait times for those blocks don't include the time before they
        //  arrived. The interval's whole length is added to the leader's cumulative total above, but a block
        //  that arrived partway through only waited for the post-arrival part; inflating its snapshot by the
        //  pre-arrival part cancels that slice out of the `cumulative - snapshot` computed at extraction.
        //  E.g. interval [0, 10] owned by leader X, block arrives at t=3 (pre-arrival fraction 3):
        //  X's cumulative gains 10, the block's snapshot for X gains 3, so its wait is 10 - 3 = 7 (t=3..10),
        //  not the full 10.
        blocksArrivedInOpenInterval.foreach { blockNumber =>
          perBlockTracking.get(blockNumber).foreach { tracking =>
            tracking.blockedTimeByLeaderSnapshot
              .updateWith(holeLeader)(atArrival =>
                Some(atArrival.getOrElse(Duration.ZERO).plus(tracking.preArrivalFraction))
              )
              .discard
          }
        }
      }
      openIntervalStart = sampledNow
      blocksArrivedInOpenInterval.clear()

      extractedBlocks.foreach { extracted =>
        val blockNumber = extracted.orderedBlockForOutput.orderedBlock.metadata.blockNumber
        // Emit exactly one total sample per extracted block, using a zero wait for in-order/epoch-gated blocks
        //  that were never tracked, so the total series counts the same per-block population as the output-stage
        //  duration and their quantiles are directly comparable.
        val blockTotalWait =
          perBlockTracking.remove(blockNumber).fold(Duration.ZERO)(emitPerLeaderWaits)
        emitTotalInterleavingWait(blockTotalWait)
      }

      // Once the queue has fully drained, no snapshot references the cumulative baseline anymore, so reset it.
      //  This bounds the cumulative map (and thus the per-arrival copy and per-extraction iteration) to the
      //  leaders active since the last drain, evicting leaders retired by validator churn instead of retaining
      //  every leader ever observed.
      if (perBlockTracking.isEmpty)
        blockedTimeByLeader.clear()
    }

  // Emits one interleaving-wait sample per leader that delayed the given block, reconstructed from the current
  //  cumulative totals and the block's arrival snapshot (into which the pre-arrival fraction has been set), and
  //  returns the sum over those leaders (the block's total interleaving wait) for the caller to emit as the single
  //  per-block total sample. Only strictly positive per-leader waits are emitted, keeping the per-leader (labeled)
  //  series free of zero-wait noise/cardinality; the total series instead gets a sample for every extracted block.
  private def emitPerLeaderWaits(tracking: InterleavingWaitTracking): Duration = {
    var blockTotalWait = Duration.ZERO
    blockedTimeByLeader.foreach { case (leader, cumulativeTotal) =>
      val atArrival = tracking.blockedTimeByLeaderSnapshot.getOrElse(leader, Duration.ZERO)
      val wait = cumulativeTotal.minus(atArrival)
      if (!wait.isNegative && !wait.isZero) {
        emitPerLeaderInterleavingWait(leader, wait)
        blockTotalWait = blockTotalWait.plus(wait)
      }
    }
    blockTotalWait
  }

  // Emits the interleaving wait of a single extracted output block, attributed to the leader that ordered the
  //  delaying hole (tagged with the `leader` label; see `stageLatencyLabels.stage.values.output.InterleavingWait`).
  private def emitPerLeaderInterleavingWait(leader: BftNodeId, duration: Duration): Unit =
    stageLatency.emitOrderingStageLatency(
      stageLatencyLabels.stage.values.output.InterleavingWait,
      duration,
    )(mc.withExtraLabels(stageLatencyLabels.leader.Key -> leader))

  // Emits the total interleaving wait of a single extracted output block (the sum over the leaders of all delaying
  //  holes, or zero for in-order/epoch-gated blocks that never waited), recorded once per extracted block without a
  //  `leader` label. Emitting it for every block (including zero-wait ones) makes the total series count the same
  //  per-block population as the output-stage duration, so their quantiles are directly comparable; the per-leader
  //  series above are means and are not additive across quantiles.
  private def emitTotalInterleavingWait(duration: Duration): Unit =
    stageLatency.emitOrderingStageLatency(
      stageLatencyLabels.stage.values.output.InterleavingTotalWait,
      duration,
    )(mc)
}

private[output] object OutputQueueInterleavingWaitTracker {

  // Per-block bookkeeping for attributing output Peano queue extraction wait to the leaders of the holes that
  //  delayed it. `blockedTimeByLeaderSnapshot` is the cumulative per-leader head-blocked total captured when the
  //  block arrived (mutated once, when the block's arrival interval closes, to set `preArrivalFraction` for that
  //  interval's leader); `preArrivalFraction` is the fraction of that interval that elapsed before the block
  //  arrived, which must not be included in wait times for the block.
  private final case class InterleavingWaitTracking(
      preArrivalFraction: Duration,
      blockedTimeByLeaderSnapshot: mutable.Map[BftNodeId, Duration],
  )
}
