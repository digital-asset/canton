// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology

import EpochState.Segment

/** Tells whether the ordering node's local segment should sequence _some_ block, even if that block
  * is empty, because it is potentially blocking progress within the epoch (including across other
  * nodes' segments) or when it comes to BFT time.
  *
  * Created once per epoch; the state is transient to detect lack of progress and doesn't need to be
  * preserved across crashes.
  */
class BlockedProgressDetector(
    mySegment: Segment,
    otherLeadersToSegmentState: Map[BftNodeId, Segment],
    isBlockComplete: BlockNumber => Boolean,
    isBlockEmpty: BlockNumber => Boolean,
) {

  def isProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean = isEpochProgressBlocked(
    nextRelativeBlockIndexToFill
  )

  // Note that up to f nodes can be blacklisted. (this is also configurable via topology transactions, but expected not to change)
  // Here, we consider only the active leaders as the total set size
  // Then, we consider most segments are complete if a sufficient number of peers' segments are already complete:
  //  - the minimum acceptable set size is f+1 -- (we can't allow only the bad nodes to force all other peers to rush complete their segment)
  //  - the maximum acceptable set size is total - 1 -- (this is the case were only the local segment is still outstanding)
  // In the code below, we are using a strong quorum of the active leaders' set, which is considered a happy medium between the above min and max
  def areMostSegmentsComplete: Boolean = {
    val numberOfSegments = otherLeadersToSegmentState.size + 1
    val numberOfOtherSegmentsComplete =
      otherLeadersToSegmentState.values.count(_.slotNumbers.forall(isBlockComplete))
    OrderingTopology.isStrongQuorumReached(numberOfSegments, numberOfOtherSegmentsComplete)
  }

  // The heuristic we use asks the following question:
  // As a leader, for the next relative block index I must fill, are there leaders in other
  // segments that have a non-empty block already ordered at the same relative index or higher?
  // When this leader gets to the last slot in the segment, special care must be taken to
  // consider other leaders whose segments may be shorter than the one of this leader.
  // In that case, if we looked just at their slot at the same relative index as our own, we would not find
  // anything and possibly miss that they might have completed their segment. So if are at our last slot, we
  // check whether another leader finished their last slot (regardless of it being an empty block or not)
  // to unblock them from going to the next epoch. An advantage of only checking that when we are at the last
  // slot is that we avoid that a view-change in another segment would make us rush to fill our segment with empty blocks.
  private def isEpochProgressBlocked(nextRelativeBlockIndexToFill: Int): Boolean = {
    val lastSlotToFill = nextRelativeBlockIndexToFill == mySegment.slotNumbers.size - 1
    otherLeadersToSegmentState.values
      .map(_.slotNumbers)
      .exists { slotNumbers =>
        (lastSlotToFill && isBlockComplete(slotNumbers.last1)) ||
        slotNumbers
          .drop(nextRelativeBlockIndexToFill)
          .exists(slot => isBlockComplete(slot) && !isBlockEmpty(slot))
      }
  }
}
