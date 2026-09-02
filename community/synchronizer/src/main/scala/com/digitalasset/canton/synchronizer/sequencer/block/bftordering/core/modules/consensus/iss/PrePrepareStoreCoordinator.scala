// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.PeanoQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.SendPbftMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  PrePrepare,
  PrePrepareStored,
}

import scala.collection.mutable

trait PrePrepareStoreCoordinator {
  def onPrePrepareStored(stored: PrePrepareStored): Option[SendPbftMessage[PrePrepare]]
  def canStoreAndSendPrePrepare(send: SendPbftMessage[PrePrepare]): Boolean
}

object PrePrepareStoreCoordinator {

  def apply(
      segment: Segment,
      membership: Membership,
      completedBlocks: Seq[Block],
      abort: String => Nothing,
  ): PrePrepareStoreCoordinator = {
    val isLeaderOfThisView: Boolean = membership.myId == segment.originalLeader
    if (isLeaderOfThisView)
      new MySegmentCoordinator(segment, completedBlocks, abort)
    else new NoopCoordinator
  }

  class NoopCoordinator extends PrePrepareStoreCoordinator {
    override def onPrePrepareStored(stored: PrePrepareStored): Option[SendPbftMessage[PrePrepare]] =
      None
    override def canStoreAndSendPrePrepare(send: SendPbftMessage[PrePrepare]): Boolean =
      true
  }

  /** Coordinator for the segment that this node is responsible for filling (leader). It keeps track
    * of the earliest block number in the segment that has not been stored yet and delays storing
    * and sending pre-prepare messages for later blocks until the earlier blocks are stored
    */
  class MySegmentCoordinator(
      segment: Segment,
      completedBlocks: Seq[Block],
      abort: String => Nothing,
  ) extends PrePrepareStoreCoordinator {
    // contains send pre-prepare messages that we delay because there are earlier pre-prepares that have not been stored yet
    private val delayedSendAndStoreMsgMap =
      mutable.HashMap[BlockNumber, SendPbftMessage[PrePrepare]]()
    // to keep track of the earliest block number in the segment whose pre-prepare has not been stored yet
    private val storedPrePrepareQueue = new PeanoQueue[Long, BlockNumber](0L)(abort)

    // consider completed blocks to have pre-prepares stored and move head forward
    completedBlocks.foreach { b =>
      val blockSegmentIndex = segment.slotNumbers.indexOf(b.blockNumber).toLong
      storedPrePrepareQueue.insert(blockSegmentIndex, b.blockNumber)
    }
    storedPrePrepareQueue.pollAvailable().discard

    // head points to the earliest block number in the segment that has not been stored yet
    private def headBlockNumber = segment.slotNumbers.lift(storedPrePrepareQueue.head.v.toInt)

    // if the pre-prepare is for a block number that is greater than the head block number,
    // we delay sending it until the previous pre-prepares are stored
    def canStoreAndSendPrePrepare(send: SendPbftMessage[PrePrepare]): Boolean = {
      val msg = send.pbftMessage.message
      val msgBlockNumber = msg.blockMetadata.blockNumber
      headBlockNumber.filter(head => msg.viewNumber == 0 && msgBlockNumber > head).fold(true) { _ =>
        delayedSendAndStoreMsgMap.put(msgBlockNumber, send).discard
        false
      }
    }

    // when a pre-prepare is stored (when processing a SendPbftMessage[PrePrepare]), the PrePrepareStored event is emitted,
    // and we can check if there are any previously delayed pre-prepares that can now be sent (and stored)
    def onPrePrepareStored(stored: PrePrepareStored): Option[SendPbftMessage[PrePrepare]] =
      if (
        stored.viewNumber == 0 && segment.slotNumbers.contains(stored.blockMetadata.blockNumber)
      ) {
        val blockSegmentIndex = segment.slotNumbers.indexOf(stored.blockMetadata.blockNumber).toLong
        storedPrePrepareQueue.insert(blockSegmentIndex, stored.blockMetadata.blockNumber)
        storedPrePrepareQueue.pollAvailable().discard
        headBlockNumber.fold[Option[SendPbftMessage[PrePrepare]]](None) { head =>
          delayedSendAndStoreMsgMap.remove(head)
        }
      } else None

  }

}
