// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.{
  SendPbftMessage,
  StorePrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.PrePrepareStoreCoordinator.MySegmentCoordinator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  PrePrepare,
  PrePrepareStored,
}
import org.scalatest.wordspec.AsyncWordSpec

class PrePrepareStoreCoordinatorTest extends AsyncWordSpec with BftSequencerBaseTest {

  private val defaultSlotNumbers: NonEmpty[Seq[BlockNumber]] =
    NonEmpty.mk(Seq, BlockNumber.First, 4L, 8L, 12L, 16L, 20L).map(BlockNumber(_))
  private val oneBlockSlotNumbers: NonEmpty[Seq[BlockNumber]] =
    NonEmpty.mk(Seq, BlockNumber.First).map(BlockNumber(_))
  private val myId: BftNodeId = BftNodeId("self")

  private def createCoordinator(
      completedBlocks: Seq[Block],
      slotNumbers: NonEmpty[Seq[BlockNumber]] = defaultSlotNumbers,
  ): MySegmentCoordinator =
    new MySegmentCoordinator(
      segment = Segment(myId, slotNumbers),
      completedBlocks = completedBlocks,
      abort = fail(_),
    )

  "MySegmentCoordinator" should {
    "send the first block pre-prepare immediately on view 0" in {
      val coordinator = createCoordinator(completedBlocks = Seq.empty)
      val sendFirst = sendPrePrepare(BlockNumber.First)
      coordinator.canStoreAndSendPrePrepare(sendFirst) shouldBe true
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber.First)) shouldBe empty
    }

    "have no issues when getting to the end of segment" in {
      // segment with only one block
      val coordinator = createCoordinator(completedBlocks = Seq.empty, oneBlockSlotNumbers)
      val sendFirst = sendPrePrepare(BlockNumber.First)
      coordinator.canStoreAndSendPrePrepare(sendFirst) shouldBe true
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber.First)) shouldBe empty
    }

    "postpone a future pre-prepare until the previous block pre-prepare is stored" in {
      val coordinator = createCoordinator(completedBlocks = Seq.empty)
      val sendFirst = sendPrePrepare(BlockNumber.First)
      val sendSecond = sendPrePrepare(BlockNumber(4L))
      val sendThird = sendPrePrepare(BlockNumber(8L))

      coordinator.canStoreAndSendPrePrepare(sendSecond) shouldBe false
      coordinator.canStoreAndSendPrePrepare(sendFirst) shouldBe true
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber.First)) should contain(
        sendSecond
      )
      coordinator.canStoreAndSendPrePrepare(sendThird) shouldBe false
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber(4L))) should contain(sendThird)
    }

    "postpone multiple future pre-prepares until the previous ones are stored" in {
      val coordinator = createCoordinator(completedBlocks = Seq.empty)
      val sendFirst = sendPrePrepare(BlockNumber.First)
      val sendSecond = sendPrePrepare(BlockNumber(4L))
      val sendThird = sendPrePrepare(BlockNumber(8L))

      coordinator.canStoreAndSendPrePrepare(sendSecond) shouldBe false
      coordinator.canStoreAndSendPrePrepare(sendThird) shouldBe false
      coordinator.canStoreAndSendPrePrepare(sendFirst) shouldBe true

      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber.First)) should contain(
        sendSecond
      )
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber(4L))) should contain(sendThird)
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber(8L))) shouldBe None
    }

    "start after already completed blocks" in {
      val coordinator = createCoordinator(completedBlocks = Seq(completedBlock(BlockNumber.First)))
      val sendSecond = sendPrePrepare(BlockNumber(4L))
      val sendThird = sendPrePrepare(BlockNumber(8L))

      coordinator.canStoreAndSendPrePrepare(sendSecond) shouldBe true
      coordinator.canStoreAndSendPrePrepare(sendThird) shouldBe false
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber(4L))) should contain(
        sendThird
      )
    }

    "start after already completed blocks and rehydrated store messages" in {
      val coordinator = createCoordinator(completedBlocks =
        Seq(completedBlock(BlockNumber.First), completedBlock(BlockNumber(8L)))
      )
      // as if from rehydration
      coordinator.onPrePrepareStored(prePrepareStored(BlockNumber(4L))) shouldBe None

      val sendFourth = sendPrePrepare(BlockNumber(12L))
      coordinator.canStoreAndSendPrePrepare(sendFourth) shouldBe true
    }

    "just accept all from views above 0" in {
      val coordinator = createCoordinator(completedBlocks = Seq.empty)
      val sendSecondInLaterView = sendPrePrepare(BlockNumber(4L), view = ViewNumber(1L))
      val sendThirdInLaterView = sendPrePrepare(BlockNumber(8L), view = ViewNumber(1L))

      coordinator.canStoreAndSendPrePrepare(sendThirdInLaterView) shouldBe true
      coordinator.canStoreAndSendPrePrepare(sendSecondInLaterView) shouldBe true
      coordinator.onPrePrepareStored(
        prePrepareStored(BlockNumber.First, ViewNumber(1L))
      ) shouldBe None
      coordinator.onPrePrepareStored(
        prePrepareStored(BlockNumber(8L), ViewNumber(1L))
      ) shouldBe None
      coordinator.onPrePrepareStored(
        prePrepareStored(BlockNumber(4L), ViewNumber(1L))
      ) shouldBe None
    }
  }

  private def sendPrePrepare(
      blockNumber: BlockNumber,
      view: ViewNumber = ViewNumber.First,
  ): SendPbftMessage[PrePrepare] = {
    val prePrepare = createPrePrepare(blockNumber, view, myId)
    SendPbftMessage(prePrepare, Some(StorePrePrepare(prePrepare)), traceContext)
  }

  private def prePrepareStored(
      blockNumber: BlockNumber,
      view: ViewNumber = ViewNumber.First,
  ): PrePrepareStored =
    PrePrepareStored(BlockMetadata(EpochNumber.First, blockNumber), view)

  private def createPrePrepare(
      blockNumber: BlockNumber,
      view: ViewNumber,
      from: BftNodeId,
  ): SignedMessage[PrePrepare] =
    SegmentStateTest.createBottomPrePrepare(blockNumber, view, from)

  private def completedBlock(blockNumber: BlockNumber): Block =
    Block(
      epochNumber = EpochNumber.First,
      blockNumber = blockNumber,
      commitCertificate =
        SegmentStateTest.createCommitCertificate(blockNumber, ViewNumber.First, myId),
    )
}
