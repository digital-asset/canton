// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreReader
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.BlocksRecoveredFromConsensusMessagesTest.TestStep
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.BlocksRecoveredFromConsensusMessages.LoadPoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput.Mode.FromConsensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.{
  AddMessageChunkFromRestart,
  BlockOrdered,
}
import org.scalatest.wordspec.AnyWordSpecLike

class BlocksRecoveredFromConsensusMessagesTest extends AnyWordSpecLike with BftSequencerBaseTest {

  "BlocksRecoveredFromConsensusMessages" should {
    "release messages and load new chunks" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val limit = 10
      val epochStoreReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
      val outputModule = mock[Output[ProgrammableUnitTestEnv]]
      val blocksRecoveredFromConsensusMessages =
        new OutputModule.BlocksRecoveredFromConsensusMessages[ProgrammableUnitTestEnv](
          epochStoreReader,
          limit,
          loggerFactory,
        )

      val targetEpoch = EpochNumber(18)
      val loadEpoch = EpochNumber(5)
      val loadFrom = EpochNumber(10)

      blocksRecoveredFromConsensusMessages.setTargetEpoch(
        target = targetEpoch,
        nextWhenToLoad = loadEpoch,
        nextWhereToLoadFrom = loadFrom,
      )

      blocksRecoveredFromConsensusMessages.addMessages(
        Seq(mkBlock(4), mkBlock(5), mkBlock(6), mkBlock(9))
      )

      val steps = Seq(
        TestStep(4, Seq(mkBlock(4)), None),
        TestStep(
          5,
          Seq(mkBlock(5)),
          Some(10 -> Seq(mkBlock(10), mkBlock(14), mkBlock(18))),
        ), // first load point
        TestStep(6, Seq(mkBlock(6)), None),
        TestStep(9, Seq(mkBlock(9)), None),
        TestStep(10, Seq(mkBlock(10)), None),
        TestStep(14, Seq(mkBlock(14)), None),
        TestStep(15, Seq.empty, None), // second load point but we will not load anything
        TestStep(18, Seq(mkBlock(18)), None),
        TestStep(19, Seq.empty, None),
        TestStep(
          25,
          Seq.empty,
          None,
        ), // would be third load point, but we are above target so we should not call epochStoreReader
      )

      steps.foreach { testStep =>
        testStep.messagesLoaded.foreach { case (epochToLoadFrom, messages) =>
          when(epochStoreReader.loadOrderedBlocks(EpochNumber(epochToLoadFrom.toLong), limit))
            .thenReturn(() => messages)
        }
      }

      steps.foreach { step =>
        blocksRecoveredFromConsensusMessages.releaseMessagesForEpoch(EpochNumber(step.epochNumber))

        context.extractSelfMessages() shouldBe step.messagesReleased.map(BlockOrdered(_))
        context.runPipedMessagesUntilNoMorePiped(outputModule)

        step.messagesLoaded.foreach { case (_, messages) =>
          verify(outputModule, times(1)).receive(AddMessageChunkFromRestart(messages))
          blocksRecoveredFromConsensusMessages.addMessages(messages)

        }
      }

      verify(epochStoreReader, never).loadOrderedBlocks(EpochNumber(15), limit)
      verify(epochStoreReader, never).loadOrderedBlocks(EpochNumber(25), limit)
    }

    "set new target when reaching load point" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)
      val limit = 10
      val epochStoreReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
      val blocksRecoveredFromConsensusMessages =
        new OutputModule.BlocksRecoveredFromConsensusMessages[ProgrammableUnitTestEnv](
          epochStoreReader,
          limit,
          loggerFactory,
        )
      val targetEpoch = EpochNumber(28)
      val loadEpoch = EpochNumber(5)
      val loadFrom = EpochNumber(10)

      when(epochStoreReader.loadOrderedBlocks(loadEpoch, limit)).thenReturn(() => Seq.empty)

      blocksRecoveredFromConsensusMessages.setTargetEpoch(targetEpoch, loadEpoch, loadFrom)

      val nextLoadEpoch = EpochNumber(15)
      val nextHighestLoad = EpochNumber(20)

      when(epochStoreReader.loadOrderedBlocks(nextLoadEpoch, limit)).thenReturn(() => Seq.empty)

      blocksRecoveredFromConsensusMessages.releaseMessagesForEpoch(loadEpoch)
      context.extractSelfMessages() shouldBe Seq.empty
      verify(epochStoreReader, times(1)).loadOrderedBlocks(loadFrom, limit)
      // next load point is set without needing to finish loading
      blocksRecoveredFromConsensusMessages.nextLoadPoint shouldBe Some(
        LoadPoint(nextLoadEpoch, nextHighestLoad)
      )

      blocksRecoveredFromConsensusMessages.releaseMessagesForEpoch(nextLoadEpoch)
      context.extractSelfMessages() shouldBe Seq.empty
      verify(epochStoreReader, times(1)).loadOrderedBlocks(nextHighestLoad, limit)

      // next load point is set without needing to finish loading
      blocksRecoveredFromConsensusMessages.nextLoadPoint shouldBe None
    }

    "release messages if the chunk is loaded late" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)
      val limit = 10
      val epochStoreReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
      val blocksRecoveredFromConsensusMessages =
        new OutputModule.BlocksRecoveredFromConsensusMessages[ProgrammableUnitTestEnv](
          epochStoreReader,
          limit,
          loggerFactory,
        )

      blocksRecoveredFromConsensusMessages.releaseMessagesForEpoch(EpochNumber(10))
      context.selfMessages shouldBe Seq.empty
      context.runPipedMessages() shouldBe Seq.empty

      blocksRecoveredFromConsensusMessages.addMessages(Seq(mkBlock(10)))
      context.selfMessages shouldBe Seq(BlockOrdered(mkBlock(10)))
      context.runPipedMessages() shouldBe Seq.empty

    }

    "calculate next load point correctly" in {
      Table(
        ("target", "result"),
        (19, None), // will be loaded when we load from 10
        (20, Some(LoadPoint(EpochNumber(15), EpochNumber(20)))), // requires one more load from 20
      ).forEvery { case (target, result) =>
        LoadPoint(EpochNumber(5), EpochNumber(10))
          .computeNextLoadPoint(Some(EpochNumber(target.toLong)), 10) shouldBe result
      }
    }
  }

  private def mkBlock(i: Long): OrderedBlockForOutput =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata(EpochNumber(i), BlockNumber(i)),
        Seq.empty,
        CanonicalCommitSet.empty,
      ),
      ViewNumber.First,
      BftNodeId("node"),
      isLastInEpoch = true,
      FromConsensus,
    )
}

object BlocksRecoveredFromConsensusMessagesTest {
  final case class TestStep(
      epochNumber: Long,
      messagesReleased: Seq[OrderedBlockForOutput],
      messagesLoaded: Option[(Int, Seq[OrderedBlockForOutput])],
  )
}
