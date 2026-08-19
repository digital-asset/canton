// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait EpochStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  import EpochStoreTest.*

  private def aMembership: Membership =
    Membership(
      BftNodeId("self"),
      OrderingTopology.forTesting(
        Set(BftNodeId("self"), BftNodeId("node1"))
      ),
      leaders = Seq(BftNodeId("self"), BftNodeId("node1")),
      blacklistedNodes = Seq.empty,
    )

  private[bftordering] def epochStore(
      createStore: () => EpochStore[PekkoEnv] & EpochStoreReader[PekkoEnv]
  ): Unit = {

    "completeEpoch" should {
      "create and retrieve Epochs" in {
        val store = createStore()
        val blockNumber0 = 9L
        val prePrepare0 = prePrepare(EpochNumber.First, blockNumber0)
        val commitMessages0 = commitMessages(EpochNumber.First, blockNumber0)
        val epochInfo0 = EpochInfo.forTesting(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 10L,
        )
        val epoch0 = Epoch(
          epochInfo0,
          commitMessages0.map(_.value),
        )

        val epochNumber1 = 10L
        val prePrepare1 = prePrepare(epochNumber1, 19L)
        val commitMessages1 = commitMessages(epochNumber1, 19L)

        val epochInfo1 = EpochInfo.forTesting(
          number = epochNumber1,
          startBlockNumber = 10L,
          length = 10L,
        )
        val epoch1 = Epoch(
          epochInfo1,
          commitMessages1.map(_.value),
        )

        for {
          _ <- store.startEpoch(epochInfo0)
          // idempotent writes are supported
          _ <- store.startEpoch(epochInfo0)

          e0 <- store.latestEpoch(includeInProgress = false)
          e1 <- store.latestEpoch(includeInProgress = true)

          _ <- store.addOrderedBlockAtomically(prePrepare0, commitMessages0)
          // idempotent writes are supported
          _ <- store.addOrderedBlockAtomically(prePrepare0, commitMessages0)
          e2 <- store.loadEpochInfo(EpochNumber.First)

          _ <- store.completeEpoch(epochInfo0.number)
          e3 <- store.latestEpoch(includeInProgress = false)
          e4 <- store.latestEpoch(includeInProgress = true)

          // idempotent writes are supported
          _ <- store.completeEpoch(epochInfo0.number)
          e5 <- store.latestEpoch(includeInProgress = false)
          e6 <- store.latestEpoch(includeInProgress = true)
          e7 <- store.loadEpochInfo(EpochNumber.First)

          _ <- store.startEpoch(epochInfo1)
          _ <- store.addOrderedBlockAtomically(prePrepare1, commitMessages1)
          e8 <- store.latestEpoch(includeInProgress = false)
          e9 <- store.latestEpoch(includeInProgress = true)
          e10 <- store.loadEpochInfo(epochInfo1.number)
          e11 <- store.loadEpochInfo(EpochNumber(1500L))
        } yield {
          e0 shouldBe None
          // Check that epochs can be loaded even if they don't have `lastBlockCommits`
          e1 shouldBe Some(epoch0.copy(lastBlockCommits = Seq.empty))
          e2 shouldBe Some(epochInfo0)
          e3 shouldBe Some(epoch0)
          e4 shouldBe Some(epoch0)
          e5 shouldBe Some(epoch0)
          e6 shouldBe Some(epoch0)
          e7 shouldBe Some(epochInfo0)
          e8 shouldBe Some(epoch0)
          e9 shouldBe Some(epoch1)
          e10 shouldBe Some(epochInfo1)
          e11 shouldBe None
        }
      }
    }

    "latestEpoch" should {
      "return None initially" in {
        val store = createStore()
        for {
          e0 <- store.latestEpoch(includeInProgress = false)
          e1 <- store.latestEpoch(includeInProgress = true)
        } yield {
          e0 shouldBe None
          e1 shouldBe None
        }
      }
    }

    "addOrderedBlock" should {
      "create and retrieve EpochInProgress" in {
        val store = createStore()
        val activeEpoch0Info = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, 10)
        val activeEpoch1Info = EpochInfo.forTesting(1L, 10L, 10)

        def addOrderedBlock(
            epochNumber: Long,
            blockNumber: Long,
            viewNumber: Long = ViewNumber.First,
        ) =
          store.addOrderedBlockAtomically(
            prePrepare(epochNumber, blockNumber, viewNumber),
            commitMessages(epochNumber, blockNumber, viewNumber),
          )

        for {
          _ <- store.startEpoch(activeEpoch0Info)

          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, BlockNumber.First))
          _ <- store.addPreparesAtomically(
            NonEmpty(Seq, Traced(prepare(EpochNumber.First, BlockNumber.First)))
          )

          _ <- addOrderedBlock(EpochNumber.First, BlockNumber.First)
          _ <- addOrderedBlock(EpochNumber.First, 1L)
          _ <- addOrderedBlock(EpochNumber.First, 2L)

          // these will appear in loadEpochProgress as pbftMessagesForIncompleteBlocks because block 3 is not complete
          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, 3L))
          _ <- store.addPreparesAtomically(NonEmpty(Seq, Traced(prepare(EpochNumber.First, 3L))))

          // view change messages will appear always because we don't check in the DB if the segment has finished
          _ <- store.addViewChangeMessage(viewChange(EpochNumber.First, 0L))
          _ <- store.addViewChangeMessage(newView(EpochNumber.First, 0L))

          // in-progress messages for later views are accounted for separately
          _ <- store.addPrePrepare(
            prePrepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1)
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(Seq, Traced(prepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1)))
          )

          _ <- store.addViewChangeMessage(viewChange(EpochNumber.First, 0L, ViewNumber(1L)))

          e0 <- store.loadEpochProgress(
            EpochState.Epoch(activeEpoch0Info, aMembership, aMembership)
          )

          // updating an existing row should be ignored
          _ <- addOrderedBlock(
            EpochNumber.First,
            BlockNumber.First,
            viewNumber = ViewNumber.First + 1,
          )

          _ <- store.startEpoch(activeEpoch1Info)

          // test out-of-order and gap inserts in new activeEpoch
          _ <- addOrderedBlock(1L, 13L)
          _ <- addOrderedBlock(1L, 10L)
          _ <- addOrderedBlock(1L, 11L)

          e1 <- store.loadEpochProgress(
            EpochState.Epoch(activeEpoch1Info, aMembership, aMembership)
          )
        } yield {
          e0 should matchPattern {
            case EpochInProgress(
                  completedBlocks,
                  pbftMessagesForIncompleteBlocks,
                )
                if completedBlocks == Seq(BlockNumber.First, 1L, 2L).map(n =>
                  Block(
                    activeEpoch0Info.number,
                    BlockNumber(n),
                    CommitCertificate(
                      prePrepare(activeEpoch0Info.number, n),
                      commitMessages(activeEpoch0Info.number, n).map(_.value),
                    ),
                  )
                ) &&
                  pbftMessagesForIncompleteBlocks.toSet ==
                  Set[SignedMessage[PbftNetworkMessage]](
                    // prePrepare and prepare for block 0 are filtered out because it has been completed
                    // viewChange at ViewNumber.First is filtered out because we only consider the ones on views higher than the highest NewView
                    newView(EpochNumber.First, 0L),
                    viewChange(EpochNumber.First, 0L, ViewNumber(1L)),
                    prePrepare(EpochNumber.First, 3L),
                    prepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1),
                  ) =>
          }
          e1 shouldBe EpochInProgress(
            Seq(10L, 11L, 13L).map(n =>
              Block(
                activeEpoch1Info.number,
                BlockNumber(n),
                CommitCertificate(
                  prePrepare(activeEpoch1Info.number, n),
                  commitMessages(activeEpoch1Info.number, n).map(_.value),
                ),
              )
            ),
            Seq.empty,
          )
        }
      }
    }

    "loadEpochProgress" should {
      "load only the relevant high view-numbered messages" in {
        val store = createStore()
        val epochInfo = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, 10)

        def addOrderedBlock(
            epochNumber: Long,
            blockNumber: Long,
            viewNumber: Long = ViewNumber.First,
        ) =
          store.addOrderedBlockAtomically(
            prePrepare(epochNumber, blockNumber, viewNumber),
            commitMessages(epochNumber, blockNumber, viewNumber, numVotes = 2),
          )

        // Note: in this example, there are two segments: even (BlockNumber.First) and odd (blockNumber=1L)
        for {
          _ <- store.startEpoch(epochInfo)

          // b0 and b1 are completed with commit sets
          _ <- addOrderedBlock(EpochNumber.First, BlockNumber.First)
          _ <- addOrderedBlock(EpochNumber.First, 1L)

          // b2 partially completes in view0
          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, 2L))
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, 2L)),
              Traced(prepare(EpochNumber.First, 2L, from = "node1")),
            )
          )

          // We load the highest view-numbered quorum of prepares with the corresponding PrePrepare or NewView
          // for each block number.

          // Simulate prepare quorums from different views for a few block numbers

          // LOAD: this will be the highest view number prepare quorum for block 3
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 1L)
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, blockNumber = 3L, viewNumber = 1L)),
              Traced(prepare(EpochNumber.First, blockNumber = 3L, viewNumber = 1L, from = "node1")),
            )
          )
          // SKIP: this newView and prepare quorum for block5 gets superseded below with a quorum at view3
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 2L)
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, blockNumber = 5L, viewNumber = 2L)),
              Traced(prepare(EpochNumber.First, blockNumber = 5L, viewNumber = 2L, from = "node1")),
            )
          )
          // LOAD: this quorum of prepares for blocks 5 and 7 share the NewView message for view 3
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 3L)
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, blockNumber = 5L, viewNumber = 3L)),
              Traced(prepare(EpochNumber.First, blockNumber = 5L, viewNumber = 3L, from = "node1")),
            )
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, blockNumber = 7L, viewNumber = 3L)),
              Traced(prepare(EpochNumber.First, blockNumber = 7L, viewNumber = 3L, from = "node1")),
            )
          )
          // LOAD: for the even segment, this newView for view=6 and the prepare quorum get loaded
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 6L)
          )
          _ <- store.addPreparesAtomically(
            NonEmpty(
              Seq,
              Traced(prepare(EpochNumber.First, blockNumber = 6L, viewNumber = 6L)),
              Traced(prepare(EpochNumber.First, blockNumber = 6L, viewNumber = 6L, from = "node1")),
            )
          )

          // === NewView messages ===
          // For each segment, normally load just the highest view-numbered NewView message
          // These newViews are lower view numbers than the above (EvenSegment, View=6) above, so these
          // are not loaded from the DB.
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 1L)
          )
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 2L)
          )
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 3L)
          )
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 4L)
          )
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, viewNumber = 5L)
          )

          // We also load the highest view-numbered NewView even if there isn't a prepare quorum
          // Simulate that for odd segment
          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 7L)
          )

          // === ViewChange messages ===
          // For ViewChange messages, load the highest ViewChange the one right before, it if exists
          // Here, we expect view2 and view3 to be loaded
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, BlockNumber.First, viewNumber = 1L)
          )
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, BlockNumber.First, viewNumber = 2L)
          )
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, BlockNumber.First, viewNumber = 3L)
          )

          // Here, we simulate that the node jumped ahead in views, so there is no previous and
          // only the highest viewChange message should load
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, segmentNumber = 1L, viewNumber = 1L)
          )
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, segmentNumber = 1L, viewNumber = 2L)
          )
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, segmentNumber = 1L, viewNumber = 8L)
          )

          e0 <- store.loadEpochProgress(EpochState.Epoch(epochInfo, aMembership, aMembership))
        } yield {
          e0 should matchPattern {
            case EpochInProgress(
                  completedBlocks,
                  pbftMessagesForIncompleteBlocks,
                )
                if completedBlocks == Seq(BlockNumber.First, 1L).map(n =>
                  Block(
                    epochInfo.number,
                    BlockNumber(n),
                    CommitCertificate(
                      prePrepare(epochInfo.number, n),
                      commitMessages(epochInfo.number, n, numVotes = 2).map(_.value),
                    ),
                  )
                ) &&
                  pbftMessagesForIncompleteBlocks.toSet ==
                  Set[SignedMessage[PbftNetworkMessage]](
                    prePrepare(EpochNumber.First, 2L),
                    prepare(EpochNumber.First, 2L),
                    prepare(EpochNumber.First, 2L, from = "node1"),
                    prepare(EpochNumber.First, 3L, 1L),
                    prepare(EpochNumber.First, 3L, 1L, from = "node1"),
                    prepare(EpochNumber.First, 5L, 3L),
                    prepare(EpochNumber.First, 5L, 3L, from = "node1"),
                    prepare(EpochNumber.First, 7L, 3L),
                    prepare(EpochNumber.First, 7L, 3L, from = "node1"),
                    prepare(EpochNumber.First, 6L, 6L),
                    prepare(EpochNumber.First, 6L, 6L, from = "node1"),
                    newView(EpochNumber.First, BlockNumber.First, viewNumber = 6L),
                    newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 1L),
                    newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 3L),
                    newView(EpochNumber.First, segmentNumber = 1L, viewNumber = 7L),
                    viewChange(EpochNumber.First, segmentNumber = 1L, viewNumber = 8L),
                  ) =>
          }
        }
      }

      "filter out all messages from blocks and segments that are complete (including view change and new views)" in {
        val store = createStore()
        val epochInfo = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, 4)

        def addOrderedBlock(
            epochNumber: Long,
            blockNumber: Long,
            viewNumber: Long = ViewNumber.First,
        ) =
          store.addOrderedBlockAtomically(
            prePrepare(epochNumber, blockNumber, viewNumber),
            commitMessages(epochNumber, blockNumber, viewNumber, numVotes = 2),
          )

        for {
          _ <- store.startEpoch(epochInfo)

          // Add some messages for block 1
          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, BlockNumber.First))
          _ <- store.addPreparesAtomically(
            NonEmpty(Seq, Traced(prepare(EpochNumber.First, BlockNumber.First)))
          )

          _ <- store.addViewChangeMessage(
            newView(EpochNumber.First, BlockNumber.First, ViewNumber(1))
          )
          _ <- store.addViewChangeMessage(
            viewChange(EpochNumber.First, BlockNumber.First, ViewNumber(2))
          )

          e0 <- store.loadEpochProgress(EpochState.Epoch(epochInfo, aMembership, aMembership))

          // Complete block 0
          _ <- addOrderedBlock(EpochNumber.First, BlockNumber.First)
          e1 <- store.loadEpochProgress(EpochState.Epoch(epochInfo, aMembership, aMembership))

          // Complete segment 0
          _ <- addOrderedBlock(EpochNumber.First, BlockNumber(2))
          e2 <- store.loadEpochProgress(EpochState.Epoch(epochInfo, aMembership, aMembership))
        } yield {
          e0 should matchPattern {
            case EpochInProgress(Seq(), pbftMessagesForIncompleteBlocks)
                if pbftMessagesForIncompleteBlocks.toSet ==
                  Set[SignedMessage[PbftNetworkMessage]](
                    prePrepare(EpochNumber.First, BlockNumber.First),
                    prepare(EpochNumber.First, BlockNumber.First),
                    newView(EpochNumber.First, BlockNumber.First, ViewNumber(1)),
                    viewChange(EpochNumber.First, BlockNumber.First, ViewNumber(2)),
                  ) =>
          }
          e1 should matchPattern {
            case EpochInProgress(completedBlocks, pbftMessagesForIncompleteBlocks)
                if (completedBlocks.map(_.blockNumber).toSet == Set(
                  BlockNumber.First
                )) && pbftMessagesForIncompleteBlocks.toSet ==
                  Set[SignedMessage[PbftNetworkMessage]](
                    newView(EpochNumber.First, BlockNumber.First, ViewNumber(1)),
                    viewChange(EpochNumber.First, BlockNumber.First, ViewNumber(2)),
                  ) =>
          }

          e2 should matchPattern {
            case EpochInProgress(completedBlocks, pbftMessagesForIncompleteBlocks)
                if (completedBlocks.map(_.blockNumber).toSet == Set(
                  BlockNumber.First,
                  BlockNumber(2),
                )) && pbftMessagesForIncompleteBlocks.isEmpty =>
          }
        }
      }
    }

    "loadPrePrepares" should {
      "load pre-prepares" in {
        val store = createStore()
        val epoch0 = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, 1)
        val epoch1 = EpochInfo.forTesting(1L, 1L, 1)
        val epoch2 = EpochInfo.forTesting(2L, 2L, 2)
        val epoch3 = EpochInfo.forTesting(3L, 3L, 3)
        for {
          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(EpochNumber.First, BlockNumber.First),
            commitMessages(EpochNumber.First, BlockNumber.First),
          )
          _ <- store.startEpoch(epoch2)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = 2L, blockNumber = 2L),
            commitMessages(epochNumber = 2L, blockNumber = 2L),
          )
          _ <- store.startEpoch(epoch1)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = 1L, blockNumber = 1L),
            commitMessages(epochNumber = 1L, blockNumber = 1L),
          )
          _ <- store.startEpoch(epoch3)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = 3L, blockNumber = 3L),
            commitMessages(epochNumber = 3L, blockNumber = 3L),
          )
          blocks <- store.loadCompleteBlocks(
            startEpochNumberInclusive = EpochNumber(1L),
            endEpochNumberInclusive = EpochNumber(2L),
          )
        } yield {
          blocks shouldBe Seq(
            Block(
              EpochNumber(1L),
              BlockNumber(1L),
              CommitCertificate(prePrepare(1L, 1L), commitMessages(1L, 1L).map(_.value)),
            ),
            Block(
              EpochNumber(2L),
              BlockNumber(2L),
              CommitCertificate(prePrepare(2L, 2L), commitMessages(2L, 2L).map(_.value)),
            ),
          )
        }
      }
    }

    "loadOrderedBlocks" should {
      "load ordered blocks" in {
        val store = createStore()
        val epoch0 = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, length = 2)

        val expectedOrderedBlocks =
          Seq(
            orderedBlock(BlockNumber.First, isLastInEpoch = false),
            orderedBlock(BlockNumber(1), isLastInEpoch = true),
          )

        for {
          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber.First),
            Seq.empty,
          )
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber(1)),
            Seq.empty,
          )
          blocks <- store.loadOrderedBlocks(initialEpochNumber = EpochNumber.First, 10)
        } yield {
          blocks should contain theSameElementsInOrderAs expectedOrderedBlocks
        }
      }

      "load should respect limit" in {
        val store = createStore()
        val epoch0 = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, length = 2)

        val expectedOrderedBlocks =
          Seq(
            orderedBlock(BlockNumber.First, isLastInEpoch = false),
            orderedBlock(BlockNumber(1), isLastInEpoch = true),
          )

        for {
          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber.First),
            Seq.empty,
          )
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber(1)),
            Seq.empty,
          )
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber(1), blockNumber = BlockNumber(2)),
            Seq.empty,
          )
          blocks <- store.loadOrderedBlocks(initialEpochNumber = EpochNumber.First, limit = 1)
        } yield {
          blocks should contain theSameElementsInOrderAs expectedOrderedBlocks
        }
      }
    }

    "last completed block" should {
      "return epoch of last completed block" in {
        val store = createStore()

        val lowerBound = EpochNumber(5)
        val epochNumber = EpochNumber(13)

        for {
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber.First),
            Seq.empty,
          )
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = lowerBound, blockNumber = BlockNumber(1)),
            Seq.empty,
          )
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = epochNumber, blockNumber = BlockNumber(2)),
            Seq.empty,
          )
          epoch <- store.lastEpochWithCompletedBlock(lowerBound)
        } yield {
          epoch shouldBe Some(epochNumber)
        }
      }
    }

    "prune" should {
      "delete epochs, messages for completed blocks and messages for in progress block" in {
        val store = createStore()
        val epoch0 = EpochInfo.forTesting(EpochNumber.First, BlockNumber.First, length = 2)
        val epoch1 = EpochInfo.forTesting(1L, 1L, 1)
        val epoch2 = EpochInfo.forTesting(2L, 2L, 2)
        for {
          numberOfRecords0 <- store.loadNumberOfRecords
          _ = numberOfRecords0 shouldBe (EpochStore.NumberOfRecords.empty)

          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(EpochNumber.First, BlockNumber.First),
            commitMessages(EpochNumber.First, BlockNumber.First),
          )

          numberOfRecords1 <- store.loadNumberOfRecords
          _ = numberOfRecords1 shouldBe (EpochStore.NumberOfRecords(
            epochs = 1L,
            pbftMessagesCompleted = 4L,
            pbftMessagesInProgress = 0,
          ))

          _ <- store.startEpoch(epoch1)
          _ <- store.addOrderedBlockAtomically(
            prePrepare(epochNumber = 1L, blockNumber = 1L),
            commitMessages(epochNumber = 1L, blockNumber = 1L),
          )

          numberOfRecords2 <- store.loadNumberOfRecords
          _ = numberOfRecords2 shouldBe (EpochStore.NumberOfRecords(
            epochs = 2L,
            pbftMessagesCompleted = 8L,
            pbftMessagesInProgress = 0,
          ))

          _ <- store.startEpoch(epoch2)
          _ <- store.addPrePrepare(prePrepare(EpochNumber(2L), 3L))
          _ <- store.addPreparesAtomically(NonEmpty(Seq, Traced(prepare(EpochNumber(2L), 3L))))
          _ <- store.addViewChangeMessage(viewChange(EpochNumber(2L), 3L))
          _ <- store.addViewChangeMessage(newView(EpochNumber(2L), 3L))

          numberOfRecords3 <- store.loadNumberOfRecords
          _ = numberOfRecords3 shouldBe (EpochStore.NumberOfRecords(
            epochs = 3L,
            pbftMessagesCompleted = 8L,
            pbftMessagesInProgress = 4,
          ))

          _ <- store.prune(epochNumberExclusive = EpochNumber(1L))
          numberOfRecordsAfterPrune1 <- store.loadNumberOfRecords
          _ = numberOfRecordsAfterPrune1 shouldBe (EpochStore.NumberOfRecords(
            epochs = 2L,
            pbftMessagesCompleted = 4L,
            pbftMessagesInProgress = 4,
          ))

          _ <- store.prune(epochNumberExclusive = EpochNumber(2L))
          numberOfRecordsAfterPrune2 <- store.loadNumberOfRecords
          _ = numberOfRecordsAfterPrune2 shouldBe (EpochStore.NumberOfRecords(
            epochs = 1L,
            pbftMessagesCompleted = 0L,
            pbftMessagesInProgress = 4,
          ))

          _ <- store.prune(epochNumberExclusive = EpochNumber(3L))
          numberOfRecordsAfterPrune3 <- store.loadNumberOfRecords
          _ = numberOfRecordsAfterPrune3 shouldBe (EpochStore.NumberOfRecords(
            epochs = 0L,
            pbftMessagesCompleted = 0L,
            pbftMessagesInProgress = 0,
          ))

        } yield succeed
      }
    }
  }
}

object EpochStoreTest {

  private def prePrepare(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
  )(implicit synchronizerProtocolVersion: ProtocolVersion) = PrePrepare
    .create(
      BlockMetadata.mk(epochNumber, blockNumber),
      ViewNumber(viewNumber),
      OrderingBlock.empty,
      CanonicalCommitSet(Set.empty),
      from = BftNodeId("self"),
    )
    .fakeSign

  private def prepare(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
      from: String = "self",
  )(implicit synchronizerProtocolVersion: ProtocolVersion) =
    Prepare
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(viewNumber),
        Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        from = BftNodeId(from),
      )
      .fakeSign

  private def commitMessages(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
      numVotes: Long = 3,
  )(implicit synchronizerProtocolVersion: ProtocolVersion, traceContext: TraceContext) =
    (0L until numVotes).map { i =>
      Traced(
        Commit
          .create(
            BlockMetadata.mk(epochNumber, blockNumber),
            ViewNumber(viewNumber),
            Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
            CantonTimestamp.Epoch,
            from = BftNodeId(s"node$i"),
          )
          .fakeSign
      )
    }

  def viewChange(
      epochNumber: Long,
      segmentNumber: Long,
      viewNumber: Long = ViewNumber.First,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): SignedMessage[ViewChange] =
    ViewChange
      .create(
        BlockMetadata.mk(epochNumber, segmentNumber),
        ViewNumber(viewNumber),
        consensusCerts = Seq.empty,
        BftNodeId("self"),
      )
      .fakeSign

  def newView(
      epochNumber: Long,
      segmentNumber: Long,
      viewNumber: Long = ViewNumber.First,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): SignedMessage[NewView] =
    NewView
      .create(
        BlockMetadata.mk(epochNumber, segmentNumber),
        viewNumber = ViewNumber(viewNumber),
        viewChanges = Seq.empty,
        prePrepares = Seq.empty,
        BftNodeId("self"),
      )
      .fakeSign

  private def orderedBlock(blockNumber: BlockNumber, isLastInEpoch: Boolean) =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata.mk(EpochNumber.First, blockNumber),
        batchRefs = Seq.empty,
        CanonicalCommitSet.empty,
      ),
      ViewNumber.First,
      BftNodeId("self"),
      isLastInEpoch,
      OrderingMode.Consensus,
    )
}
