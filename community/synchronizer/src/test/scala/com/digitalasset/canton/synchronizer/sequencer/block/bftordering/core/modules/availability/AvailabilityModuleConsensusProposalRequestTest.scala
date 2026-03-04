// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.crypto.Signature.noSignature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType.BftSignedAvailabilityMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  FakePipeToSelfQueueUnitTestContext,
  FakePipeToSelfQueueUnitTestEnv,
  IgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequestBatch,
  OrderingRequestBatchStats,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeCellModule,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
  fakeRecordingModule,
}
import com.digitalasset.canton.tracing.Traced
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AvailabilityModuleConsensusProposalRequestTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives Consensus.CreateProposal (from local consensus) and " +
      "there are no batches ready for ordering" should {

        "record the proposal request" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val mempoolCell = new AtomicReference[Option[Mempool.Message]](None)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)

          val availability =
            createAndStartAvailability[IgnoringUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              mempool = fakeCellModule(mempoolCell),
              consensus = fakeCellModule(consensusCell),
            )
          mempoolCell.get() should contain(
            Mempool.CreateLocalBatches(
              (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier).toShort
            )
          )
          consensusCell.get() shouldBe empty

          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNode0,
                failingCryptoProvider,
              )
          )

          consensusCell.get() should contain(Consensus.LocalAvailability.NoProposalAvailableYet)

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.nextToBeProvidedToConsensus shouldBe ANextToBeProvidedToConsensus
        }
      }

    "it receives the same topology (from local consensus)" should {
      "Change nothing in the dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        implicit val actorContext
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = false)
        val availabilityStore =
          new FakeAvailabilityStore[ProgrammableUnitTestEnv](
            TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
          )
        val availability =
          createAndStartAvailability[ProgrammableUnitTestEnv](
            otherNodes = Set(Node1),
            disseminationProtocolState = disseminationProtocolState,
            availabilityStore = availabilityStore,
            consensus = fakeIgnoringModule,
            p2pNetworkOut =
              fakeModuleExpectingSilence, // Will throw if anything is sent out to the network
          )
        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0And1WithNode0Vote
        )

        availability.receive(
          Availability.Consensus
            .CreateProposal(
              BlockNumber.First,
              EpochNumber.First,
              OrderingTopologyNodes0And1,
              failingCryptoProvider,
            )
        )

        disseminationProtocolState.disseminationProgress should contain only ABatchDisseminationProgressNode0And1WithNode0Vote // No review
        disseminationProtocolState.nextToBeProvidedToConsensus shouldBe ANextToBeProvidedToConsensus
      }
    }

    "it receives a topology (from local consensus) and there are disseminated in-progress batches" when {

      "they are fully disseminated" should {
        "not disseminate them again" in {

          val disseminationProtocolState = new DisseminationProtocolState()

          implicit val actorContext
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext(resolveAwaits = false)
          val availabilityStore =
            new FakeAvailabilityStore[ProgrammableUnitTestEnv](
              TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
            )
          val availability =
            createAndStartAvailability[ProgrammableUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeIgnoringModule,
              p2pNetworkOut =
                fakeModuleExpectingSilence, // Will throw if anything is sent out to the network
            )
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To6WithNonQuorumVotes._1 ->
              ABatchDisseminationProgressNode0To6WithNonQuorumVotes._2
                .copy(batchSentTo = Node1To6)
          )

          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNodes0To6,
                failingCryptoProvider,
              )
          )

          // No fetches nor self-messages because all batches are already fully disseminated
          actorContext.runPipedMessages() shouldBe empty
          actorContext.extractSelfMessages() shouldBe empty
        }
      }

      "they are partially disseminated" should {
        "disseminate them partially" in {
          val disseminationProtocolState = new DisseminationProtocolState()

          implicit val actorContext
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext(resolveAwaits = false)
          val availabilityStore =
            new FakeAvailabilityStore[ProgrammableUnitTestEnv](
              TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
            )
          val p2pNetworkOutBuffer = new mutable.ArrayBuffer[P2PNetworkOut.Message]()
          val availability =
            createAndStartAvailability[ProgrammableUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeIgnoringModule,
              p2pNetworkOut = fakeRecordingModule(p2pNetworkOutBuffer),
            )
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To6WithNonQuorumVotes._1 ->
              ABatchDisseminationProgressNode0To6WithNonQuorumVotes._2
                .copy(batchSentTo = Node4To6)
          )

          val cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider

          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNodes0To6,
                cryptoProvider,
              )
          )

          actorContext.runPipedMessagesUntilNoMorePiped(availability)
          p2pNetworkOutBuffer.toSeq should matchPattern {
            case Seq(
                  P2PNetworkOut.Multicast(
                    _,
                    nodes,
                  )
                ) if nodes == Node2And3 => // Node1 has already acked it
          }
        }
      }
    }

    "it receives Consensus.CreateProposal (from local consensus), " +
      "there are more batches ready for ordering than " +
      "requested by local consensus and " +
      "topology is unchanged" should {

        "send a fully-sized proposal to local consensus and " +
          "have batches left ready for ordering" in {
            val disseminationProtocolState = new DisseminationProtocolState()
            val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
            val pipeToSelfQueue =
              new mutable.Queue[() => Option[
                Availability.Message[FakePipeToSelfQueueUnitTestEnv]
              ]]()
            implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ] =
              FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

            val numberOfBatchesReadyForOrdering =
              BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt
            val batchesReadyForOrderingRange =
              0 to numberOfBatchesReadyForOrdering // both interval extremes are inclusive, i.e., 1 extra batch
            val batchIds = batchesReadyForOrderingRange
              .map(i => BatchId.createForTesting(s"batch $i"))
            val batchIdsWithDisseminationCompletions =
              batchesReadyForOrderingRange.map(n =>
                batchIds(n) -> DisseminationStatus
                  .InProgress(
                    Membership.forTesting(Node0, OrderingTopologyNode0),
                    Traced(batchIds(n)),
                    acks = ProofOfAvailabilityNode0AckNode0InTopology
                      .copy(batchId = batchIds(n))
                      .acks
                      .toSet,
                    epochNumber = anEpochNumber,
                    stats = OrderingRequestBatchStats.ForTesting,
                  )
                  .update()
                  .toEither
                  .getOrElse(fail("Batch should be complete with the given metadata"))
              )
            disseminationProtocolState.disseminationProgress.addAll(
              batchIdsWithDisseminationCompletions
            )
            val mempoolCell = new AtomicReference[Option[Mempool.Message]](None)

            val availability =
              createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
                disseminationProtocolState = disseminationProtocolState,
                consensus = fakeCellModule(consensusCell),
                mempool = fakeCellModule(mempoolCell),
              )
            mempoolCell.get() should contain(
              Mempool.CreateLocalBatches(
                (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier - numberOfBatchesReadyForOrdering - 1).toShort
              )
            )
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber.First,
                  EpochNumber.First,
                  OrderingTopologyNode0,
                  failingCryptoProvider,
                )
            )

            val batchIdsWithProofsOfAvailabilityReadyForOrdering =
              batchIdsWithDisseminationCompletions
                .slice(0, numberOfBatchesReadyForOrdering)
            val proposedProofsOfAvailability =
              batchIdsWithProofsOfAvailabilityReadyForOrdering.map(_._2)
            consensusCell.get() should matchPattern {
              case Some(
                    Consensus.LocalAvailability
                      .ProposalCreated(
                        BlockNumber.First,
                        OrderingBlock(poas),
                      )
                  )
                  if poas.toSet.sizeIs == BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt =>
            }
            pipeToSelfQueue shouldBe empty

            disseminationProtocolState.disseminationInProgressView should be(empty)
            disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None

            availability.receive(
              Availability.Consensus.Ordered(
                proposedProofsOfAvailability.map(_.tracedProofOfAvailability.value.batchId)
              )
            )
            disseminationProtocolState.disseminationCompleteView should
              contain only batchIdsWithDisseminationCompletions(
                numberOfBatchesReadyForOrdering
              )
            mempoolCell.get() should contain(
              Mempool.CreateLocalBatches(
                (BftBlockOrdererConfig.DefaultMaxBatchesPerProposal * AvailabilityModule.DisseminateAheadMultiplier - 1).toShort
              )
            )
          }

        "return the same response if no ack is given from consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[Availability.Message[FakePipeToSelfQueueUnitTestEnv]]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          val numberOfBatchesReadyForOrdering =
            BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt * 2
          val batchIds =
            (0 until numberOfBatchesReadyForOrdering)
              .map(i => BatchId.createForTesting(s"batch $i"))
          val batchIdsWithDisseminationCompletions =
            (0 until numberOfBatchesReadyForOrdering).map(n =>
              batchIds(n) -> DisseminationStatus
                .InProgress(
                  Membership.forTesting(Node0, OrderingTopologyNode0),
                  Traced(batchIds(n)),
                  acks = ProofOfAvailabilityNode0AckNode0InTopology
                    .copy(batchId = batchIds(n))
                    .acks
                    .toSet,
                  epochNumber = anEpochNumber,
                  stats = OrderingRequestBatchStats.ForTesting,
                )
                .update()
                .toEither
                .getOrElse(fail("Batch should be complete with the given metadata"))
            )
          disseminationProtocolState.disseminationProgress.addAll(
            batchIdsWithDisseminationCompletions
          )
          val availability = createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            consensus = fakeCellModule(consensusCell),
          )

          {
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber.First,
                  EpochNumber.First,
                  OrderingTopologyNode0,
                  failingCryptoProvider,
                )
            )
            consensusCell.get() should matchPattern {
              case Some(
                    Consensus.LocalAvailability
                      .ProposalCreated(
                        EpochNumber.First,
                        OrderingBlock(poas),
                      )
                  )
                  if poas.toSet.sizeIs == BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt =>
            }
            pipeToSelfQueue shouldBe empty

            val proposedProofsOfAvailability = getPoas(consensusCell)
            consensusCell.set(None)

            // if we ask for a proposal again without acking the previous response, we'll get the same thing again
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber(1),
                  EpochNumber.First,
                  OrderingTopologyNode0,
                  failingCryptoProvider,
                )
            )
            consensusCell.get() should contain(
              Consensus.LocalAvailability
                .ProposalCreated(
                  BlockNumber(1),
                  OrderingBlock(proposedProofsOfAvailability),
                )
            )
            pipeToSelfQueue shouldBe empty

            consensusCell.set(None)

            // now we ask for a new proposal, but ack the previous one
            availability.receive(
              Availability.Consensus.CreateProposal(
                BlockNumber(2),
                EpochNumber.First,
                OrderingTopologyNode0,
                failingCryptoProvider,
                orderedBatchIds = proposedProofsOfAvailability.map(_.batchId),
              )
            )

            pipeToSelfQueue shouldBe empty
          }

          {
            val proposedProofsOfAvailability = getPoas(consensusCell)

            consensusCell.get() should contain(
              Consensus.LocalAvailability
                .ProposalCreated(BlockNumber(2), OrderingBlock(proposedProofsOfAvailability))
            )

            availability.receive(
              Availability.Consensus.Ordered(proposedProofsOfAvailability.map(_.batchId))
            )
          }

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None
        }
      }

    "it receives Consensus.CreateProposal (from local consensus), " +
      "there are less batches ready for ordering than " +
      "requested by local consensus and " +
      "topology is unchanged" should {

        "send a non-empty but not fully-sized proposal to local consensus and " +
          "have no batches left ready for ordering" in {
            val disseminationProtocolState = new DisseminationProtocolState()
            val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
            val pipeToSelfQueue =
              new mutable.Queue[() => Option[
                Availability.Message[FakePipeToSelfQueueUnitTestEnv]
              ]]()
            implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ] =
              FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

            val numberOfBatchesReadyForOrdering =
              BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt - 2
            val batchIds =
              (0 until numberOfBatchesReadyForOrdering)
                .map(i => BatchId.createForTesting(s"batch $i"))
            val batchIdsWithDisseminationCompletions =
              (0 until numberOfBatchesReadyForOrdering).map(n =>
                batchIds(n) -> DisseminationStatus
                  .InProgress(
                    Membership.forTesting(Node0, OrderingTopologyNode0),
                    Traced(batchIds(n)),
                    acks = ProofOfAvailabilityNode0AckNode0InTopology
                      .copy(batchId = batchIds(n))
                      .acks
                      .toSet,
                    epochNumber = anEpochNumber,
                    stats = OrderingRequestBatchStats.ForTesting,
                  )
                  .update()
                  .toEither
                  .getOrElse(fail("Batch should be complete with the given metadata"))
              )
            disseminationProtocolState.disseminationProgress.addAll(
              batchIdsWithDisseminationCompletions
            )
            val availability = createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              consensus = fakeCellModule(consensusCell),
            )
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber.First,
                  EpochNumber.First,
                  OrderingTopologyNode0,
                  failingCryptoProvider,
                )
            )

            disseminationProtocolState.disseminationInProgressView should be(empty)
            disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None
            disseminationProtocolState.disseminationCompleteView should not be empty

            val proposedProofsOfAvailability =
              batchIdsWithDisseminationCompletions.map(_._2).map(_.tracedProofOfAvailability)
            consensusCell.get() should matchPattern {
              case Some(
                    Consensus.LocalAvailability
                      .ProposalCreated(BlockNumber.First, OrderingBlock(poas))
                  ) if poas.toSet.sizeIs == numberOfBatchesReadyForOrdering =>
            }
            pipeToSelfQueue shouldBe empty

            availability.receive(
              Availability.Consensus.Ordered(proposedProofsOfAvailability.map(_.value.batchId))
            )
            disseminationProtocolState.disseminationInProgressView should be(empty)
          }
      }

    "it receives Consensus.CreateProposal (from local consensus), " +
      "there are no batches ready for ordering but " +
      "the new topology has a smaller weak quorum and " +
      "an in-progress batch already has a new weak quorum" should {

        "move the in-progress batch to ready and " +
          "send it in a proposal to local consensus" in {
            val disseminationProtocolState = new DisseminationProtocolState()
            val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]]()
            val pipeToSelfQueue =
              new mutable.Queue[() => Option[
                Availability.Message[FakePipeToSelfQueueUnitTestEnv]
              ]]()
            implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ] =
              FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

            disseminationProtocolState.disseminationProgress.addOne(
              ABatchDisseminationProgressNode0To6WithNonQuorumVotes
            )
            val availability =
              createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
                disseminationProtocolState = disseminationProtocolState,
                consensus = fakeCellModule(consensusCell),
              )
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber.First,
                  EpochNumber.First,
                  OrderingTopologyNodes0To3,
                  failingCryptoProvider,
                )
            )

            disseminationProtocolState.disseminationInProgressView should be(empty)
            disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None
            disseminationProtocolState.disseminationCompleteView should not be empty

            val poa =
              ADisseminationProgressNode0To6WithNonQuorumVotes
                .copy(membership = Membership.forTesting(Node0, OrderingTopologyNodes0To3))
                .update()
                .toEither
                .getOrElse(fail("PoA should be ready in new topology but isn't"))
                .tracedProofOfAvailability
                .value

            consensusCell.get() should contain(
              Consensus.LocalAvailability
                .ProposalCreated(BlockNumber.First, OrderingBlock(Seq(poa)))
            )
            pipeToSelfQueue shouldBe empty

            availability.receive(Availability.Consensus.Ordered(Seq(poa.batchId)))
            disseminationProtocolState.disseminationCompleteView should be(empty)
          }
      }

    "it receives Consensus.CreateProposal (from local consensus) and " +
      "there is a batch ready for ordering but " +
      "the new topology has a bigger weak quorum and " +
      "the batch that was ready for ordering doesn't have a new weak quorum" should {

        "complete batch dissemination" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[Availability.Message[FakePipeToSelfQueueUnitTestEnv]]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          disseminationProtocolState.disseminationProgress.addOne(BatchReadyForOrderingNode0Vote)
          val availabilityStore =
            new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
              TrieMap[BatchId, OrderingRequestBatch](ABatchId -> ABatch)
            )
          val availability =
            createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeCellModule(consensusCell),
            )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNodes0To6,
                failingCryptoProvider,
              )
          )
          availability.getActiveMembership
          val reviewedProgress =
            BatchReadyForOrderingNode0Vote._2
              .changeMembership(
                availability.getActiveMembership
                  .copy(orderingTopology = OrderingTopologyNodes0To6)
              )
              .toEither
              .leftOrFail("Batch should not be complete in new topology")
          disseminationProtocolState.disseminationInProgressView should contain only (ABatchId -> reviewedProgress
            // Regressions are reset by metrics emission
            .copy(regressionsToSigning = 0, disseminationRegressions = 0))
          disseminationProtocolState.nextToBeProvidedToConsensus shouldBe ANextToBeProvidedToConsensus
          disseminationProtocolState.disseminationCompleteView should be(empty)

          consensusCell.get() should contain(Consensus.LocalAvailability.NoProposalAvailableYet)

          val selfSendMessages = pipeToSelfQueue.flatMap(_.apply())
          selfSendMessages should contain only
            Availability.LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(Traced(ABatchId), ABatch, signature = None))
            )
        }
      }

    "it receives Consensus.CreateProposal (from local consensus), " +
      "there is a pending proposal request from consensus," +
      "there is a batch ready for ordering," +
      "there is a batch in progress but " +
      "the new topology has a smaller weak quorum; after that " +
      "the in-progress batch has a new weak quorum and " +
      "the batch that was ready for ordering doesn't have a new weak quorum" should {

        "move the previously in-progress batch to ready, " +
          "move the previously ready batch to in-progress, " +
          "propose only the ready batch to local consensus and " +
          "complete dissemination of the in-progress batch" in {
            val disseminationProtocolState = new DisseminationProtocolState()
            val consensusBuffer =
              new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
            val pipeToSelfQueue =
              new mutable.Queue[() => Option[
                Availability.Message[FakePipeToSelfQueueUnitTestEnv]
              ]]()
            implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ] =
              FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

            // This in-progress batch will become ready in the new topology
            disseminationProtocolState.disseminationProgress.addOne(
              ABatchDisseminationProgressNode0To6WithNode0And1Votes
            )
            // This ready batch will become stale in the new topology
            disseminationProtocolState.disseminationProgress.addOne(
              AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes
            )
            // We need local consensus pulls for both the in-progress and ready batches, to ensure that
            //  the ready batch that becomes stale is not included in a proposal to consensus even
            //  if there is one pending.
            disseminationProtocolState.nextToBeProvidedToConsensus =
              NextToBeProvidedToConsensus(BlockNumber.First, Some(1))
            val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
              TrieMap[BatchId, OrderingRequestBatch](AnotherBatchId -> ABatch)
            )
            val availability = createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeRecordingModule(consensusBuffer),
            )
            availability.receive(
              Availability.Consensus
                .CreateProposal(
                  BlockNumber(1),
                  EpochNumber.First,
                  OrderingTopologyNodes0To3,
                  failingCryptoProvider,
                )
            )

            val reviewedProgress =
              AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._2
                .changeMembership(
                  availability.getActiveMembership
                    .copy(orderingTopology = OrderingTopologyNodes0To3)
                )
                .toEither
                .leftOrFail("Batch should not be complete in new topology")
            disseminationProtocolState.disseminationProgress should contain(
              AnotherBatchId ->
                // Regressions are reset by metrics emission
                reviewedProgress.copy(regressionsToSigning = 0, disseminationRegressions = 0)
            )
            disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None
            disseminationProtocolState
              .disseminationStatusView(_.asComplete)
              .map(_._1) should contain(
              ABatchId
            )

            val proposedProofsOfAvailability = ADisseminationProgressNode0To6WithNonQuorumVotes
              .copy(membership =
                availability.getActiveMembership.copy(orderingTopology = OrderingTopologyNodes0To3)
              )
              .update()
              .toEither
              .getOrElse(
                fail("PoA should be ready in new topology but isn't")
              )
              .tracedProofOfAvailability
              .value

            val poa = proposedProofsOfAvailability
            consensusBuffer should contain only
              Consensus.LocalAvailability.ProposalCreated(
                BlockNumber(1),
                OrderingBlock(Seq(poa)),
              )

            val selfMessages = pipeToSelfQueue.flatMap(_.apply())
            selfMessages should contain only Availability.LocalDissemination
              .LocalBatchesStoredSigned(
                Seq(LocalBatchStoredSigned(Traced(AnotherBatchId), ABatch, signature = None))
              )
          }
      }
  }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there is a batch ready for ordering, containing ack from node that will be removed" +
    "the new topology has a smaller weak quorum; after that " +
    "the batch that was ready for ordering have a new weak quorum" should {

      "keep the previously ready batch but without the removed node" +
        "propose the ready batch to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusBuffer =
            new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          val acksBefore = ProofOfAvailability6NodesQuorumVotesNodes0And4To6InTopology.acks

          val availability =
            createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              consensus = fakeRecordingModule(consensusBuffer),
            )
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchId -> DisseminationStatus
              .InProgress(
                availability.getActiveMembership.copy(orderingTopology = OrderingTopologyNodes0To6),
                Traced(ABatchId),
                acks = acksBefore.toSet,
                epochNumber = anEpochNumber,
                stats = ABatch.stats,
              )
              .update()
              .toEither
              .getOrElse(fail("Batch should be complete with the given metadata"))
          )
          val newTopology = OrderingTopology.forTesting(Node0To6.filterNot(_ == "node6"))
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                newTopology,
                failingCryptoProvider,
              )
          )
          val acksAfter = acksBefore.filterNot(_.from == "node6")

          val poa = ProofOfAvailability(ABatchId, acks = acksAfter, anEpochNumber)

          disseminationProtocolState.disseminationInProgressView shouldBe empty
          disseminationProtocolState.nextToBeProvidedToConsensus.maxBatchesPerProposal shouldBe None
          disseminationProtocolState.disseminationCompleteView
            .map(_._1) should contain only ABatchId

          pipeToSelfQueue shouldBe empty // We should not try to sign/store anything in this case

          consensusBuffer shouldBe Seq(
            Consensus.LocalAvailability.ProposalCreated(
              BlockNumber.First,
              OrderingBlock(Seq(poa)),
            )
          )
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there is a batch ready for ordering but " +
    "the new topology has the same size but different keys and " +
    "invalidates one of its acks from other nodes" should {

      "move the previously ready batch to in-progress and " +
        "complete dissemination of the in-progress batch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val consensusBuffer =
            new ArrayBuffer[Consensus.Message[FakePipeToSelfQueueUnitTestEnv]]()
          val pipeToSelfQueue =
            new mutable.Queue[() => Option[
              Availability.Message[FakePipeToSelfQueueUnitTestEnv]
            ]]()
          implicit val selfPipeRecordingContext: FakePipeToSelfQueueUnitTestContext[
            Availability.Message[FakePipeToSelfQueueUnitTestEnv]
          ] =
            FakePipeToSelfQueueUnitTestContext(pipeToSelfQueue)

          // This ready batch will become stale in the new topology
          disseminationProtocolState.disseminationProgress.addOne(
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes
          )

          val availabilityStore = new FakeAvailabilityStore[FakePipeToSelfQueueUnitTestEnv](
            TrieMap[BatchId, OrderingRequestBatch](
              AnotherBatchId -> ABatch
            )
          )
          val availability =
            createAndStartAvailability[FakePipeToSelfQueueUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeRecordingModule(consensusBuffer),
            )
          val newTopology =
            OrderingTopologyNodes0To6.copy(
              nodesTopologyInfo =
                OrderingTopologyNodes0To6.nodesTopologyInfo.map { case (nodeId, nodeInfo) =>
                  // Change the key of node5 and node6 so that the PoA is only left with 2 valid acks < f+1 = 3
                  nodeId -> (if (nodeId == "node5" || nodeId == "node6")
                               nodeInfo.copy(keyIds =
                                 Set(
                                   BftKeyId(
                                     anotherNoSignature.authorizingLongTermKey.toProtoPrimitive
                                   )
                                 )
                               )
                             else nodeInfo)
                }
            )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                newTopology,
                failingCryptoProvider,
              )
          )

          val reviewedProgress =
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._2
              .changeMembership(
                availability.getActiveMembership.copy(orderingTopology = newTopology)
              )
              .toEither
              .leftOrFail("Progress was not regressed")

          disseminationProtocolState.disseminationInProgressView should contain only (AnotherBatchId ->
            // Regressions are reset by metrics emission
            reviewedProgress.copy(regressionsToSigning = 0, disseminationRegressions = 0))
          disseminationProtocolState.nextToBeProvidedToConsensus shouldBe
            NextToBeProvidedToConsensus(BlockNumber.First, Some(16))
          disseminationProtocolState.disseminationCompleteView shouldBe empty
          consensusBuffer should contain(Consensus.LocalAvailability.NoProposalAvailableYet)

          val selfMessages = pipeToSelfQueue.flatMap(_.apply())
          selfMessages should contain only Availability.LocalDissemination
            .LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(Traced(AnotherBatchId), ABatch, signature = None))
            )
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there is a batch ready for ordering but " +
    "the new topology has the same size but different keys and " +
    "invalidates the ack from the disseminating node" should {

      "move the previously ready batch to in-progress, " +
        "sign the batch again and " +
        "re-disseminate the in-progress batch to all recipients" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          val spiedNoSignatureCryptoProvider =
            spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
          val consensusBuffer =
            new ArrayBuffer[Consensus.Message[ProgrammableUnitTestEnv]]()
          val p2pNetworkOutBuffer = new ArrayBuffer[P2PNetworkOut.Message]()
          implicit val ctx
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]]()

          // This ready batch will become stale in the new topology
          disseminationProtocolState.disseminationProgress.addOne(
            AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._1 ->
              AnotherBatchReadyForOrdering6NodesQuorumNodes0And4To6Votes._2.copy(batchSentTo =
                Set(Node1)
              )
          )

          val availabilityStore = new FakeAvailabilityStore[ProgrammableUnitTestEnv](
            TrieMap[BatchId, OrderingRequestBatch](
              AnotherBatchId -> ABatch
            )
          )
          val availability =
            createAndStartAvailability[ProgrammableUnitTestEnv](
              disseminationProtocolState = disseminationProtocolState,
              availabilityStore = availabilityStore,
              consensus = fakeRecordingModule(consensusBuffer),
              p2pNetworkOut = fakeRecordingModule(p2pNetworkOutBuffer),
              cryptoProvider = spiedNoSignatureCryptoProvider,
            )
          val newTopology =
            OrderingTopologyNodes0To6.copy(
              nodesTopologyInfo =
                OrderingTopologyNodes0To6.nodesTopologyInfo.map { case (nodeId, nodeInfo) =>
                  // Change the key of node0 and node6 so that the PoA is only left with 2 valid acks < f+1 = 3
                  //  and it will be re-signed by node0
                  nodeId -> (if (nodeId == "node0" || nodeId == "node6")
                               nodeInfo.copy(keyIds =
                                 Set(
                                   BftKeyId(
                                     anotherNoSignature.authorizingLongTermKey.toProtoPrimitive
                                   )
                                 )
                               )
                             else nodeInfo)
                }
            )
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                newTopology,
                spiedNoSignatureCryptoProvider,
              )
          )

          disseminationProtocolState.disseminationInProgressView should not be empty
          disseminationProtocolState.nextToBeProvidedToConsensus shouldBe
            NextToBeProvidedToConsensus(BlockNumber.First, Some(16))
          disseminationProtocolState.disseminationCompleteView shouldBe empty
          consensusBuffer should contain(Consensus.LocalAvailability.NoProposalAvailableYet)

          val pipedMessages = ctx.runPipedMessages()
          pipedMessages should contain only Availability.LocalDissemination
            .LocalBatchesStored(
              Seq(Traced(AnotherBatchId) -> ABatch)
            )

          pipedMessages.foreach(availability.receive)

          ctx.runPipedMessagesUntilNoMorePiped(availability)

          val disseminationMessage = Availability.RemoteDissemination.RemoteBatch
            .create(AnotherBatchId, ABatch, from = Node0)

          // We expect only 1 signing call and 1 multicast to the nodes that need to receive the batch,
          //  i.e., the ones in the active membership that hadn't acked it and weren't yet sent the batch.

          verify(spiedNoSignatureCryptoProvider, times(1)).signMessage(
            eqTo(disseminationMessage),
            eqTo(BftSignedAvailabilityMessage),
          )(anyTraceContext, any[MetricsContext])
          val remoteMessage =
            P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
              SignedMessage(disseminationMessage, noSignature)
            )
          p2pNetworkOutBuffer should contain only
            P2PNetworkOut
              .Multicast(
                remoteMessage,
                Set(Node2, Node3, node(6)),
              )
        }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are expired batches ready for ordering" should {

      "only propose non-expired batches and remove the expired batches from the dissemination status " in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val initialEpochNumber = EpochNumber(OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
        val availability =
          createAndStartAvailability[IgnoringUnitTestEnv](
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
            initialEpochNumber = EpochNumber(initialEpochNumber - 1L),
          )

        val (validBatchIds, expiredBatchIds) = {
          val numberOfBatchesReadyForOrdering =
            BftBlockOrdererConfig.DefaultMaxBatchesPerProposal.toInt
          val batchIds =
            (0 until numberOfBatchesReadyForOrdering).map(i =>
              BatchId.createForTesting(s"batch $i")
            )
          (
            batchIds.take(numberOfBatchesReadyForOrdering / 2),
            batchIds.drop(numberOfBatchesReadyForOrdering / 2),
          )
        }

        {
          def batchIdWithMetadata(batchId: BatchId, epochNumber: EpochNumber) =
            batchId -> DisseminationStatus
              .InProgress(
                Membership.forTesting(Node0, OrderingTopologyNode0),
                Traced(batchId),
                acks =
                  ProofOfAvailabilityNode0AckNode0InTopology.copy(batchId = batchId).acks.toSet,
                epochNumber = epochNumber,
                stats = OrderingRequestBatchStats.ForTesting,
              )
              .update()
              .toEither
              .getOrElse(fail("Batch should be complete with the given metadata"))

          val validBatchIdsWithMetadata =
            validBatchIds.map(batchId => batchIdWithMetadata(batchId, initialEpochNumber))
          val expiredBatchIdsWithMetadata =
            expiredBatchIds.map(batchId => batchIdWithMetadata(batchId, EpochNumber.First))
          disseminationProtocolState.disseminationProgress.addAll(
            validBatchIdsWithMetadata ++ expiredBatchIdsWithMetadata
          )
        }

        loggerFactory.assertLogs(
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                initialEpochNumber,
                OrderingTopologyNode0,
                failingCryptoProvider,
              )
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include("Discarding the expired batches")
          },
        )

        inside(consensusCell.get()) {
          case Some(
                Consensus.LocalAvailability
                  .ProposalCreated(
                    BlockNumber.First,
                    OrderingBlock(poas),
                  )
              ) =>
            poas.map(_.batchId) should contain theSameElementsAs validBatchIds
        }

        disseminationProtocolState.disseminationCompleteView
          .map(_._1) should contain theSameElementsAs validBatchIds
      }
    }

  "it receives Consensus.CreateProposal (from local consensus), " +
    "there are expired batches being disseminated" should {

      "remove the expired batches from dissemination" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val initialEpochNumber = EpochNumber(OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
        val availability =
          createAndStartAvailability[IgnoringUnitTestEnv](
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
            initialEpochNumber = EpochNumber(initialEpochNumber - 1L),
          )

        val (validBatchIds, expiredBatchIds) = {
          val numberOfBatchesInDissemination = 10
          val batchIds =
            (0 until numberOfBatchesInDissemination).map(i => BatchId.createForTesting(s"batch $i"))
          (
            batchIds.take(numberOfBatchesInDissemination / 2),
            batchIds.drop(numberOfBatchesInDissemination / 2),
          )
        }

        {
          def batchIdWithMetadata(batchId: BatchId, epochNumber: EpochNumber) =
            batchId -> DisseminationStatus.InProgress(
              Membership.forTesting(Node0, OrderingTopologyNodes0To3),
              Traced(batchId),
              acks = Set(AvailabilityAck(Node0, Signature.noSignature)),
              epochNumber = epochNumber,
              stats = OrderingRequestBatchStats.ForTesting,
            )

          val validBatchIdsWithMetadata =
            validBatchIds.map(batchId => batchIdWithMetadata(batchId, initialEpochNumber))
          val expiredBatchIdsWithMetadata =
            expiredBatchIds.map(batchId => batchIdWithMetadata(batchId, EpochNumber.First))
          disseminationProtocolState.disseminationProgress.addAll(
            validBatchIdsWithMetadata ++ expiredBatchIdsWithMetadata
          )
        }

        loggerFactory.assertLogs(
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                initialEpochNumber,
                OrderingTopologyNodes0To3,
                failingCryptoProvider,
              )
          ),
          log => {
            log.level shouldBe Level.WARN
            log.message should include("Discarding the expired batches")
          },
        )

        disseminationProtocolState.disseminationInProgressView.map(
          _._1
        ) should contain theSameElementsAs validBatchIds
      }
    }

  "it receives multiple Consensus.CreateProposal (from local consensus) and " +
    "no batches are ready for ordering" should {

      "discard all requests but the one for the highest block" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusBuffer = new ArrayBuffer[Consensus.Message[IgnoringUnitTestEnv]]()

        val availability =
          createAndStartAvailability[IgnoringUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            consensus = fakeRecordingModule(consensusBuffer),
          )

        Seq(1, 2, 3).foreach { blockNum =>
          availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber(blockNum.toLong),
                EpochNumber.First,
                OrderingTopologyNode0,
                failingCryptoProvider,
              )
          )
        }

        disseminationProtocolState.nextToBeProvidedToConsensus shouldBe
          NextToBeProvidedToConsensus(
            BlockNumber(3),
            Some(BftBlockOrdererConfig.DefaultMaxBatchesPerProposal),
          )
      }

      "abort if a request is made for a previous block" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val consensusBuffer = new ArrayBuffer[Consensus.Message[IgnoringUnitTestEnv]]()

        val availability =
          createAndStartAvailability[IgnoringUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            consensus = fakeRecordingModule(consensusBuffer),
          )

        // A proposal request for expected block 0 is OK
        availability.receive(
          Availability.Consensus
            .CreateProposal(
              BlockNumber.First,
              EpochNumber.First,
              OrderingTopologyNode0,
              failingCryptoProvider,
            )
        )

        // A proposal request for already requested block 0 fails
        suppressProblemLogs(
          a[TestFailedException] should be thrownBy (availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNode0,
                failingCryptoProvider,
              )
          )),
          count = 2,
        )

        // A proposal request for blocks greater than minimum expected 1 is OK
        availability.receive(
          Availability.Consensus
            .CreateProposal(
              BlockNumber(2),
              EpochNumber.First,
              OrderingTopologyNode0,
              failingCryptoProvider,
            )
        )

        // A proposal request for a previous block fails
        suppressProblemLogs(
          a[TestFailedException] should be thrownBy (availability.receive(
            Availability.Consensus
              .CreateProposal(
                BlockNumber.First,
                EpochNumber.First,
                OrderingTopologyNode0,
                failingCryptoProvider,
              )
          )),
          count = 2,
        )
      }
    }
}
