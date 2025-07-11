// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModuleTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  IgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.LocalDissemination.LocalBatchStoredSigned
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  RemoteDissemination,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeCellModule,
  fakeIgnoringModule,
}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference

class AvailabilityModuleDisseminationTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" when {

    "it receives Dissemination.LocalBatchCreated (from local mempool)" should {

      "should store in the local store" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val availability = createAvailability[IgnoringUnitTestEnv](
          availabilityStore = availabilityStore,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(
          LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest))
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
      }

      "should create the batch using the known epoch number" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])

        val epochNumber = EpochNumber(50)

        val availability = createAvailability[IgnoringUnitTestEnv](
          availabilityStore = availabilityStore,
          disseminationProtocolState = disseminationProtocolState,
          initialEpochNumber = epochNumber,
        )
        availability.receive(
          LocalDissemination.LocalBatchCreated(Seq(anOrderingRequest))
        )

        val batch = OrderingRequestBatch.create(
          Seq(anOrderingRequest),
          epochNumber,
        )

        verify(availabilityStore, times(1)).addBatch(BatchId.from(batch), batch)
      }
    }
  }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "there are no other nodes (so, F == 0)" should {

      "clear dissemination progress and " +
        "mark the batch ready for ordering" in {
          implicit val ctx
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext()
          val disseminationProtocolState = new DisseminationProtocolState()

          val cryptoProvider = newMockCrypto

          val me = Node0
          val availability = createAvailability[ProgrammableUnitTestEnv](
            disseminationProtocolState = disseminationProtocolState,
            myId = me,
            cryptoProvider = cryptoProvider,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
          ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me, metrics),
            operationId = "availability-sign-local-batchId",
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          locally {
            import BatchReadyForOrderingNode0Vote._2.*
            disseminationProtocolState.batchesReadyForOrdering.values.toSeq should
              matchPattern {
                case Seq(
                      DisseminatedBatchMetadata(
                        `proofOfAvailability`,
                        `epochNumber`,
                        `stats`,
                        _,
                        _,
                        _,
                        _,
                      )
                    ) =>
              }
          }
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are no consensus requests and " +
    "F > 0" should {

      "just update dissemination progress" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()

        val me = Node0
        val cryptoProvider = newMockCrypto

        val availability = createAvailability[ProgrammableUnitTestEnv](
          otherNodes = Node1To3,
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me, metrics),
          operationId = "availability-sign-local-batchId",
        )

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To3WithNode0Vote
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are no other nodes (so, F == 0) " should {

      "just send proposal to local consensus" in {
        implicit val ctx
            : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext()
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
        val cryptoProvider = newMockCrypto
        val me = Node0
        val availability = createAvailability[ProgrammableUnitTestEnv](
          consensus = fakeCellModule(consensusCell),
          myId = me,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))
        ctx.runPipedMessagesAndReceiveOnModule(availability) // Perform signing

        verify(cryptoProvider).signHash(
          AvailabilityAck.hashFor(ABatchId, anEpochNumber, me, metrics),
          operationId = "availability-sign-local-batchId",
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should not be empty

        consensusCell.get() should contain(ABatchProposalNode0VoteNode0InTopology)
        availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
      }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests, " +
    "there are other nodes and " +
    "F == 0" should {

      "send proposal to local consensus and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1And2,
            myId = me,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me, metrics),
            operationId = "availability-sign-local-batchId",
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Some(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          disseminationProtocolState.batchesReadyForOrdering should not be empty
          consensusCell.get() should contain(ABatchProposalNode0VoteNodes0To2InTopology)
          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)

          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch
            .create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Set(Node1, Node2),
            )
          )
        }
    }

  "it receives Dissemination.LocalBatchStored (from local store), " +
    "there are consensus requests and " +
    "F > 0 (so, there must also be other nodes)" should {

      "update dissemination progress and " +
        "broadcast Dissemination.RemoteBatch" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)
          val me = Node0
          val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)
          implicit val context
              : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext
          val availability = createAvailability[ProgrammableUnitTestEnv](
            otherNodes = Node1To3,
            myId = me,
            cryptoProvider = cryptoProvider,
            p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          availability.receive(LocalDissemination.LocalBatchesStored(Seq(ABatchId -> ABatch)))

          verify(cryptoProvider).signHash(
            AvailabilityAck.hashFor(ABatchId, anEpochNumber, me, metrics),
            operationId = "availability-sign-local-batchId",
          )

          availability.receive(
            LocalDissemination.LocalBatchesStoredSigned(
              Seq(LocalBatchStoredSigned(ABatchId, ABatch, Some(Signature.noSignature)))
            )
          )

          disseminationProtocolState.disseminationProgress should
            contain only ABatchDisseminationProgressNode0To3WithNode0Vote
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
          p2pNetworkOutCell.get() shouldBe None
          val remoteBatch = RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, Node0)
          verify(cryptoProvider).signMessage(
            remoteBatch,
            AuthenticatedMessageType.BftSignedAvailabilityMessage,
          )

          context.runPipedMessagesAndReceiveOnModule(availability)

          p2pNetworkOutCell.get() should contain(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
                remoteBatch.fakeSign
              ),
              Node1To3,
            )
          )
        }
    }

  "it receives Dissemination.RemoteBatch (from node)" should {

    "store in the local store" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      availability.receive(
        RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(availabilityStore, times(1)).addBatch(ABatchId, ABatch)
    }

    "not store if it is the wrong batchId" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(WrongBatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("BatchId doesn't match digest")
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if too many requests in a batch" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        maxRequestsInBatch = 0,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include regex
            """Batch BatchId\(SHA-256:[^)]+\) from 'node1' contains more requests \(1\) than allowed \(0\), skipping"""
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if batch contains requests with invalid tags" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch
            .create(ABatchIdWithInvalidTags, ABatchWithInvalidTags, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          val validTags = OrderingRequest.ValidTags.mkString(", ")
          log.message should include regex
            """Batch BatchId\(SHA-256:[^)]+\) from 'node1' contains requests with invalid tags, """ +
            s"valid tags are: \\($validTags\\); skipping"
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if any single request is too large" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        maxRequestPayloadBytes = 0,
      )
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ANonEmptyBatchId, ANonEmptyBatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include regex (
            """Batch BatchId\(SHA-256:[^)]+\) from 'node1' contains one or more batches that exceed the maximum allowed request size bytes \(0\), skipping"""
          )
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if batch is expired or too far in the future" in {
      val disseminationProtocolState = new DisseminationProtocolState()

      val availabilityStore = spy(new FakeAvailabilityStore[IgnoringUnitTestEnv])
      val initialEpochNumber = EpochNumber(OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
      val availability = createAvailability[IgnoringUnitTestEnv](
        availabilityStore = availabilityStore,
        disseminationProtocolState = disseminationProtocolState,
        initialEpochNumber = initialEpochNumber,
      )

      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include regex
            """Batch BatchId\(SHA-256:[^)]+\) from 'node1' contains an expired batch at epoch number 0 which is 500 epochs or more older than last known epoch 501, skipping"""
        },
      )

      val tooFarInTheFutureBatch = OrderingRequestBatch.create(
        Seq(anOrderingRequest),
        EpochNumber(initialEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs * 2),
      )

      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch
            .create(BatchId.from(tooFarInTheFutureBatch), tooFarInTheFutureBatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include regex
            """Batch BatchId\(SHA-256:[^)]+\) from 'node1' contains a batch whose epoch number 1501 is too far in the future compared to last known epoch 501, skipping"""
        },
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verifyZeroInteractions(availabilityStore)
    }

    "not store if there is no dissemination quota available for node" in {
      implicit val ctx: ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()

      val disseminationProtocolState = new DisseminationProtocolState()
      val disseminationQuotas = disseminationProtocolState.disseminationQuotas
      val disseminationQuotaSize = 1

      val secondBatch = OrderingRequestBatch.create(
        Seq(anOrderingRequest, anOrderingRequest),
        anEpochNumber,
      )
      val secondBatchId = BatchId.from(secondBatch)

      val availability = createAvailability[ProgrammableUnitTestEnv](
        disseminationProtocolState = disseminationProtocolState,
        maxNonOrderedBatchesPerNode = disseminationQuotaSize.toShort,
        cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        consensus = fakeIgnoringModule,
      )

      def canAcceptBatch(batchId: BatchId) =
        disseminationQuotas.canAcceptForNode(Node1, batchId, disseminationQuotaSize)

      // initially we can take a batch
      canAcceptBatch(ABatchId) shouldBe true
      availability.receive(
        RemoteDissemination.RemoteBatch.create(ABatchId, ABatch, from = Node1)
      )
      canAcceptBatch(secondBatchId) shouldBe true
      ctx.runPipedMessagesThenVerifyAndReceiveOnModule(availability) { message =>
        message shouldBe Availability.LocalDissemination.RemoteBatchStored(
          ABatchId,
          anEpochNumber,
          Node1,
        )
      }

      // then after processing and storing the remote batch, we count it towards the quota
      // so we can no longer take a batch. Note that we use a different batch id to check now,
      // because the initial batch id will be accepted since we always accept a batch that has been accepted before
      canAcceptBatch(secondBatchId) shouldBe false
      // receiving a new batch after the quota is full gives a warning and the batch is rejected
      loggerFactory.assertLogs(
        availability.receive(
          RemoteDissemination.RemoteBatch.create(secondBatchId, secondBatch, from = Node1)
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message shouldBe (
            s"Batch $secondBatchId from 'node1' cannot be taken because we have reached the limit of 1 unordered and unexpired batches from this node that we can hold on to, skipping"
          )
        },
      )

      // request from output module to fetch block data with this batch id will free one spot in the quota for this node
      val block = OutputModuleTest.anOrderedBlockForOutput(batchIds = Seq(ABatchId))
      availability.receive(
        Availability.LocalOutputFetch.FetchBlockData(block)
      )
      canAcceptBatch(secondBatchId) shouldBe true

      // so now we can take another batch, which will then fill up the quota again
      availability.receive(
        Availability.LocalDissemination.RemoteBatchStored(
          secondBatchId,
          anEpochNumber,
          Node1,
        )
      )
      canAcceptBatch(AnotherBatchId) shouldBe false

      // we can also free up a spot when a batch in the quota expires
      val expiringEpochNumber =
        EpochNumber(anEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs)
      availability.receive(
        Availability.Consensus
          .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, expiringEpochNumber)
      )
      canAcceptBatch(AnotherBatchId) shouldBe true
    }
    "evict batches that are being tracked of after some epochs" in {
      implicit val ctx: ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext()
      val availabilityStore = spy(new FakeAvailabilityStore[ProgrammableUnitTestEnv])
      val disseminationProtocolState = new DisseminationProtocolState()

      val availability = createAvailability[ProgrammableUnitTestEnv](
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
        availabilityStore = availabilityStore,
        consensus = fakeIgnoringModule,
        initialEpochNumber = Genesis.GenesisEpochNumber,
      )

      availability.receive(
        Availability.LocalDissemination.RemoteBatchStored(
          ABatchId,
          anEpochNumber,
          Node1,
        )
      )

      // just expiring an epoch is not enough to evict its batches
      val expiringEpochNumber =
        EpochNumber(anEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs)
      availability.receive(
        Availability.Consensus
          .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, expiringEpochNumber)
      )
      verifyZeroInteractions(availabilityStore)

      // an extra window of batch validity has to go by for an epoch have its batches evicted
      val evictionEpochNumber =
        EpochNumber(anEpochNumber + 2 * OrderingRequestBatch.BatchValidityDurationEpochs)
      availability.receive(
        Availability.Consensus
          .CreateProposal(OrderingTopologyNode0, failingCryptoProvider, evictionEpochNumber)
      )

      // the batch got evicted
      verify(availabilityStore).gc(Seq(ABatchId))
    }
  }

  "it receives Dissemination.RemoteBatchStored (from local store)" should {

    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]

      val myId = Node0
      val availability = createAvailability[IgnoringUnitTestEnv](
        myId = myId,
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = cryptoProvider,
      )
      availability.receive(
        LocalDissemination.RemoteBatchStored(ABatchId, anEpochNumber, from = Node1)
      )

      disseminationProtocolState.disseminationProgress should be(empty)
      disseminationProtocolState.batchesReadyForOrdering should be(empty)
      disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      verify(cryptoProvider).signHash(
        AvailabilityAck.hashFor(ABatchId, anEpochNumber, myId, metrics),
        operationId = "availability-sign-remote-batchId",
      )
    }
  }

  "it receives Dissemination.RemoteBatchStoredSigned" should {

    "just acknowledge the originating node" in {
      val disseminationProtocolState = new DisseminationProtocolState()
      val p2pNetworkOutCell = new AtomicReference[Option[P2PNetworkOut.Message]](None)

      implicit val context
          : ProgrammableUnitTestContext[Availability.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val availability = createAvailability[ProgrammableUnitTestEnv](
        p2pNetworkOut = fakeCellModule(p2pNetworkOutCell),
        disseminationProtocolState = disseminationProtocolState,
        cryptoProvider = ProgrammableUnitTestEnv.noSignatureCryptoProvider,
      )
      val signature = Signature.noSignature
      availability.receive(
        LocalDissemination.RemoteBatchStoredSigned(ABatchId, from = Node1, signature)
      )

      p2pNetworkOutCell.get() shouldBe None

      context.runPipedMessagesAndReceiveOnModule(availability)

      p2pNetworkOutCell.get() should contain(
        P2PNetworkOut.Multicast(
          P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(
            RemoteDissemination.RemoteBatchAcknowledged
              .create(ABatchId, Node0, signature)
              .fakeSign
          ),
          Set(Node1),
        )
      )
    }
  }

  "it receives one Dissemination.RemoteBatchAcknowledged (from node) but " +
    "the batch is not being disseminated [anymore]" should {

      "do nothing" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        disseminationProtocolState.disseminationProgress should be(empty)
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        verifyZeroInteractions(cryptoProvider)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives one Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {

      "ignore the ACK" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0And1WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Set(Node1),
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        val msg = remoteBatchAcknowledged(idx = 1)
        availability.receive(msg)

        verify(cryptoProvider).verifySignature(
          AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from, metrics),
          msg.from,
          msg.signature,
          operationId = "availability-signature-verify-ack",
        )

        availability.receive(
          LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
        )

        disseminationProtocolState.disseminationProgress should be(empty)
        locally {
          import BatchReadyForOrderingNode0And1Votes._2.*
          disseminationProtocolState.batchesReadyForOrdering.values.toSeq should matchPattern {
            case Seq(
                  DisseminatedBatchMetadata(
                    `proofOfAvailability`,
                    `epochNumber`,
                    `stats`,
                    _,
                    _,
                    _,
                    _,
                  )
                ) =>
          }
        }
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from node " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated and " +
    "there are no consensus requests" should {

      "reset dissemination progress and " +
        "mark the batch ready for ordering" in {
          val disseminationProtocolState = new DisseminationProtocolState()

          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from, metrics),
              quorumAck.from,
              quorumAck.signature,
              operationId = "availability-signature-verify-ack",
            )
          }

          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          disseminationProtocolState.disseminationProgress should be(empty)
          locally {
            import BatchReadyForOrdering4NodesQuorumVotes._2.*
            disseminationProtocolState.batchesReadyForOrdering.values.toSeq should matchPattern {
              case Seq(
                    DisseminatedBatchMetadata(
                      `proofOfAvailability`,
                      `epochNumber`,
                      `stats`,
                      _,
                      _,
                      _,
                      _,
                    )
                  ) =>
            }
          }
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are no consensus requests" should {

      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()

        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from, metrics),
            quorumAck.from,
            quorumAck.signature,
            operationId = "availability-signature-verify-ack",
          )
        }

        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should be(empty)
      }
    }

  "F == 0 (i.e., proof of availability is complete), " +
    "it receives Dissemination.RemoteBatchAcknowledged from node, " +
    "the batch is being disseminated and " +
    "there are consensus requests" should {

      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0And1WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Set(Node1),
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          val msg = remoteBatchAcknowledged(idx = 1)
          availability.receive(msg)
          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(msg.batchId, anEpochNumber, msg.from, metrics),
            msg.from,
            msg.signature,
            operationId = "availability-signature-verify-ack",
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(msg.batchId, msg.from, msg.signature)
          )

          consensusCell.get() should contain(ABatchProposalNode0And1Votes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `>= quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is complete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {

      "reset dissemination progress and " +
        "send proposal to local consensus" in {
          val disseminationProtocolState = new DisseminationProtocolState()
          disseminationProtocolState.disseminationProgress.addOne(
            ABatchDisseminationProgressNode0To3WithNode0Vote
          )
          disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
          val consensusCell = new AtomicReference[Option[Consensus.ProtocolMessage]](None)
          val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
          val availability = createAvailability[IgnoringUnitTestEnv](
            otherNodes = Node1To3,
            cryptoProvider = cryptoProvider,
            consensus = fakeCellModule(consensusCell),
            disseminationProtocolState = disseminationProtocolState,
          )
          QuorumAcksForNode0To3.tail.foreach { quorumAck =>
            availability.receive(quorumAck)
            verify(cryptoProvider).verifySignature(
              AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from, metrics),
              quorumAck.from,
              quorumAck.signature,
              operationId = "availability-signature-verify-ack",
            )
            availability.receive(
              LocalDissemination.RemoteBatchAcknowledgeVerified(
                quorumAck.batchId,
                quorumAck.from,
                quorumAck.signature,
              )
            )
          }

          consensusCell.get() should contain(ABatchProposal4NodesQuorumVotes)
          disseminationProtocolState.batchesReadyForOrdering should not be empty
          disseminationProtocolState.disseminationProgress should be(empty)
          disseminationProtocolState.toBeProvidedToConsensus should be(empty)

          availability.receive(Availability.Consensus.Ordered(Seq(ABatchId)))
          disseminationProtocolState.batchesReadyForOrdering should be(empty)
        }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {

      "just update dissemination progress" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        NonQuorumAcksForNode0To6.tail.foreach { quorumAck =>
          availability.receive(quorumAck)

          verify(cryptoProvider).verifySignature(
            AvailabilityAck.hashFor(quorumAck.batchId, anEpochNumber, quorumAck.from, metrics),
            quorumAck.from,
            quorumAck.signature,
            operationId = "availability-signature-verify-ack",
          )

          availability.receive(
            LocalDissemination.RemoteBatchAcknowledgeVerified(
              quorumAck.batchId,
              quorumAck.from,
              quorumAck.signature,
            )
          )
        }

        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNonQuorumVotes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
      }
    }

  "F > 0, " +
    "it receives `< quorum-1` Dissemination.RemoteBatchAcknowledged (w/ duplicates) from nodes " +
    "(i.e., proof of availability is incomplete), " +
    "the batch is being disseminated, " +
    "there are consensus requests" should {

      "update dissemination progress and ignore duplicates" in {
        val disseminationProtocolState = new DisseminationProtocolState()
        disseminationProtocolState.disseminationProgress.addOne(
          ABatchDisseminationProgressNode0To6WithNode0Vote
        )
        disseminationProtocolState.toBeProvidedToConsensus.addOne(AToBeProvidedToConsensus)
        val cryptoProvider = mock[CryptoProvider[IgnoringUnitTestEnv]]
        val availability = createAvailability[IgnoringUnitTestEnv](
          otherNodes = Node1To6,
          cryptoProvider = cryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
        )
        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNode0Vote

        // First time receiving node1Ack: verified by the cryptoProvider and
        // added to the in-memory AvailabilityAck set
        val node1Ack = remoteBatchAcknowledged(1)
        availability.receive(node1Ack)
        verify(cryptoProvider, times(1)).verifySignature(
          AvailabilityAck.hashFor(node1Ack.batchId, anEpochNumber, node1Ack.from, metrics),
          node1Ack.from,
          node1Ack.signature,
          operationId = "availability-signature-verify-ack",
        )
        availability.receive(
          LocalDissemination.RemoteBatchAcknowledgeVerified(
            node1Ack.batchId,
            node1Ack.from,
            node1Ack.signature,
          )
        )
        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNode0And1Votes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus

        // Second time receiving node1Ack: deduplicated before cryptoProvider.verify
        availability.receive(node1Ack)
        verifyNoMoreInteractions(cryptoProvider)
        disseminationProtocolState.disseminationProgress should
          contain only ABatchDisseminationProgressNode0To6WithNode0And1Votes
        disseminationProtocolState.batchesReadyForOrdering should be(empty)
        disseminationProtocolState.toBeProvidedToConsensus should contain only AToBeProvidedToConsensus
      }
    }
}
