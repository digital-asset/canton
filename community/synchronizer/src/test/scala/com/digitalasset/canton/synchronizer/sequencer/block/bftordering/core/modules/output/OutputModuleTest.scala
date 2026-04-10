// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.Block.TickTopology
import com.digitalasset.canton.synchronizer.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  Bootstrap,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.{
  DefaultRequestInspector,
  FixedResultRequestInspector,
  RequestInspector,
  StartupState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.{
  OutputBlockMetadata,
  OutputEpochMetadata,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  BlacklistLeaderSelectionPolicyState,
  LeaderSelectionInitializer,
  LeaderSelectionPolicy,
  SimpleLeaderSelectionPolicy,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime.MinimumBlockTimeGranularity
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  BaseIgnoringUnitTestEnv,
  FakePipeToSelfCellUnitTestContext,
  FakePipeToSelfCellUnitTestEnv,
  IgnoringUnitTestContext,
  IgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.TopologyFetched
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  EmptyBlockSubscription,
  ModuleRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeCellModule,
  fakeIgnoringModule,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext, LfTimestamp}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.Sink
import org.mockito.Mockito.clearInvocations
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.jdk.DurationConverters.*
import scala.util.Try

class OutputModuleTest
    extends AsyncWordSpecLike
    with BftSequencerBaseTest
    with HasActorSystem
    with HasExecutionContext {

  import OutputModuleTest.*

  "OutputModule" should {
    val initialBlock = anOrderedBlockForOutput()

    "fetch block from availability" when {
      "a block hasn't been provided yet and is not being fetched" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, times(1)).asyncSend(
          eqTo(Availability.LocalOutputFetch.FetchBlockData(initialBlock))
        )(any[TraceContext], any[MetricsContext])
        succeed
      }
    }

    "do nothing" when {
      "a block hasn't been provided yet but is already being fetched" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, times(1)).asyncSend(
          eqTo(Availability.LocalOutputFetch.FetchBlockData(initialBlock))
        )(any[TraceContext], any[MetricsContext])

        clearInvocations(availabilityRef)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, never).asyncSend(
          eqTo(Availability.LocalOutputFetch.FetchBlockData(initialBlock))
        )(any[TraceContext], any[MetricsContext])
        succeed
      }

      "a block has already been provided" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))
        output.receiveInternal(Output.BlockDataFetched(CompleteBlockData(initialBlock, Seq.empty)))

        clearInvocations(availabilityRef)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, never).asyncSend(
          eqTo(Availability.LocalOutputFetch.FetchBlockData(initialBlock))
        )(any[TraceContext], any[MetricsContext])
        succeed
      }
    }

    "store ordered block" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](store = store)()

      val completeBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)

      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(completeBlockData))
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      blocks.size shouldBe 1
      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
    }

    "store 2 blocks out of order" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val store = createOutputMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](store = store)()

      val outOfOrderBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      val initialBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)

      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(outOfOrderBlockData))
      context.sizeOfPipedMessages shouldBe 0

      val emptyBlocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      output.receive(Output.BlockDataFetched(initialBlockData))
      context.sizeOfPipedMessages shouldBe 2
      context.runPipedMessagesAndReceiveOnModule(output) // store blocks

      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()

      emptyBlocks should be(empty)
      blocks.size shouldBe 2

      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
      assertBlock(
        blocks(1),
        expectedBlockNumber = secondBlockNumber,
        expectedTimestamp = aTimestamp.add(MinimumBlockTimeGranularity.toJava),
      )
    }

    "restart with a non-zero initial block number" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val store = createOutputMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](store = store)()
      val completeInitialBlock = CompleteBlockData(initialBlock, batches = Seq.empty)

      // To test that the epoch metadata store is correctly loaded on restart
      store
        .insertEpochIfMissing(
          OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        )
        .apply()

      // Start the output module with an empty store and simulate the arrival of block data and store it.
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(completeInitialBlock))
      context.runPipedMessagesAndReceiveOnModule(output) // store block

      // Restart the output module with a non-zero initial block number.
      val availabilityCell =
        new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
      val orderedBlocksReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
      when(
        orderedBlocksReader.loadOrderedBlocks(initialBlockNumber = BlockNumber.First)(traceContext)
      ).thenReturn(() => Seq(initialBlock))
      when(
        orderedBlocksReader.loadEpochInfo(EpochNumber.First)(traceContext)
      ).thenReturn(() => None)
      val outputAfterRestart =
        createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = secondBlockNumber,
          availabilityRef = fakeCellModule(availabilityCell),
          store = store,
          epochStoreReader = orderedBlocksReader,
        )()
      outputAfterRestart.receive(Output.Start)

      // The output module should rehydrate the pending topology changes flag
      outputAfterRestart.currentEpochCouldAlterOrderingTopology shouldBe true

      // The output module should re-process the last stored block and fetch its data from availability.
      context.selfMessages should contain only Output.BlockOrdered(initialBlock)
      context.selfMessages.foreach(outputAfterRestart.receive)
      availabilityCell.get().getOrElse(fail("Availability not requested for block data")) shouldBe
        Availability.LocalOutputFetch.FetchBlockData(initialBlock)

      // Simulate the arrival of the next block's data and store it (idempotent).
      outputAfterRestart.receive(Output.BlockDataFetched(completeInitialBlock))
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      outputAfterRestart.receive(Output.BlockDataFetched(nextBlockData))
      context.runPipedMessagesAndReceiveOnModule(output) // store block

      // The store should contain both blocks.
      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      blocks.size shouldBe 2
      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
      assertBlock(
        blocks(1),
        expectedBlockNumber = secondBlockNumber,
        expectedTimestamp = aTimestamp.add(MinimumBlockTimeGranularity.toJava),
      )
    }

    "recover from the correct block" in {
      Table(
        "Last stored completed block number" -> "Initial block number to provide",
        None -> BlockNumber.First,
        None -> secondBlockNumber,
        Some(BlockNumber.First) -> BlockNumber.First,
        Some(secondBlockNumber) -> BlockNumber.First,
        Some(BlockNumber.First) -> secondBlockNumber,
        Some(secondBlockNumber) -> secondBlockNumber,
      ).forEvery {
        case (
              lastStoredCompletedBlock: Option[BlockNumber],
              initialBlockNumberToProvide,
            ) =>
          implicit val context
              : ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
            new ProgrammableUnitTestContext(resolveAwaits = true)

          // Expected computation logic for block number to recover from
          val recoverFromBlockNumber = {
            val lastAcknowledgedBlockNumber =
              if (initialBlockNumberToProvide == BlockNumber.First) None
              else Some(BlockNumber(initialBlockNumberToProvide - 1))
            Seq(
              lastAcknowledgedBlockNumber.getOrElse(BlockNumber.First),
              lastStoredCompletedBlock.getOrElse(BlockNumber.First),
            ).min
          }

          val store = mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
          when(store.getEpoch(EpochNumber.First)(traceContext)).thenReturn(() =>
            Some(OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true))
          )
          when(store.getLastBlockInLatestCompletedEpoch(traceContext)).thenReturn(() =>
            lastStoredCompletedBlock.map { blockNumber =>
              OutputBlockMetadata(
                epochNumber = secondEpochNumber,
                blockNumber = blockNumber,
                blockBftTime = aTimestamp,
              )
            }
          )
          when(store.getBlock(recoverFromBlockNumber)(traceContext)).thenReturn(() =>
            Some(
              OutputBlockMetadata(
                epochNumber = secondEpochNumber,
                blockNumber = recoverFromBlockNumber,
                blockBftTime = aTimestamp,
              )
            )
          )
          val lastStoredBlock =
            lastStoredCompletedBlock.map(blockNumber =>
              anOrderedBlockForOutput(blockNumber = blockNumber)
            )
          // The output module will recover from the recovery block, if any, to rebuilt its volatile state.
          val orderedBlocksReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
          when(
            orderedBlocksReader.loadOrderedBlocks(recoverFromBlockNumber)(traceContext)
          ).thenReturn(() => Seq(lastStoredBlock).flatten)
          when(
            orderedBlocksReader.loadEpochInfo(secondEpochNumber)(traceContext)
          ).thenReturn(() => None)
          // The previous block's BFT time will be rehydrated for BFT time computation.
          val previousStoredBlockNumber = BlockNumber(recoverFromBlockNumber - 1)
          val previousStoredBlockBftTime = aTimestamp.minusSeconds(1)
          when(store.getBlock(previousStoredBlockNumber)(traceContext)).thenReturn(() =>
            Option.when(recoverFromBlockNumber > 0)(
              OutputBlockMetadata(
                secondEpochNumber,
                previousStoredBlockNumber,
                previousStoredBlockBftTime,
              )
            )
          )
          val availabilityCell =
            new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
          val output = createOutputModule[ProgrammableUnitTestEnv](
            initialHeight = initialBlockNumberToProvide,
            availabilityRef = fakeCellModule(availabilityCell),
            store = store,
            epochStoreReader = orderedBlocksReader,
          )()
          output.receive(Output.Start)

          verify(store, times(1)).getLastBlockInLatestCompletedEpoch(traceContext)
          verify(orderedBlocksReader, times(1))
            .loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
              traceContext
            )
          context.selfMessages should contain theSameElementsInOrderAs
            lastStoredBlock.toList.map(Output.BlockOrdered.apply)
          output.previousStoredBlock.getBlockNumberAndBftTime should be(
            if (recoverFromBlockNumber > 0) {
              Some(
                (
                  previousStoredBlockNumber,
                  previousStoredBlockBftTime,
                )
              )
            } else {
              None
            }
          )
          output.currentEpochCouldAlterOrderingTopology shouldBe true
      }
    }

    "uses the correct leader selection policy when restarting from older epoch" in {
      val sequencerBlockIsAt = BlockNumber(101L)
      val sequencerEpochIsAt = EpochNumber(10L)
      val sequencerFirstBlockOfEpoch = BlockNumber(75L)
      val sequencerTimestampOfEpoch = CantonTimestamp(LfTimestamp(10000L))
      val outputBlockIsAt = BlockNumber(200L)
      val outputEpochIsAt = EpochNumber(20L)

      val previousBlockNumber = BlockNumber(sequencerFirstBlockOfEpoch - 1)
      val previousTimestamp = CantonTimestamp(LfTimestamp(9000L))

      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)
      val store = mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
      val epochStoreReader = mock[EpochStoreReader[ProgrammableUnitTestEnv]]
      val orderingTopologyProvider = mock[OrderingTopologyProvider[ProgrammableUnitTestEnv]]
      val leaderSelectionInitializer = mock[LeaderSelectionInitializer[ProgrammableUnitTestEnv]]
      val output = createOutputModule(
        initialHeight = sequencerBlockIsAt,
        initialEpochWeHaveLeaderSelectionStateFor = outputEpochIsAt,
        orderingTopologyProvider = orderingTopologyProvider,
        store = store,
        epochStoreReader = epochStoreReader,
        leaderSelectionInitializer = Some(leaderSelectionInitializer),
      )()

      val blacklistState = BlacklistLeaderSelectionPolicyState.create(
        sequencerEpochIsAt,
        sequencerFirstBlockOfEpoch,
        Map.empty,
      )(testedProtocolVersion)
      val orderingTopology = OrderingTopology.forTesting(nodes = Set(BftNodeId("node1")))
      val oldCryptoProvider = mock[CryptoProvider[ProgrammableUnitTestEnv]]
      val policy = mock[LeaderSelectionPolicy[ProgrammableUnitTestEnv]]

      when(store.getLastBlockInLatestCompletedEpoch(traceContext)).thenReturn(() =>
        Some(
          OutputBlockMetadata(
            outputEpochIsAt,
            outputBlockIsAt,
            aTimestamp,
          )
        )
      )
      when(store.getBlock(BlockNumber(sequencerBlockIsAt - 1))(traceContext)).thenReturn(() =>
        Some(
          OutputBlockMetadata(
            sequencerEpochIsAt,
            sequencerBlockIsAt,
            aTimestamp,
          )
        )
      )
      when(epochStoreReader.loadEpochInfo(sequencerEpochIsAt)).thenReturn(() =>
        Some(
          EpochInfo(
            sequencerEpochIsAt,
            sequencerFirstBlockOfEpoch,
            EpochLength(10), // arbitrary
            TopologyActivationTime(sequencerTimestampOfEpoch),
          )
        )
      )
      when(epochStoreReader.loadOrderedBlocks(sequencerFirstBlockOfEpoch)).thenReturn(() =>
        Seq(
          anOrderedBlockForOutput(sequencerEpochIsAt, sequencerBlockIsAt)
        )
      )
      when(store.getBlock(previousBlockNumber))
        .thenReturn(() =>
          Some(
            OutputBlockMetadata(
              EpochNumber(sequencerEpochIsAt - 1),
              previousBlockNumber,
              previousTimestamp,
            )
          )
        )
      when(store.getEpoch(sequencerEpochIsAt)).thenReturn(() =>
        Some(
          OutputEpochMetadata(
            sequencerEpochIsAt,
            couldAlterOrderingTopology = true,
          )
        )
      )
      when(
        orderingTopologyProvider.getOrderingTopologyAt(
          Some(TopologyActivationTime(sequencerTimestampOfEpoch)),
          checkPendingChanges = true,
        )
      ).thenReturn(() => Some(orderingTopology -> oldCryptoProvider))
      when(store.getLeaderSelectionPolicyState(sequencerEpochIsAt)).thenReturn(() =>
        Some(blacklistState)
      )
      when(leaderSelectionInitializer.leaderSelectionPolicy(blacklistState, orderingTopology))
        .thenReturn(policy)

      output.receive(Output.Start)

      output.previousStoredBlock.getBlockNumberAndBftTime shouldBe Some(
        previousBlockNumber -> previousTimestamp
      )
      output.leaderSelectionPolicy shouldBe policy
    }

    "allow subscription from the initial block" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          initialHeight = BlockNumber.First,
          timeouts,
          loggerFactory,
        )(fail(_))
      val output =
        createOutputModule[FakePipeToSelfCellUnitTestEnv](store = store)(
          blockSubscription
        )

      val initialBlockData =
        CompleteBlockData(
          initialBlock,
          batches = Seq(
            OrderingRequestBatch.create(
              Seq(Traced(OrderingRequest(aTag, messageId = "", ByteString.EMPTY))),
              EpochNumber.First,
            )
          ).map(x => BatchId.from(x) -> x),
        )
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(initialBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("Callback didn't send message"))
      )
      output.receive(Output.BlockDataFetched(nextBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("Callback didn't send message"))
      )

      blockSubscription.subscription().take(2).runWith(Sink.seq).map { blocks =>
        blocks.size shouldBe 2
        val initialBlock = blocks.head.value
        initialBlock.blockHeight shouldBe BlockNumber.First
        initialBlock.requests.size shouldBe 1
        initialBlock.requests.head shouldBe Traced(
          OrderedRequest(aTimestamp.toMicros, aTag, ByteString.EMPTY, BftNodeId.Empty)
        )
        val nextBlock = blocks(1)
        nextBlock.value.blockHeight shouldBe secondBlockNumber
        nextBlock.value.requests should be(empty)
      }
    }

    "allow subscription from a non-0 height" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]

      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          secondBlockNumber,
          timeouts,
          loggerFactory,
        )(fail(_))
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](
        store = store,
        initialHeight = secondBlockNumber,
      )(blockSubscription)

      val initialBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(initialBlockData))
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block
      output.receive(Output.BlockDataFetched(nextBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("didn't send message"))
      )

      blockSubscription.subscription().runWith(Sink.head).map { block =>
        block.value.blockHeight shouldBe secondBlockNumber
        block.value.requests should be(empty)
      }
    }

    "not send a block upstream if the number is lower than the initial height" in {
      implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
        IgnoringUnitTestContext()

      val store = createOutputMetadataStore[IgnoringUnitTestEnv]

      val initialHeight = BlockNumber(2L)
      val blockSubscription =
        new PekkoBlockSubscription[IgnoringUnitTestEnv](initialHeight, timeouts, loggerFactory)(
          fail(_)
        )
      val output = createOutputModule[IgnoringUnitTestEnv](
        store = store,
        initialHeight = initialHeight,
      )(blockSubscription)
      output.receive(Output.Start)

      val blocks =
        (secondBlockNumber to initialHeight)
          .map(blockNumber =>
            anOrderedBlockForOutput(
              blockNumber = blockNumber,
              commitTimestamp = CantonTimestamp.MinValue, /* irrelevant for the test */
            )
          )
          .map(CompleteBlockData(_, batches = Seq.empty))
      blocks.foreach(blockData =>
        output.receive(
          Output.BlockDataStored(
            blockData,
            blockData.orderedBlockForOutput.orderedBlock.metadata.blockNumber,
            CantonTimestamp.MinValue, // irrelevant for the test case
            epochCouldAlterOrderingTopology = false, // irrelevant for the test case
          )
        )
      )

      blockSubscription.subscription().runWith(Sink.head).map { block =>
        block.value.blockHeight shouldBe initialHeight
        block.value.requests should be(empty)
      }
    }

    "set the potential topology change flag to `true`, " +
      "request to tick the topology, " +
      "fetch a new topology, " +
      "if not in the middle of state transfer send the topology to consensus and " +
      "set up the new topology including the potential topology changes flag if pending changes are reported" when {

        "at least one block in the just completed epoch has requests to all members of synchronizer" in {
          val topologyActivationTime = TopologyActivationTime(anotherTimestamp.immediateSuccessor)

          Table(
            ("Pending Canton topology changes", "block mode"),
            (
              false,
              OrderedBlockForOutput.Mode.FromConsensus,
            ),
            (
              true,
              OrderedBlockForOutput.Mode.FromConsensus,
            ),
            (
              false,
              OrderedBlockForOutput.Mode.FromStateTransfer,
            ),
            (
              true,
              OrderedBlockForOutput.Mode.FromStateTransfer,
            ),
          ).forEvery { case (pendingChanges, blockMode) =>
            val store = spy(createOutputMetadataStore[ProgrammableUnitTestEnv])
            val topologyProviderMock = mock[OrderingTopologyProvider[ProgrammableUnitTestEnv]]
            val consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]]
            val newOrderingTopology =
              OrderingTopology.forTesting(
                nodes = Set(BftNodeId("node1")),
                SequencingParameters.Default,
                topologyActivationTime,
                areTherePendingCantonTopologyChanges = Some(pendingChanges),
              )
            val newCryptoProvider = failingCryptoProvider[ProgrammableUnitTestEnv]
            when(
              topologyProviderMock.getOrderingTopologyAt(
                Some(topologyActivationTime),
                checkPendingChanges = true,
              )
            )
              .thenReturn(() => Some((newOrderingTopology, newCryptoProvider)))
            val subscriptionBlocks = mutable.Queue.empty[Traced[BlockFormat.Block]]
            implicit val context
                : ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext(resolveAwaits = true)
            val output = createOutputModule[ProgrammableUnitTestEnv](
              store = store,
              consensusRef = consensusRef,
              orderingTopologyProvider = topologyProviderMock,
              requestInspector = new AccumulatingRequestInspector,
            )(blockSubscription = new EnqueueingBlockSubscription(subscriptionBlocks))

            val blockData1 = // lastInEpoch = false, isRequestToAllMembersOfSynchronizer = true
              completeBlockData(
                BlockNumber.First,
                commitTimestamp = aTimestamp,
                mode = blockMode,
              )
            val blockData2 = // lastInEpoch = true, isRequestToAllMembersOfSynchronizer = false
              completeBlockData(
                BlockNumber(BlockNumber.First + 1L),
                commitTimestamp = anotherTimestamp,
                lastInEpoch = true,
                mode = blockMode,
              )

            output.receive(Output.Start)
            output.receive(Output.BlockDataFetched(blockData1))

            val piped1 = context.runPipedMessages()
            piped1 should contain only Output.BlockDataStored(
              blockData1,
              BlockNumber.First,
              aTimestamp,
              epochCouldAlterOrderingTopology = true,
            )
            piped1.foreach(output.receive) // Store first block's metadata

            output.currentEpochCouldAlterOrderingTopology shouldBe true

            output.receive(Output.BlockDataFetched(blockData2))

            val piped2 = context.runPipedMessages()
            piped2 should contain only Output.BlockDataStored(
              blockData2,
              BlockNumber(1L),
              anotherTimestamp,
              epochCouldAlterOrderingTopology = true,
            )
            piped2.foreach(output.receive) // Store last block's metadata

            // The epoch metadata should be stored only once, i.e.,
            //  only for the first block after epochCouldAlterOrderingTopology is set
            verify(store, times(1)).insertEpochIfMissing(
              OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
            )

            context.runPipedMessagesThenVerifyAndReceiveOnModule(output) {
              _ shouldBe Output.UpdateLeaderSelection(
                Output.TopologyFetched(
                  EpochNumber(1L), // Epoch number
                  newOrderingTopology,
                  newCryptoProvider,
                )
              )
            }
            val piped3 = context.runPipedMessages()
            piped3 should contain only
              Output.TopologyFetched(
                EpochNumber(1L), // Epoch number
                newOrderingTopology,
                newCryptoProvider,
              )

            // All blocks have now been output to the subscription
            subscriptionBlocks should have size 2
            val block1 = subscriptionBlocks.dequeue().value
            block1.blockHeight shouldBe BlockNumber.First
            block1.tickTopology shouldBe None
            val block2 = subscriptionBlocks.dequeue().value
            block2.blockHeight shouldBe BlockNumber(1)
            // We should tick even during state transfer if the epoch has potential sequencer topology changes
            block2.tickTopology shouldBe Some(
              TickTopology(anotherTimestamp.toMicros, broadcast = false)
            )

            verify(topologyProviderMock, times(1))
              .getOrderingTopologyAt(Some(topologyActivationTime), checkPendingChanges = true)
            // Update the last block if needed and set up the new topology
            piped3.foreach(output.receive)

            if (pendingChanges) { // Then the last block will be updated
              val piped4 = context.runPipedMessages()
              piped4 should matchPattern {
                case Seq(
                      Output.MetadataStoredForNewEpoch(
                        1L, // Epoch number
                        `newOrderingTopology`,
                        _, // A fake crypto provider instance
                      )
                    ) =>
              }
              piped4.foreach(output.receive)
            }

            // The topology alteration flag should be set if needed
            output.currentEpochCouldAlterOrderingTopology shouldBe pendingChanges

            if (output.currentEpochCouldAlterOrderingTopology) {
              // Store epoch metadata
              val piped5 = context.runPipedMessages()
              piped5 should be(empty)
            }

            verify(consensusRef, times(1)).asyncSend(
              Consensus.NewEpochTopology(
                secondEpochNumber,
                Membership(BftNodeId("node1"), newOrderingTopology, Seq.empty),
                any[CryptoProvider[ProgrammableUnitTestEnv]],
              )
            )(any[TraceContext], any[MetricsContext])

            succeed
          }
        }
      }

    "process NewEpochTopology messages sequentially in order" when {
      "new topologies are fetched out-of-order" in {
        val consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]]
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)
        val output = createOutputModule[ProgrammableUnitTestEnv](consensusRef = consensusRef)()

        output.receive(Output.Start)

        output.maybeNewEpochTopologyMessagePeanoQueue.putIfAbsent(
          new PeanoQueue(EpochNumber.First)(fail(_))
        )

        // the behavior will always be the same across block modes, so the chosen one is irrelevant
        val anOrderingTopology =
          OrderingTopology.forTesting(
            nodes = Set(BftNodeId("node1")),
            SequencingParameters.Default,
          )
        val aNewMembership =
          Membership(BftNodeId("node1"), anOrderingTopology, Seq(BftNodeId("node1")))
        val aCryptoProvider = failingCryptoProvider[ProgrammableUnitTestEnv]
        output.receive(
          TopologyFetched(
            secondEpochNumber,
            anOrderingTopology,
            aCryptoProvider,
          )
        )

        verify(consensusRef, never).asyncSend(
          eqTo(
            Consensus.NewEpochTopology(
              secondEpochNumber,
              aNewMembership,
              aCryptoProvider,
            )
          )
        )(any[TraceContext], any[MetricsContext])

        output.receive(
          TopologyFetched(
            EpochNumber.First,
            anOrderingTopology,
            aCryptoProvider,
          )
        )

        val order = inOrder(consensusRef)
        order
          .verify(consensusRef, times(1))
          .asyncSend(
            eqTo(
              Consensus.NewEpochTopology(
                EpochNumber.First,
                aNewMembership,
                aCryptoProvider,
              )
            )
          )(any[TraceContext], any[MetricsContext])
        order
          .verify(consensusRef, times(1))
          .asyncSend(
            eqTo(
              Consensus.NewEpochTopology(
                secondEpochNumber,
                aNewMembership,
                aCryptoProvider,
              )
            )
          )(any[TraceContext], any[MetricsContext])

        succeed
      }
    }

    "not process a block from a future epoch" when {
      "receiving multiple state-transferred blocks" in {
        val subscriptionBlocks = mutable.Queue.empty[Traced[BlockFormat.Block]]
        val output =
          createOutputModule[ProgrammableUnitTestEnv](requestInspector =
            new FixedResultRequestInspector(true)
          )(
            blockSubscription = new EnqueueingBlockSubscription(subscriptionBlocks)
          )
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)

        val blockData1 =
          completeBlockData(
            BlockNumber.First,
            aTimestamp,
            lastInEpoch = false, // Do not complete the epoch!
            EpochNumber.First,
            mode = OrderedBlockForOutput.Mode.FromStateTransfer,
          )
        val blockNumber2 = BlockNumber(BlockNumber.First + 1L)
        val blockData2 =
          completeBlockData(
            blockNumber2,
            anotherTimestamp,
            epochNumber = EpochNumber(EpochNumber.First + 1L),
            mode = OrderedBlockForOutput.Mode.FromStateTransfer,
          )

        output.receive(Output.Start)
        // We insert blocks out of order to ensure that processing fetched blocks doesn't cross epoch
        //  boundaries even when multiple blocks from multiple epochs are next in the peano queue.
        output.receive(Output.BlockDataFetched(blockData2))
        output.receive(Output.BlockDataFetched(blockData1))

        val piped1 = context.runPipedMessages()

        // Only the block in the first epoch will be polled from the completed blocks queue
        piped1 should contain only
          Output.BlockDataStored(
            blockData1,
            BlockNumber.First,
            aTimestamp,
            epochCouldAlterOrderingTopology = true,
          )
        piped1.foreach(output.receive) // Store blocks' metadata

        // Only the first block has now been output to the subscription after its metadata has been stored
        context.runPipedMessages() shouldBe empty
        subscriptionBlocks should have size 1

        // The topology alteration flag should not be reset
        output.currentEpochCouldAlterOrderingTopology shouldBe true
      }
    }

    "not insert epoch metadata again" when {
      "the cache about epoch metadata says it's been inserted already" in {
        val store = spy(createOutputMetadataStore[ProgrammableUnitTestEnv])
        val output = createOutputModule[ProgrammableUnitTestEnv](
          store = store,
          requestInspector = new FixedResultRequestInspector(true),
        )()
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)

        val blockData1 =
          completeBlockData(
            BlockNumber.First,
            aTimestamp,
            lastInEpoch = false,
            EpochNumber.First,
          )
        val blockNumber2 = BlockNumber(BlockNumber.First + 1L)
        val blockData2 =
          completeBlockData(
            blockNumber2,
            anotherTimestamp,
            epochNumber = EpochNumber.First,
          )

        output.receive(Output.Start)

        output.receive(Output.BlockDataFetched(blockData1))
        // Store the epoch and block metadata
        context.runPipedMessagesUntilNoMorePiped(output)

        // The cache should say that the epoch metadata has been stored
        output.epochsWithMetadataStoredCache should contain only EpochNumber.First

        output.receive(Output.BlockDataFetched(blockData2))
        // Store the block metadata only
        context.runPipedMessagesUntilNoMorePiped(output)

        // Epoch metadata should have been inserted only once
        verify(store, times(1)).insertEpochIfMissing(
          OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        )
        // The cache should still say that the epoch metadata has been stored
        output.epochsWithMetadataStoredCache should contain only EpochNumber.First
      }
    }

    "cache correctly that epoch metadata has been stored, but only for the last 2 epochs" when {
      "processing block insertions out of order across > 2 epochs" in {
        val output =
          createOutputModule[ProgrammableUnitTestEnv](consensusRef = fakeIgnoringModule)()
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)

        output.receive(Output.Start)

        output.maybeNewEpochTopologyMessagePeanoQueue.putIfAbsent(
          new PeanoQueue(EpochNumber.First)(fail(_))
        )

        val blockData1 =
          completeBlockData(
            BlockNumber.First,
            aTimestamp,
            epochNumber = EpochNumber.First,
          )
        val blockDataStored1 =
          Output.BlockDataStored(
            blockData1,
            BlockNumber.First,
            aTimestamp,
            epochCouldAlterOrderingTopology = true,
          )
        val blockNumber2 = BlockNumber(BlockNumber.First + 1L)
        val epochNumber2 = EpochNumber(EpochNumber.First + 1L)
        val blockData2 =
          completeBlockData(
            blockNumber2,
            anotherTimestamp,
            epochNumber = epochNumber2,
          )
        val blockDataStored2 =
          Output.BlockDataStored(
            blockData2,
            blockNumber2,
            anotherTimestamp,
            epochCouldAlterOrderingTopology = true,
          )
        val blockNumber3 = BlockNumber(BlockNumber.First + 2L)
        val epochNumber3 = EpochNumber(EpochNumber.First + 2L)
        val blockData3 =
          completeBlockData(
            blockNumber3,
            yetAnotherTimestamp,
            epochNumber = epochNumber3,
          )
        val blockDataStored3 =
          Output.BlockDataStored(
            blockData3,
            blockNumber3,
            yetAnotherTimestamp,
            epochCouldAlterOrderingTopology = true,
          )

        val anOrderingTopology =
          OrderingTopology.forTesting(Set(BftNodeId("node1"))) // Irrelevant for this test
        val aCryptoProvider = failingCryptoProvider[ProgrammableUnitTestEnv]

        output.epochsWithMetadataStoredCache shouldBe empty

        // Receive-blocks in different epochs out of order
        output.receive(blockDataStored3)
        output.receive(blockDataStored2)

        output.epochsWithMetadataStoredCache should contain theSameElementsAs Seq(
          epochNumber2,
          epochNumber3,
        )

        output.receive(blockDataStored1)

        output.epochsWithMetadataStoredCache should contain theSameElementsAs Seq(
          EpochNumber.First,
          epochNumber2,
          epochNumber3,
        )

        // Switch to the third epoch and reduce the cache
        output.receive(TopologyFetched(EpochNumber.First, anOrderingTopology, aCryptoProvider))
        output.receive(TopologyFetched(epochNumber2, anOrderingTopology, aCryptoProvider))
        output.receive(TopologyFetched(epochNumber3, anOrderingTopology, aCryptoProvider))
        context.runPipedMessagesUntilNoMorePiped(output)

        output.epochsWithMetadataStoredCache should contain theSameElementsAs Seq(
          epochNumber2,
          epochNumber3,
        )

      }
    }

    "not try to issue a new topology but still send a topology to consensus" when {
      "no block in the epoch has requests to all members of synchronizer" in {
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)
        val topologyProviderSpy =
          spy(new FakeOrderingTopologyProvider[ProgrammableUnitTestEnv])
        val consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]]
        val output = createOutputModule[ProgrammableUnitTestEnv](
          initialOrderingTopology = OrderingTopology.forTesting(Set(BftNodeId("node1"))),
          orderingTopologyProvider = topologyProviderSpy,
          consensusRef = consensusRef,
          requestInspector = new FixedResultRequestInspector(false),
        )()

        val blockData =
          completeBlockData(BlockNumber.First, commitTimestamp = aTimestamp, lastInEpoch = true)

        output.receive(Output.Start)
        output.receive(Output.BlockDataFetched(blockData))
        output.currentEpochCouldAlterOrderingTopology shouldBe false

        context.runPipedMessagesThenVerifyAndReceiveOnModule(output) { msg =>
          msg shouldBe Output.BlockDataStored(
            blockData,
            BlockNumber.First,
            aTimestamp,
            epochCouldAlterOrderingTopology = false,
          )
        }

        context.runPipedMessages() shouldBe Seq.empty

        verify(topologyProviderSpy, never).getOrderingTopologyAt(
          any[Option[TopologyActivationTime]],
          checkPendingChanges = any[Boolean],
        )(
          any[TraceContext]
        )
        verify(consensusRef, times(1)).asyncSend(
          Consensus.NewEpochTopology(
            secondEpochNumber,
            Membership.forTesting(BftNodeId("node1")),
            any[CryptoProvider[ProgrammableUnitTestEnv]],
          )
        )(any[TraceContext], any[MetricsContext])
        succeed
      }
    }

    "get sequencer snapshot additional info" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val store = createOutputMetadataStore[ProgrammableUnitTestEnv]
      val epochStore = createEpochStore[ProgrammableUnitTestEnv]
      val sequencerNodeRef = mock[ModuleRef[SequencerNode.SnapshotMessage]]
      val node1 = BftNodeId("node1")
      val node2 = BftNodeId("node2")
      val node1ActivationTime = TopologyActivationTime(aTimestamp.minusMillis(2))
      val node2ActivationTime = TopologyActivationTime(aTimestamp)
      val topologyActivationTime =
        TopologyActivationTime(aTimestamp.plusMillis(2))
      val previousTopologyActivationTime =
        TopologyActivationTime(topologyActivationTime.value.minusSeconds(1L))
      val topology = OrderingTopology(
        nodesTopologyInfo = Map(
          node1 -> nodeTopologyInfo(),
          node2 -> nodeTopologyInfo(),
          BftNodeId("node from the future") -> nodeTopologyInfo(),
        ),
        DefaultEpochLength,
        SequencingParameters.Default,
        defaultMaxBytesToDecompress, // irrelevant for this test
        topologyActivationTime,
        areTherePendingCantonTopologyChanges = Some(false),
      )
      val firstKnownAt = Map(
        node1 -> node1ActivationTime,
        node2 -> node2ActivationTime,
        BftNodeId("node from the future") -> TopologyActivationTime(aTimestamp.plusMillis(1)),
      )

      def bftTimeForBlockInFirstEpoch(blockNumber: Long) =
        node2ActivationTime.value.minusSeconds(1).plusMillis(blockNumber)

      // Store the "previous epoch"
      epochStore
        .startEpoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            DefaultEpochLength,
            previousTopologyActivationTime,
          )
        )
        .apply()
      epochStore.completeEpoch(EpochNumber.First).apply()
      // Store the "current epoch"
      epochStore
        .startEpoch(
          EpochInfo(
            EpochNumber(1L),
            BlockNumber(DefaultEpochLength),
            DefaultEpochLength,
            topologyActivationTime,
          )
        )
        .apply()
      store
        .insertEpochIfMissing(
          OutputEpochMetadata(EpochNumber(1L), couldAlterOrderingTopology = true)
        )
        .apply()

      val output =
        createOutputModule[ProgrammableUnitTestEnv](
          initialOrderingTopology = topology,
          store = store,
          epochStoreReader = epochStore,
          orderingTopologyProvider = new FakeOrderingTopologyProvider(Some(firstKnownAt)),
          consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]],
        )()
      output.receive(Output.Start)

      // Store "previous epoch" blocks
      for (blockNumber <- BlockNumber.First until DefaultEpochLength) {
        output.receive(
          Output.BlockDataFetched(
            CompleteBlockData(
              anOrderedBlockForOutput(
                blockNumber = blockNumber,
                commitTimestamp = bftTimeForBlockInFirstEpoch(blockNumber),
              ),
              batches = Seq.empty,
            )
          )
        )
        context.runPipedMessages() // store block
      }

      // Progress to the next epoch
      output.maybeNewEpochTopologyMessagePeanoQueue.putIfAbsent(
        new PeanoQueue(EpochNumber(1L))(fail(_))
      )
      output.receive(Output.TopologyFetched(EpochNumber(1L), topology, failingCryptoProvider))

      // Store the first block in the "current epoch"
      output.receive(
        Output.BlockDataFetched(
          CompleteBlockData(
            anOrderedBlockForOutput(
              epochNumber = 1L,
              blockNumber = DefaultEpochLength,
              commitTimestamp = node2ActivationTime.value,
            ),
            batches = Seq.empty,
          )
        )
      )
      context.runPipedMessages() // store block

      output.receive(
        Output.SequencerSnapshotMessage
          .GetAdditionalInfo(timestamp = node2ActivationTime.value, sequencerNodeRef)
      )

      context.runPipedMessagesUntilNoMorePiped(output)

      verify(sequencerNodeRef, times(1)).asyncSend(
        eqTo(
          SequencerNode.SnapshotMessage.AdditionalInfo(
            v30.BftSequencerSnapshotAdditionalInfo(
              Map(
                node1 ->
                  v30.BftSequencerSnapshotAdditionalInfo
                    .SequencerActiveAt(
                      timestamp = node1ActivationTime.value.toMicros,
                      startEpochNumber = Some(EpochNumber.First),
                      firstBlockNumberInStartEpoch = Some(BlockNumber.First),
                      startEpochTopologyQueryTimestamp =
                        Some(previousTopologyActivationTime.value.toMicros),
                      startEpochCouldAlterOrderingTopology = None,
                      previousBftTime = None,
                      previousEpochTopologyQueryTimestamp = None,
                      leaderSelectionPolicyState = None,
                    ),
                node2 ->
                  v30.BftSequencerSnapshotAdditionalInfo
                    .SequencerActiveAt(
                      timestamp = node2ActivationTime.value.toMicros,
                      startEpochNumber = Some(EpochNumber(1L)),
                      firstBlockNumberInStartEpoch = Some(BlockNumber(DefaultEpochLength)),
                      startEpochTopologyQueryTimestamp =
                        Some(topologyActivationTime.value.toMicros),
                      startEpochCouldAlterOrderingTopology = Some(true),
                      previousBftTime = Some(
                        bftTimeForBlockInFirstEpoch(BlockNumber(DefaultEpochLength - 1L)).toMicros
                      ),
                      previousEpochTopologyQueryTimestamp =
                        Some(previousTopologyActivationTime.value.toMicros),
                      leaderSelectionPolicyState = None,
                    ),
              )
            )
          )
        )
      )(any[TraceContext], any[MetricsContext])
      succeed
    }
  }

  "adjust time for a state-transferred block based on the previous BFT time" in {
    val cell =
      new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](None)
    implicit val context
        : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
      FakePipeToSelfCellUnitTestContext(cell)

    val blockNumber = BlockNumber(2L)
    val availabilityRef = mock[ModuleRef[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
    val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
    val previousBftTimeForOnboarding = Some(aTimestamp)
    val output =
      createOutputModule[FakePipeToSelfCellUnitTestEnv](
        initialHeight = blockNumber,
        availabilityRef = availabilityRef,
        store = store,
        previousBftTimeForOnboarding = previousBftTimeForOnboarding,
      )()

    // Needs to be earlier than the previous block BFT time so that we test monotonicity adjustment.
    val newBlockCommitTime = aTimestamp.minusSeconds(1)
    val block = anOrderedBlockForOutput(
      blockNumber = blockNumber,
      commitTimestamp = newBlockCommitTime,
    )
    output.receive(Output.Start)
    output.receive(Output.BlockOrdered(block))

    verify(availabilityRef, times(1)).asyncSend(
      eqTo(Availability.LocalOutputFetch.FetchBlockData(block))
    )(any[TraceContext], any[MetricsContext])
    val completeBlockData = CompleteBlockData(block, batches = Seq.empty)
    output.receive(Output.BlockDataFetched(completeBlockData))
    cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

    val blocks = store.getBlockFromInclusive(initialBlockNumber = blockNumber)(traceContext)()
    blocks.size shouldBe 1
    assertBlock(
      blocks.head,
      expectedBlockNumber = blockNumber,
      expectedTimestamp = aTimestamp.plus(BftTime.MinimumBlockTimeGranularity.toJava),
    )
    succeed
  }

  "set that there are pending topology changes based on onboarding state" in {
    val output =
      createOutputModule[FakePipeToSelfCellUnitTestEnv](
        areTherePendingTopologyChangesInOnboardingEpoch = true
      )()

    output.currentEpochCouldAlterOrderingTopology shouldBe true
  }

  private def completeBlockData(
      blockNumber: BlockNumber,
      commitTimestamp: CantonTimestamp,
      lastInEpoch: Boolean = false,
      epochNumber: EpochNumber = EpochNumber.First,
      mode: OrderedBlockForOutput.Mode = OrderedBlockForOutput.Mode.FromConsensus,
  ): CompleteBlockData =
    CompleteBlockData(
      anOrderedBlockForOutput(
        epochNumber,
        blockNumber,
        commitTimestamp,
        lastInEpoch,
        mode,
      ),
      batches = Seq(
        OrderingRequestBatch.create(
          Seq(Traced(OrderingRequest(aTag, messageId = "", ByteString.EMPTY))),
          epochNumber,
        )
      ).map(x => BatchId.from(x) -> x),
    )

  private def assertBlock(
      actualBlock: OutputMetadataStore.OutputBlockMetadata,
      expectedBlockNumber: BlockNumber,
      expectedTimestamp: CantonTimestamp,
  ) = {
    actualBlock.blockNumber shouldBe expectedBlockNumber
    actualBlock.blockBftTime shouldBe expectedTimestamp
  }

  private def createOutputModule[E <: BaseIgnoringUnitTestEnv[E]](
      initialHeight: Long = BlockNumber.First,
      initialEpochWeHaveLeaderSelectionStateFor: Long = Bootstrap.BootstrapEpochNumber,
      initialOrderingTopology: OrderingTopology = OrderingTopology.forTesting(nodes = Set.empty),
      availabilityRef: ModuleRef[Availability.Message[E]] = fakeModuleExpectingSilence,
      consensusRef: ModuleRef[Consensus.Message[E]] = fakeModuleExpectingSilence,
      store: OutputMetadataStore[E] = createOutputMetadataStore[E],
      epochStoreReader: EpochStoreReader[E] = createEpochStore[E],
      leaderSelectionInitializer: Option[LeaderSelectionInitializer[E]] = None,
      orderingTopologyProvider: OrderingTopologyProvider[E] = new FakeOrderingTopologyProvider[E],
      previousBftTimeForOnboarding: Option[CantonTimestamp] = None,
      areTherePendingTopologyChangesInOnboardingEpoch: Boolean = false,
      requestInspector: RequestInspector = DefaultRequestInspector,
  )(
      blockSubscription: BlockSubscription = new EmptyBlockSubscription
  ): OutputModule[E] = {
    val thisNode = BftNodeId("node1")
    val startupState =
      StartupState[E](
        thisNode,
        BlockNumber(initialHeight),
        EpochNumber(initialEpochWeHaveLeaderSelectionStateFor),
        previousBftTimeForOnboarding,
        areTherePendingTopologyChangesInOnboardingEpoch,
        failingCryptoProvider,
        initialOrderingTopology,
        initialLowerBound = None,
        new SimpleLeaderSelectionPolicy[E],
      )
    val config = new BftBlockOrdererConfig()
    new OutputModule(
      startupState,
      orderingTopologyProvider,
      leaderSelectionInitializer.getOrElse(
        LeaderSelectionInitializer.create(
          thisNode,
          config,
          testedProtocolVersion,
          store,
          timeouts,
          error => _ => fail(error),
          SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          loggerFactory,
        )
      ),
      store,
      epochStoreReader,
      blockSubscription,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      availabilityRef,
      consensusRef,
      loggerFactory,
      timeouts,
      requestInspector,
    )(
      config,
      synchronizerProtocolVersion,
      MetricsContext.Empty,
      NoReportingTracerProvider.tracer,
    )
  }

  private def createOutputMetadataStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new TestOutputMetadataStore[E]

  private def createEpochStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new GenericInMemoryEpochStore[E] {

      override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
        () => value().getOrElse(fail())

      override def close(): Unit = ()
    }
}

object OutputModuleTest {

  private class AccumulatingRequestInspector extends RequestInspector {
    // Ensure that `currentEpochCouldAlterOrderingTopology` accumulates correctly by processing
    //  more than one block and alternating the outcome starting from `true`.
    private var outcome = true

    override def isRequestToAllMembersOfSynchronizer(
        blockMetadata: BlockMetadata,
        requestNumber: Int,
        _request: OrderingRequest,
        _maxBytesToDecompress: MaxBytesToDecompress,
        _logger: TracedLogger,
        _traceContext: TraceContext,
    )(implicit _synchronizerProtocolVersion: ProtocolVersion): Boolean = {
      val result = outcome
      outcome = !outcome
      result
    }
  }

  class TestOutputMetadataStore[E <: BaseIgnoringUnitTestEnv[E]]
      extends GenericInMemoryOutputMetadataStore[E]
      with BaseTest {

    override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
      () => value().getOrElse(fail())

    override def close(): Unit = ()

    override protected def reportError(errorMessage: String)(implicit
        traceContext: TraceContext
    ): Unit = fail(errorMessage)
  }

  private class EnqueueingBlockSubscription(
      subscriptionBlocks: mutable.Queue[Traced[BlockFormat.Block]]
  ) extends EmptyBlockSubscription {

    override def receiveBlock(block: BlockFormat.Block)(implicit
        traceContext: TraceContext
    ): Unit =
      subscriptionBlocks.enqueue(Traced(block))
  }

  private class FakeOrderingTopologyProvider[E <: BaseIgnoringUnitTestEnv[E]](
      firstKnownAtAnswer: Option[Map[BftNodeId, TopologyActivationTime]] = None
  ) extends OrderingTopologyProvider[E] {

    override def getOrderingTopologyAt(
        activationTime: Option[TopologyActivationTime],
        checkPendingChanges: Boolean,
    )(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[Option[(OrderingTopology, CryptoProvider[E])]] =
      createFuture(None)

    override def getFirstKnownAt(activationTime: TopologyActivationTime)(implicit
        traceContext: TraceContext
    ): () => Option[Map[BftNodeId, TopologyActivationTime]] =
      createFuture(firstKnownAtAnswer)

    private def createFuture[A](a: A): E#FutureUnlessShutdownT[A] =
      () => a
  }

  private val aTag = BlockFormat.SendTag
  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val anotherTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:01:00.000Z"))
  private val yetAnotherTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:02:00.000Z"))

  private val secondEpochNumber = EpochNumber(1L)
  private val secondBlockNumber = BlockNumber(1L)

  private def nodeTopologyInfo() =
    NodeTopologyInfo(
      keyIds = Set.empty
    )

  def anOrderedBlockForOutput(
      epochNumber: Long = EpochNumber.First,
      blockNumber: Long = BlockNumber.First,
      commitTimestamp: CantonTimestamp = aTimestamp,
      lastInEpoch: Boolean = false,
      mode: OrderedBlockForOutput.Mode = OrderedBlockForOutput.Mode.FromConsensus,
      batchIds: Seq[BatchId] = Seq.empty,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): OrderedBlockForOutput =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata(EpochNumber(epochNumber), BlockNumber(blockNumber)),
        batchRefs =
          batchIds.map(id => ProofOfAvailability(id, Seq.empty, EpochNumber(epochNumber))),
        CanonicalCommitSet(
          Set(
            Commit
              .create(
                BlockMetadata.mk(epochNumber = -1L /* whatever */, blockNumber - 1),
                viewNumber = ViewNumber.First,
                Hash
                  .digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
                commitTimestamp,
                from = BftNodeId.Empty,
              )
              .fakeSign
          )
        ),
      ),
      ViewNumber.First,
      BftNodeId.Empty,
      lastInEpoch,
      mode,
    )
}
