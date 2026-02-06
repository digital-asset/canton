// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  IgnoringModuleRef,
  IgnoringUnitTestEnv,
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeModuleExpectingSilence,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import scala.util.Random

class PreIssConsensusModuleTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with HasExecutionContext {

  import PreIssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  "PreIssConsensusModule" should {
    "set up the epoch store and state correctly" in {
      implicit val metricsContext: MetricsContext = MetricsContext.Empty
      implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()
      val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

      Table(
        (
          "latest completed epoch",
          "latest epoch",
          "expected epoch info in state",
          "completed last blocks",
        ),
        (GenesisEpoch, GenesisEpoch, GenesisEpoch.info, Seq.empty),
        (GenesisEpoch, anEpoch, anEpoch.info, Seq.empty),
        (anEpoch, anEpoch, anEpoch.info, Seq.empty),
        (
          anEpoch.copy(lastBlockCommits = someLastBlockCommits),
          anEpoch,
          anEpoch.info,
          someLastBlocks,
        ),
      ).forEvery { (latestCompletedEpoch, latestEpoch, expectedEpochInfoInState, lastBlocks) =>
        val epochStore = mock[EpochStore[ProgrammableUnitTestEnv]]
        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpoch
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() => latestEpoch)
        val epochInProgress = EpochStore.EpochInProgress(Seq.empty, Seq.empty)
        when(epochStore.loadEpochProgress(latestEpoch.info)).thenReturn(() => epochInProgress)
        when(
          epochStore.loadCompleteBlocks(
            EpochNumber(
              latestCompletedEpoch.info.number - RetransmissionsManager.HowManyEpochsToKeep + 1
            ),
            EpochNumber(latestCompletedEpoch.info.number),
          )
        )
          .thenReturn(() => lastBlocks)
        implicit val context
            : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]]()
        val preIssConsensusModule = createPreIssConsensusModule(epochStore)

        preIssConsensusModule.receive(Consensus.Init.KickOff)

        verify(epochStore).latestEpoch(includeInProgress = true)
        verify(epochStore).latestEpoch(includeInProgress = false)
        var selfMessages = context.runPipedMessages()
        selfMessages should contain only Consensus.Init.LatestEpochsLoaded(
          latestCompletedEpoch,
          latestEpoch,
        )

        selfMessages.foreach(preIssConsensusModule.receive)

        verify(epochStore).loadEpochProgress(latestEpoch.info)
        verify(epochStore).loadCompleteBlocks(
          EpochNumber(
            latestCompletedEpoch.info.number - RetransmissionsManager.HowManyEpochsToKeep + 1
          ),
          EpochNumber(latestCompletedEpoch.info.number),
        )
        selfMessages = context.runPipedMessages()
        selfMessages should contain only Consensus.Init.EpochInitDataLoaded(
          latestCompletedEpoch,
          latestEpoch,
          epochInProgress,
          lastBlocks,
        )

        val epochState =
          preIssConsensusModule.initialEpochState(
            latestCompletedEpoch.lastBlockCommits,
            latestEpoch,
            epochInProgress,
          )

        epochState.epoch.info shouldBe expectedEpochInfoInState
        epochState
          .segmentModuleRefFactory(
            new SegmentState(
              Segment(myId, NonEmpty(Seq, BlockNumber.First)), // fake
              epochState.epoch,
              clock,
              completedBlocks = Seq.empty,
              fail(_),
              metrics,
              loggerFactory,
            ),
            mock[EpochMetricsAccumulator],
          )
          .asInstanceOf[IgnoringSegmentModuleRef[IgnoringUnitTestEnv]]
          .latestCompletedEpochLastCommits shouldBe latestCompletedEpoch.lastBlockCommits
      }
    }
  }

  private def createPreIssConsensusModule(
      epochStore: EpochStore[ProgrammableUnitTestEnv]
  ): PreIssConsensusModule[ProgrammableUnitTestEnv] = {
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()

    val orderingTopology = OrderingTopology.forTesting(Set(myId))
    new PreIssConsensusModule[ProgrammableUnitTestEnv](
      OrderingTopologyInfo(
        myId,
        orderingTopology,
        failingCryptoProvider,
        Seq(myId),
        previousTopology = orderingTopology, // not relevant
        failingCryptoProvider,
        Seq(myId),
      ),
      epochLength,
      epochStore,
      None,
      clock,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      new SegmentModuleRefFactory[ProgrammableUnitTestEnv] {
        override def apply(
            context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]],
            epoch: EpochState.Epoch,
            cryptoProvider: CryptoProvider[ProgrammableUnitTestEnv],
            latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
            epochInProgress: EpochInProgress,
        )(
            segmentState: SegmentState,
            metricsAccumulator: EpochMetricsAccumulator,
        ): IgnoringSegmentModuleRef[ConsensusSegment.Message] =
          new IgnoringSegmentModuleRef(latestCompletedEpochLastCommits)
      },
      new Random(4),
      new ConsensusModuleDependencies[ProgrammableUnitTestEnv](
        fakeModuleExpectingSilence,
        fakeModuleExpectingSilence,
        fakeModuleExpectingSilence,
      ),
      loggerFactory,
      timeouts,
    )
  }
}

object PreIssConsensusModuleTest {

  private val epochLength = DefaultEpochLength
  private val myId = BftNodeId("self")
  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val anEpoch =
    EpochStore.Epoch(
      EpochInfo(
        EpochNumber.First,
        BlockNumber.First,
        EpochLength(0),
        TopologyActivationTime(aTimestamp),
      ),
      lastBlockCommits = Seq.empty,
    )
  private def someLastBlockCommits(implicit synchronizerProtocolVersion: ProtocolVersion) = Seq(
    Commit
      .create(
        BlockMetadata(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        Hash.digest(
          HashPurpose.BftOrderingPbftBlock,
          ByteString.EMPTY,
          HashAlgorithm.Sha256,
        ),
        CantonTimestamp.Epoch,
        myId,
      )
      .fakeSign
  )
  private def someLastBlocks(implicit synchronizerProtocolVersion: ProtocolVersion) =
    someLastBlockCommits.map { commitMsg =>
      EpochStore.Block(
        anEpoch.info.number,
        BlockNumber.First,
        CommitCertificate(
          PrePrepare
            .create(
              BlockMetadata.mk(anEpoch.info.number, BlockNumber.First),
              ViewNumber.First,
              OrderingBlock(Seq()),
              CanonicalCommitSet.empty,
              from = myId,
            )
            .fakeSign,
          Seq(commitMsg),
        ),
      )
    }

  final class IgnoringSegmentModuleRef[-MessageT](
      val latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]]
  ) extends IgnoringModuleRef[MessageT] {
    override def asyncSend(
        msg: MessageT
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit = ()
  }

  def createCompletedBlocks(
      epochNumber: EpochNumber,
      numberOfBlocks: Int,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): Seq[EpochStore.Block] =
    LazyList
      .from(0)
      .map(blockNumber =>
        EpochStore.Block(
          epochNumber,
          BlockNumber(blockNumber.toLong),
          CommitCertificate(
            PrePrepare
              .create(
                BlockMetadata.mk(epochNumber, BlockNumber(blockNumber.toLong)),
                ViewNumber.First,
                OrderingBlock(Seq()),
                CanonicalCommitSet.empty,
                from = BftNodeId("self"),
              )
              .fakeSign,
            Seq.empty,
          ),
        )
      )
      .take(numberOfBlocks)
}
