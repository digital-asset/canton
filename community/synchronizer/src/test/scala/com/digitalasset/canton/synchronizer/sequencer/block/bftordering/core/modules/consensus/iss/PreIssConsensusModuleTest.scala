// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  IgnoringUnitTestContext,
  IgnoringUnitTestEnv,
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

  private implicit val context: IgnoringUnitTestContext[Consensus.Message[IgnoringUnitTestEnv]] =
    IgnoringUnitTestContext()

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
        ),
        (GenesisEpoch, GenesisEpoch, GenesisEpoch.info),
        (GenesisEpoch, anEpoch, anEpoch.info),
        (anEpoch, anEpoch, anEpoch.info),
        (anEpoch.copy(lastBlockCommits = someLastBlockCommits), anEpoch, anEpoch.info),
      ).forEvery { (latestCompletedEpoch, latestEpoch, expectedEpochInfoInState) =>
        val epochStore = mock[EpochStore[IgnoringUnitTestEnv]]
        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpoch
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() => latestEpoch)
        when(epochStore.loadEpochProgress(latestEpoch.info)).thenReturn(() =>
          EpochStore.EpochInProgress(Seq.empty, Seq.empty)
        )
        when(
          epochStore.loadCompleteBlocks(
            EpochNumber(
              latestCompletedEpoch.info.number - RetransmissionsManager.HowManyEpochsToKeep + 1
            ),
            EpochNumber(latestCompletedEpoch.info.number),
          )
        )
          .thenReturn(() => Seq.empty)
        val preIssConsensusModule = createPreIssConsensusModule(epochStore)
        val (epochState, lastCompletedEpochRestored, previousEpochsCommitCerts) =
          preIssConsensusModule.restoreEpochStateFromDB()

        verify(epochStore).latestEpoch(includeInProgress = true)
        verify(epochStore).latestEpoch(includeInProgress = false)
        verify(epochStore).loadEpochProgress(latestEpoch.info)

        lastCompletedEpochRestored shouldBe latestCompletedEpoch
        previousEpochsCommitCerts shouldBe empty
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

    "correctly load commit certificates from previously completed epochs" in {
      val completedBlocks =
        createCompletedBlocks(EpochNumber(3), numberOfBlocks = 3) ++
          createCompletedBlocks(EpochNumber(4), numberOfBlocks = 4) ++
          createCompletedBlocks(EpochNumber(5), numberOfBlocks = 3) ++
          createCompletedBlocks(EpochNumber(6), numberOfBlocks = 5) ++
          createCompletedBlocks(EpochNumber(7), numberOfBlocks = 2)

      val epochStore = mock[EpochStore[IgnoringUnitTestEnv]]
      when(
        epochStore.loadCompleteBlocks(
          EpochNumber(3),
          EpochNumber(7),
        )
      ).thenReturn(() => completedBlocks)

      val result =
        PreIssConsensusModule.loadPreviousEpochCommitCertificates(epochStore)(EpochNumber(7), 5)

      result.keySet should contain theSameElementsAs Set(
        EpochNumber(3),
        EpochNumber(4),
        EpochNumber(5),
        EpochNumber(6),
        EpochNumber(7),
      )

      result(EpochNumber(3)) should have size 3
      result(EpochNumber(4)) should have size 4
      result(EpochNumber(5)) should have size 3
      result(EpochNumber(6)) should have size 5
      result(EpochNumber(7)) should have size 2
    }
  }

  private def createPreIssConsensusModule(
      epochStore: EpochStore[IgnoringUnitTestEnv]
  ): PreIssConsensusModule[IgnoringUnitTestEnv] = {
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    implicit val config: BftBlockOrdererConfig = BftBlockOrdererConfig()

    val orderingTopology = OrderingTopology.forTesting(Set(myId))
    new PreIssConsensusModule[IgnoringUnitTestEnv](
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
      new SegmentModuleRefFactory[IgnoringUnitTestEnv] {
        override def apply(
            context: IgnoringUnitTestContext[Consensus.Message[IgnoringUnitTestEnv]],
            epoch: EpochState.Epoch,
            cryptoProvider: CryptoProvider[IgnoringUnitTestEnv],
            latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
            epochInProgress: EpochInProgress,
        )(
            segmentState: SegmentState,
            metricsAccumulator: EpochMetricsAccumulator,
        ): IgnoringSegmentModuleRef[ConsensusSegment.Message] =
          new IgnoringSegmentModuleRef(latestCompletedEpochLastCommits)
      },
      new Random(4),
      new ConsensusModuleDependencies[IgnoringUnitTestEnv](
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
