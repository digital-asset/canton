// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.RetransmissionsManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.ConsensusMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, ModuleRef}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.FairBoundedQueue
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.util.Random

import EpochState.Epoch

final class PreIssConsensusModule[E <: Env[E]](
    bootstrapTopologyInfo: OrderingTopologyInfo[E],
    epochLength: EpochLength,
    epochStore: EpochStore[E],
    sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
    clock: Clock,
    metrics: BftOrderingMetrics,
    segmentModuleRefFactory: SegmentModuleRefFactory[E],
    random: Random,
    override val dependencies: ConsensusModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    override val config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends Consensus[E]
    with HasDelayedInit[Consensus.Message[E]] {

  override def ready(self: ModuleRef[Consensus.Message[E]]): Unit =
    self.asyncSendNoTrace(Consensus.Init.KickOff)

  override protected def receiveInternal(message: Consensus.Message[E])(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case Consensus.Init.KickOff =>
        loadLatestEpochs()

      case Consensus.Init.LatestEpochsLoaded(latestCompletedEpoch, latestEpoch) =>
        loadEpochInitData(latestCompletedEpoch, latestEpoch)

      case Consensus.Init.EpochInitDataLoaded(
            latestCompletedEpoch,
            latestEpoch,
            epochInProgress,
            completedBlocks,
          ) =>
        val previousEpochsCommitCerts =
          completedBlocks
            .groupBy(_.epochNumber)
            .fmap(_.sortBy(_.blockNumber).map(_.commitCertificate))
        val epochState =
          initialEpochState(
            latestCompletedEpoch.lastBlockCommits,
            latestEpoch,
            epochInProgress,
          )
        val consensus =
          new IssConsensusModule(
            epochLength,
            IssConsensusModule.InitialState(
              bootstrapTopologyInfo,
              epochState,
              latestCompletedEpoch,
              sequencerSnapshotAdditionalInfo,
            ),
            epochStore,
            clock,
            metrics,
            segmentModuleRefFactory,
            new RetransmissionsManager[E](
              bootstrapTopologyInfo.thisNode,
              dependencies.p2pNetworkOut,
              abort,
              previousEpochsCommitCerts,
              metrics,
              clock,
              loggerFactory,
            ),
            random,
            dependencies,
            loggerFactory,
            timeouts,
            futurePbftMessageQueue =
              new FairBoundedQueue[ConsensusMessage.PbftUnverifiedNetworkMessage](
                config.consensusQueueMaxSize,
                config.consensusQueuePerNodeQuota,
                // Drop newest to ensure continuity of messages (and fall back to retransmissions or state transfer later if needed)
                DropStrategy.DropNewest,
              ),
          )()()
        context.become(consensus)

        // This will send all queued messages to the proper Consensus module.
        initCompleted(consensus.receive(_))

      case message =>
        ifInitCompleted(message) { _ =>
          abort(s"${this.getClass.toString} shouldn't receive any messages after init")
        }
    }

  private def loadLatestEpochs()(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    context.pipeToSelf(
      context.futureContext.zipFuture(
        epochStore.latestEpoch(includeInProgress = false),
        epochStore.latestEpoch(includeInProgress = true),
      )
    ) {
      case scala.util.Success(latestCompletedEpoch -> latestEpoch) =>
        Some(Consensus.Init.LatestEpochsLoaded(latestCompletedEpoch, latestEpoch))
      case scala.util.Failure(exception) =>
        abort(s"Failed to load latest epochs from store: ${exception.getMessage}")
    }

  private def loadEpochInitData(
      latestCompletedEpoch: EpochStore.Epoch,
      latestEpoch: EpochStore.Epoch,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    context.pipeToSelf(
      context.futureContext.zipFuture(
        epochStore.loadEpochProgress(latestEpoch.info),
        epochStore.loadCompleteBlocks(
          EpochNumber(
            latestCompletedEpoch.info.number - RetransmissionsManager.HowManyEpochsToKeep + 1
          ),
          latestCompletedEpoch.info.number,
        ),
      )
    ) {
      case scala.util.Success(epochInProgress -> completeBlocks) =>
        Some(
          Consensus.Init
            .EpochInitDataLoaded(latestCompletedEpoch, latestEpoch, epochInProgress, completeBlocks)
        )
      case scala.util.Failure(exception) =>
        abort(s"Failed to load latest epoch init data from store: ${exception.getMessage}")
    }

  @VisibleForTesting
  private[iss] def initialEpochState(
      latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
      latestEpochFromStore: EpochStore.Epoch,
      epochInProgress: EpochStore.EpochInProgress,
  )(implicit mc: MetricsContext, context: E#ActorContextT[Consensus.Message[E]]): EpochState[E] = {
    val epoch = Epoch(
      latestEpochFromStore.info,
      bootstrapTopologyInfo.currentMembership,
      bootstrapTopologyInfo.previousMembership,
    )

    new EpochState(
      epoch,
      clock,
      abort(_)(context, TraceContext.empty),
      metrics,
      segmentModuleRefFactory(
        context,
        epoch,
        bootstrapTopologyInfo.currentCryptoProvider,
        latestCompletedEpochLastCommits,
        epochInProgress,
      ),
      epochInProgress.completedBlocks,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )
  }
}
