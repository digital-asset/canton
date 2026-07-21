// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfSynchronizer, DecompressionPolicy}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.deserializeSignedSubmissionRequest
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreReader
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.BlocksRecoveredFromConsensusMessages.LoadPoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.{
  BlocksRecoveredFromConsensusMessages,
  DefaultRequestInspector,
  PreviousStoredBlock,
  RequestInspector,
  StartupState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModuleMetrics.emitRequestsOrderingStats
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.{
  OutputBlockMetadata,
  OutputEpochMetadata,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  LeaderSelectionInitializer,
  LeaderSelectionPolicy,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.snapshot.SequencerSnapshotAdditionalInfoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PartitionManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.NewEpochMembership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.Admin.GetOrderingTopologyResponse
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.SequencerSnapshotMessage.{
  AdditionalInfo,
  AdditionalInfoRetrievalError,
  GetAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.{
  AddMessageChunkFromRestart,
  AsyncException,
  BlockDataFetched,
  BlockDataStored,
  BlockOrdered,
  Message,
  MetadataStoredForNewEpoch,
  NoTopologyAvailable,
  ProcessNewEpochTopologyMessagesIfPossible,
  SequencerSnapshotMessage,
  Start,
  TopologyFetched,
  UpdateLeaderSelection,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  CancellableEvent,
  Env,
  ModuleRef,
  PureFun,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{MaxBytesToDecompress, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.{Span, Tracer}

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.chaining.scalaUtilChainingOps
import scala.util.{Failure, Success}

/** A module responsible for calculating the [[time.BftTime]], querying the topology at epoch ends
  * (if needed), and sending blocks to the sequencer runtime (via the block subscription). It
  * leverages topology ticks that are needed for epochs that could change the topology to make sure
  * we can then query the topology client at the end of an epoch. An epoch potentially changes a
  * topology if sequencer-addressed submissions have been ordered during the epoch, or if the
  * previous epoch had pending topology changes.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class OutputModule[E <: Env[E]](
    startupState: StartupState[E],
    orderingTopologyProvider: OrderingTopologyProvider[E],
    leaderSelectionInitializer: LeaderSelectionInitializer[E],
    store: OutputMetadataStore[E],
    epochStoreReader: EpochStoreReader[E],
    blockSubscription: BlockSubscription,
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    requestInspector: RequestInspector = DefaultRequestInspector, // For testing
    epochChecker: EpochChecker = EpochChecker.DefaultEpochChecker, // For testing
    // Passed from BftBlockOrderer to allow a near-0 latency `GetTime` implementation
    private[bftordering] val previousStoredBlock: PreviousStoredBlock = new PreviousStoredBlock,
    partitionCreator: Option[(PartitionManager.PartitionCreator[E])] = None,
)(implicit
    override val config: BftBlockOrdererConfig,
    synchronizerProtocolVersion: ProtocolVersion,
    mc: MetricsContext,
    tracer: Tracer,
) extends Output[E]
    with HasDelayedInit[Message[E]] {

  private val thisNode = startupState.thisNode

  private val lastAcknowledgedBlockNumber =
    if (startupState.initialHeightToProvide == BlockNumber.First) None
    else Some(BlockNumber(startupState.initialHeightToProvide - 1))

  // We use a Peano queue to ensure that we can process blocks in order and deterministically produce the correct
  //  BFT time, even if they arrive from Consensus, and/or we finish retrieving their data from availability,
  //  out of order.
  //  There is a further, distinct Peano queue, part of the block subscription, whose job instead is to ensure
  //  that blocks are received in order by the sequencer runtime.
  private val maybeCompletedBlocksProcessingPeanoQueue =
    new SingleUseCell[PeanoQueue[BlockNumber, CompleteBlockData]]
  private def completedBlocksPeanoQueue: PeanoQueue[BlockNumber, CompleteBlockData] =
    maybeCompletedBlocksProcessingPeanoQueue.getOrElse(
      throw new IllegalStateException(
        "Completed block processing Peano queue not initialized: Start message not received"
      )
    )

  // TODO(#24737) consider removing
  // We sequence NewEpochTopology messages because state transfer can process blocks from multiple epochs
  //  resulting in fetching multiple topologies concurrently.
  @VisibleForTesting
  private[output] val maybeNewEpochTopologyMessagePeanoQueue =
    new SingleUseCell[PeanoQueue[EpochNumber, NewEpochMembership[E]]]
  private def newEpochTopologyMessagePeanoQueue: PeanoQueue[EpochNumber, NewEpochMembership[E]] =
    maybeNewEpochTopologyMessagePeanoQueue.getOrElse(
      throw new IllegalStateException(
        "NewEpochTopology message Peano queue not initialized: no new topologies were being fetched"
      )
    )

  startupState.previousBftTimeForOnboarding.foreach { time =>
    previousStoredBlock.update(
      BlockNumber(startupState.initialHeightToProvide - 1),
      time,
    )
  }

  private var currentEpochNumber: EpochNumber =
    startupState.initialEpochWeHaveLeaderSelectionStateFor
  private var currentMembership: Membership = startupState.initialMembership
  private var currentEpochCryptoProvider: CryptoProvider[E] = startupState.initialCryptoProvider
  @VisibleForTesting
  private[output] var currentEpochCouldAlterOrderingTopology =
    startupState.onboardingEpochCouldAlterOrderingTopology

  // Storing metadata is idempotent, but we try to avoid unnecessary writes
  @VisibleForTesting
  private[output] val epochsWithMetadataStoredCache = mutable.Set[EpochNumber]()

  private val snapshotAdditionalInfoProvider =
    new SequencerSnapshotAdditionalInfoProvider[E](
      store,
      epochStoreReader,
      orderingTopologyProvider,
      loggerFactory,
    )

  private val blocksBeingFetched = mutable.Map[BlockNumber, Instant]()

  // Used to ensure ordered blocks from an epoch are processed only after the transition to that epoch
  //  has completed, so that epoch-related transient state in this module, which is updated
  //  during the processing of ordered blocks, remains consistent.
  //  It is initialized as soon as the first ordered block is processed, and it is never `None` after that.
  //  It is initialized lazily because the output module currently doesn't know the start epoch in case of
  //  onboarding.
  private var processingFetchedBlocksInEpoch: Option[EpochNumber] = None

  @VisibleForTesting
  private[output] var leaderSelectionPolicy = startupState.initialLeaderSelectionPolicy

  private val blockSpanMap: mutable.Map[BlockNumber, (Span, TraceContext)] = mutable.Map()

  @VisibleForTesting
  private[output] val blocksRecoveredFromConsensus =
    new BlocksRecoveredFromConsensusMessages[E](
      epochStoreReader,
      config.outputSizeOfChunkOfEpochsToLoadAtStart,
      loggerFactory,
    )

  private var backPressureStartInstant: Option[Instant] = None

  private var backPressureDelayedEvent: Option[CancellableEvent] = None

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def receiveInternal(message: Message[E])(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {

      case Start =>
        logger.info("Output module starting initialization")
        startupState.initialLowerBound.foreach { case (epochNumber, blockNumber) =>
          context
            .blockingAwait(
              store.saveOnboardedNodeLowerBound(epochNumber, blockNumber),
              config.blockingDbReadTimeout,
            )
            .fold(error => abort(error), _ => ())
        }

        startupState.previousBftTimeForOnboarding.foreach { previousBftTime =>
          val boundaryBlockNumber = startupState.initialHeightToProvide - 1
          if (boundaryBlockNumber >= BlockNumber.First) {
            logger.info(
              s"Onboarding: persisting boundary block $boundaryBlockNumber with BFT time $previousBftTime " +
                "to seed BFT-time computation if we crash and restart within the onboarding start epoch"
            )
            context.blockingAwait(
              store.insertBlockIfMissing(
                OutputBlockMetadata(
                  epochNumber =
                    EpochNumber(startupState.initialEpochWeHaveLeaderSelectionStateFor - 1),
                  blockNumber = BlockNumber(boundaryBlockNumber),
                  blockBftTime = previousBftTime,
                )
              ),
              config.blockingDbReadTimeout,
            )
          }
        }

        val lastStoredOutputBlockMetadata =
          context.blockingAwait(
            store.getLastNonSequentialBlockMetadataStored,
            config.blockingDbReadTimeout,
          )
        val lastStoredBlockNumber = lastStoredOutputBlockMetadata.map(_.blockNumber)

        // The durable lower bound is the first block (inclusive) this node ever supports serving, set either by
        //  pruning or, for an onboarded node, when it was onboarded (see `saveOnboardedNodeLowerBound`). We must
        //  never try to recover from a block below it: such blocks were either pruned or, in the onboarding case,
        //  never stored by this node at all (it only ever had blocks from its onboarding height onwards).
        val lowerBound =
          context.blockingAwait(store.getLowerBound(), config.blockingDbReadTimeout)

        // The logic to compute `recoverFromBlockNumber` takes into account the following scenarios:
        //
        // - `lastAcknowledgedBlockNumber` is `None` and `lastStoredOutputBlockMetadata` is also `None`: the node is
        //   an initial node starting from scratch.
        // - `lastAcknowledgedBlockNumber` is `None` and `lastStoredOutputBlockMetadata` is defined or
        //    `lastAcknowledgedBlockNumber` is less than `lastStoredOutputBlockMetadata`: the sequencer
        //   runtime is behind w.r.t. the blocks already processed by the output module.
        // - `lastAcknowledgedBlockNumber` is defined and `lastStoredOutputBlockMetadata` is `None` or
        //   `lastStoredOutputBlockMetadata` is less than `lastAcknowledgedBlockNumber`: data loss has occurred in
        //    the output module or the sequencer runtime is interested in later blocks w.r.t. the blocks already
        //    processed by the output module; barring data loss, this should not happen, as we first complete
        //    processing and only then provide blocks to the sequencer runtime, so we expect the sequencer
        //    runtime to be behind or aligned with the output module. However, this case is supported defensively
        //    and to avoid adding `BftBlockOrderer` API restrictions on the subscriber.
        //
        // Note that, in the common case of the sequencer runtime being behind, to keep things simple we still go
        //  through the normal processing flow and just re-process some already processed blocks, leveraging idempotent
        //  storage and deterministic processing of this module.
        //
        // Furthermore, we also load and reprocess one more block before the first one that has to be recovered,
        //  i.e. the last either stored or acknowledged block, to restore the correct volatile state of this module,
        //  thus ensuring that:
        //
        // - We produce the correct BFT time and store the correct last topology timestamp for the unprocessed blocks.
        // - If the system halted after the last block in an epoch was ordered, its output metadata stored, and it was
        //   also sent to the sequencer, but before the consensus module processed the topology for the
        //   new epoch, a topology is sent to consensus, unblocking it.
        //
        // Another reason that we may need to recover is that the leader selection is only snapshotting the state at
        //  epoch boundaries. As such we might need to recover from the start of the epoch.
        @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
        val recoverFromBlockNumberThatCouldBeInMiddleOfEpoch =
          NonEmpty(
            Seq,
            lastAcknowledgedBlockNumber.getOrElse(BlockNumber.First),
            lastStoredBlockNumber.getOrElse(BlockNumber.First),
            leaderSelectionPolicy.firstBlockWeNeedToAdd.getOrElse(
              // We use Max since None means we don't need to recover
              BlockNumber.Max
            ),
          ).min

        val (recoverFromBlockNumber, startingEpochNumberO) = {
          val firstBlockO = for {
            blockMetadata <- context.blockingAwait(
              store.getBlock(recoverFromBlockNumberThatCouldBeInMiddleOfEpoch),
              config.blockingDbReadTimeout,
            )
            startEpochNumber = blockMetadata.epochNumber
            epochInfo <- context.blockingAwait(
              epochStoreReader.loadEpochInfo(startEpochNumber),
              config.blockingDbReadTimeout,
            )
            startBlockNumber = epochInfo.startBlockNumber
          } yield {
            logger.info(
              s"Output module bootstrap wanted to recover from block $recoverFromBlockNumberThatCouldBeInMiddleOfEpoch " +
                s"= min(ack: $lastAcknowledgedBlockNumber, stored: $lastStoredBlockNumber, leader: ${leaderSelectionPolicy.firstBlockWeNeedToAdd}) " +
                s"which is in epoch $startEpochNumber; " +
                s"we adjust to first block of that epoch which is $startBlockNumber}"
            )
            (startBlockNumber, Some(startEpochNumber))
          }

          firstBlockO.getOrElse {
            lowerBound
              .filter(_.blockNumber > recoverFromBlockNumberThatCouldBeInMiddleOfEpoch)
              .map { lb =>
                logger.info(
                  s"Output module bootstrap wanted to recover from block $recoverFromBlockNumberThatCouldBeInMiddleOfEpoch " +
                    s"= min(ack: $lastAcknowledgedBlockNumber, stored: $lastStoredBlockNumber, leader: ${leaderSelectionPolicy.firstBlockWeNeedToAdd}) " +
                    s"which is not stored; recovering instead from the lower bound block ${lb.blockNumber} in epoch ${lb.epochNumber}"
                )
                lb.blockNumber -> Some(lb.epochNumber)
              }
              .getOrElse(recoverFromBlockNumberThatCouldBeInMiddleOfEpoch -> None)
          }
        }

        logger.info(
          s"Output module bootstrap: last acknowledged block number = $lastAcknowledgedBlockNumber, " +
            s"last stored block number = $lastStoredBlockNumber => recover from block number = $recoverFromBlockNumber, " +
            s"epoch could alter ordering topology = $currentEpochCouldAlterOrderingTopology"
        )

        // If we are onboarding, rather than an initial node starting or restarting, there will be no actual blocks
        //  stored and the genesis will be returned, but we`ll have a truncated log and we`ll need to start from the
        //  initial height, rather than from the genesis height, which will be set correctly by the sequencer
        //  runtime as the first block height that we are expected to serve.
        val firstBlockToProcess =
          if (startupState.previousBftTimeForOnboarding.isDefined) {
            val initialHeight = startupState.initialHeightToProvide
            logger.info(
              s"Output module bootstrap: onboarding, providing blocks from initial height $initialHeight"
            )
            initialHeight
          } else {
            logger.info(
              s"Output module bootstrap: [re-]starting, providing blocks from $recoverFromBlockNumber"
            )
            recoverFromBlockNumber
          }
        // We skip the "is it empty" label for the startup figure of blocks ordered
        metrics.global.blocksOrdered.mark(Math.max(0L, firstBlockToProcess - 1))
        maybeCompletedBlocksProcessingPeanoQueue
          .putIfAbsent(new PeanoQueue(firstBlockToProcess)(abort))
          .foreach(_ => abort("Completed block processing Peano Queue has already been set"))

        if (startupState.previousBftTimeForOnboarding.isEmpty) {
          logger.info(
            s"Output module bootstrap: [re-]starting, [re-]processing blocks from $recoverFromBlockNumber"
          )
          val startEpochNumber =
            startingEpochNumberO.getOrElse(EpochNumber.First)

          val targetEpochToLoadO =
            context.blockingAwait(
              epochStoreReader.lastEpochWithCompletedBlock(startEpochNumber),
              config.blockingDbReadTimeout,
            )
          logger.info(
            "Output module bootstrap: " + {
              targetEpochToLoadO match {
                case Some(value) => s"[re-]processing blocks up and until epoch $value"
                case None => "no previous completed block to reprocess"
              }
            }
          )
          val orderedBlocksToProcess =
            context.blockingAwait(
              epochStoreReader.loadOrderedBlocks(
                startEpochNumber,
                config.outputSizeOfChunkOfEpochsToLoadAtStart,
              ),
              config.blockingDbReadTimeout,
            )
          logger.info(s"Output module bootstrap: [re-]starting from epoch $startEpochNumber")
          blocksRecoveredFromConsensus.addMessages(orderedBlocksToProcess)

          targetEpochToLoadO.foreach { targetEpochToLoad =>
            val limit = config.outputSizeOfChunkOfEpochsToLoadAtStart
            // we pick the next load point to be the halfway point, so when should have already loaded the next chunk
            // when output module finish processing the chunk. But we will only have at most 1.5 chunks loaded at any
            // given time
            val nextWhenToLoad = EpochNumber(startEpochNumber + limit / 2)
            val nextWhereToLoadFrom = EpochNumber(startEpochNumber + limit)
            if (nextWhereToLoadFrom <= targetEpochToLoad) {
              // we only set target if the next load would be before, otherwise all the blocks to be loaded can fit in a
              // single chunk and there's no need for further loading
              blocksRecoveredFromConsensus.setTargetEpoch(
                target = targetEpochToLoad,
                nextWhenToLoad = nextWhenToLoad,
                nextWhereToLoadFrom = nextWhereToLoadFrom,
              )
            }
          }

          // Rehydrate the transient local state containing the previous stored block information (if any)
          //  to ensure that the BFT time is computed correctly even when restarting blocks with
          //  adjusted BFT time.
          context
            .blockingAwait(
              store.getBlock(BlockNumber(recoverFromBlockNumber - 1)),
              config.blockingDbReadTimeout,
            )
            .foreach { previousBlock =>
              previousStoredBlock.update(
                previousBlock.blockNumber,
                previousBlock.blockBftTime,
              )
            }

          // The previous block might have been pruned (but in that case we do have the current block we try to recover
          // from). It is okay to set initial even if we set the previous, as long as we set one of them (or we are in
          // genesis).
          context
            .blockingAwait(
              store.getBlock(BlockNumber(recoverFromBlockNumber)),
              config.blockingDbReadTimeout,
            )
            .foreach { initialBlock =>
              previousStoredBlock.setInitial(
                initialBlock.blockNumber,
                initialBlock.blockBftTime,
              )
            }
          val epochMetadata =
            context.blockingAwait(
              store.getEpoch(startEpochNumber),
              config.blockingDbReadTimeout,
            )
          // If an epoch's metadata was not recorded, then it had default values, so we can safely assume that
          //  the epoch could not alter the ordering topology.
          currentEpochCouldAlterOrderingTopology =
            epochMetadata.exists(_.couldAlterOrderingTopology)
          if (epochMetadata.isDefined)
            setEpochMetadataStoredCache(startEpochNumber)

          val initialEpochWeHaveLeaderSelectionStateFor =
            startupState.initialEpochWeHaveLeaderSelectionStateFor
          if (startEpochNumber < initialEpochWeHaveLeaderSelectionStateFor) {
            logger.info(
              s"Output module bootstrap: detected start epoch $startEpochNumber, which is before " +
                s"the epoch we have leader selection state for, i.e. $initialEpochWeHaveLeaderSelectionStateFor: " +
                "loading epoch info"
            )
            for {
              epochInfo <- context.blockingAwait(
                epochStoreReader.loadEpochInfo(startEpochNumber),
                config.blockingDbReadTimeout,
              )
              bootstrapTopologyActivationTime = epochInfo.topologyActivationTime
              _ = logger.info(
                s"Output module bootstrap: querying restart topology at $bootstrapTopologyActivationTime for epoch $startEpochNumber " +
                  s"(could alter ordering topology: $currentEpochCouldAlterOrderingTopology)"
              )
              (orderingTopology, cryptoProvider) <- context.blockingAwait(
                orderingTopologyProvider.getOrderingTopologyAt(
                  activationTime = Some(bootstrapTopologyActivationTime),
                  // Don't check for pending changes if we are restarting from the first epoch,
                  //  since we know there can't be any, and `awaitMaxTimestamp` can be get stuck
                  //  for the first epoch's activation time, since we haven't ticked it
                  //  (being the first epoch).
                  checkPendingChanges = startEpochNumber > EpochNumber.First,
                ),
                config.blockingDbReadTimeout,
              )
              leaderSelectionPolicyState <- context.blockingAwait(
                store.getLeaderSelectionPolicyState(startEpochNumber),
                config.blockingDbReadTimeout,
              )
            } {
              leaderSelectionPolicy = leaderSelectionInitializer
                .leaderSelectionPolicy(leaderSelectionPolicyState, orderingTopology)
              currentEpochNumber = startEpochNumber
              currentMembership = Membership(
                thisNode,
                orderingTopology,
                leaderSelectionPolicy.getLeaders(orderingTopology, startEpochNumber),
                leaderSelectionPolicy.getBlacklistedNodes(orderingTopology, startEpochNumber),
              )
              metrics.topology.update(currentMembership)
              currentEpochCryptoProvider = cryptoProvider
              currentEpochCouldAlterOrderingTopology =
                orderingTopology.areTherePendingCantonTopologyChanges.exists(identity)
              logger.info(
                s"Output module bootstrap is reading blocks from an older epoch $startEpochNumber " +
                  s"we fetched topology info from $bootstrapTopologyActivationTime " +
                  s"(could alter ordering topology: $currentEpochCouldAlterOrderingTopology)" +
                  s"and got the following leader selection state $leaderSelectionPolicyState"
              )
            }
          }

          blocksRecoveredFromConsensus.releaseMessagesForEpoch(startEpochNumber)
        }

        scheduleBackpressureCheck(context)
        initCompleted(receiveInternal)
        logger.info("Output module initialization complete, ready to process messages")

      case _ =>
        ifInitCompleted(message) {
          case Start =>
            logger.info(
              "Output module received Start message, but initialization is already complete, ignoring"
            )

          case message: Output.Admin =>
            handleAdminMessage(message)

          case ProcessNewEpochTopologyMessagesIfPossible =>
            scheduleBackpressureCheck(context)
            val isSequencerCoreSlow = blockSubscription.isSequencerCoreSlow
            val backpressureBufferSize = blockSubscription.bufferSize
            logger.info(
              "Checking if sequencer core is still slow or if we can process new epoch topology messages " +
                s"(backPressureStartInstant = $backPressureStartInstant, " +
                s"from block subscription: isSequencerCoreSlow = $isSequencerCoreSlow, " +
                s"bufferSize = $backpressureBufferSize)"
            )
            processNewEpochTopologyMessagesIfPossible()

          // From local consensus
          case BlockOrdered(
                orderedBlockForOutput @ OrderedBlockForOutput(
                  orderedBlock,
                  _,
                  _,
                  _,
                  mode,
                )
              ) =>
            if (leaderSelectionPolicy.currentEpoch.exists(_ < orderedBlock.metadata.epochNumber)) {
              // Leader Selection wants us to process epochs in order and this block is from a future one, so we delay it
              blocksRecoveredFromConsensus.addMessages(Seq(orderedBlockForOutput))
            } else {
              val blockNumber = orderedBlock.metadata.blockNumber
              val newTraceContext: TraceContext = if (orderedBlock.batchRefs.nonEmpty) {
                val (span, tc) = startSpan(s"BftOrderer.Output")
                blockSpanMap
                  .put(blockNumber, (span.setAttribute("block.number", blockNumber), tc))
                  .discard
                tc
              } else traceContext

              logger.debug(
                s"Output received from local consensus ordered block (mode = $mode) with batch IDs ${orderedBlock.batchRefs
                    .map(_.batchId)}"
              )
              if (completedBlocksPeanoQueue.alreadyInserted(blockNumber)) {
                // This can happen if we start catching up in the middle of an epoch, as state transfer has epoch granularity.
                logger.debug(s"Skipping block $blockNumber as it's been provided already")
              } else if (!blocksBeingFetched.contains(blockNumber)) {
                leaderSelectionPolicy.addBlock(
                  orderedBlockForOutput.orderedBlock.metadata.epochNumber,
                  blockNumber,
                  orderedBlockForOutput.viewNumber,
                )
                // Block batches will be fetched by the availability module either from the local store or,
                //  if unavailable, from remote nodes.
                //  We need to fetch the batches to provide requests, and their BFT sequencing time,
                //  to the sequencer runtime, but this also ensures that all batches are stored locally
                //  when the epoch ends, so that we can provide past block data (e.g. to a re-subscription from
                //  the sequencer runtime after a crash) even if the topology changes drastically afterward.
                logger.debug(s"Fetching data for block $blockNumber through local availability")
                availability.asyncSend(
                  Availability.LocalOutputFetch.FetchBlockData(orderedBlockForOutput)
                )(newTraceContext, mc)
                blocksBeingFetched.put(blockNumber, Instant.now()).discard
              } else {
                logger.debug(s"Block $blockNumber is already being fetched")
              }
            }

          // From availability
          case BlockDataFetched(completedBlockData) =>
            val orderedBlock = completedBlockData.orderedBlockForOutput.orderedBlock
            val blockNumber = orderedBlock.metadata.blockNumber
            blocksBeingFetched
              .remove(blockNumber)
              .foreach(emitFetchLatency)
            logger.debug(
              s"Output received completed block; epoch: ${orderedBlock.metadata.epochNumber}, " +
                s"blockID: $blockNumber, batchIDs: ${completedBlockData.batches.map(_._1)}"
            )
            logger.debug(
              s"Inserting block $blockNumber into Peano queue (head=${completedBlocksPeanoQueue.head})"
            )
            completedBlocksPeanoQueue.insert(blockNumber, completedBlockData)
            processFetchedBlocks()

          case BlockDataStored(
                orderedBlockData,
                orderedBlockNumber,
                orderedBlockBftTime,
                epochCouldAlterOrderingTopology,
              ) =>
            // Blocks metadata persistence can complete in any order, so no assumption can be made
            //  on the epoch number in this handler.
            emitRequestsOrderingStats(metrics, orderedBlockData, orderedBlockBftTime)

            val epochNumber =
              orderedBlockData.orderedBlockForOutput.orderedBlock.metadata.epochNumber

            // If the epoch could alter the ordering topology as a result of this block data,
            //  the epoch metadata was stored before sending this message.
            if (epochCouldAlterOrderingTopology)
              setEpochMetadataStoredCache(epochNumber)

            // Since consensus will wait for the topology before starting the new epoch, and we send it only when all
            //  blocks, including the last block of the previous epoch, are fully fetched, all blocks can always be read
            //  locally, which is essential because all other nodes could (in principle, although this is definitely
            //  not sensible governance) be swapped in the new epoch, so they would have no past data and would thus
            //  be unable to provide it to us.
            // We fetch the topology once the last block is stored as, based on the returned topology, the last block
            //  might need to be updated with pending topology changes.
            if (orderedBlockData.orderedBlockForOutput.isLastInEpoch) {
              fetchNewEpochTopologyIfNeeded(
                orderedBlockData,
                orderedBlockBftTime,
                epochCouldAlterOrderingTopology,
              )

              partitionCreator.foreach { creator =>
                context.pipeToSelf(creator.createPartitionsIfNeeded(epochNumber)) {
                  case Success(partitionsCreated) =>
                    if (partitionsCreated > 0)
                      logger.info(
                        s"Created $partitionsCreated partitions at epoch $epochNumber and block $orderedBlockNumber"
                      )
                    None
                  case Failure(exception) =>
                    logger.error(
                      s"Failed to create partitions at epoch $epochNumber and block $orderedBlockNumber",
                      exception,
                    )
                    None
                }
              }
            }

            // This is just a defensive check, as the block subscription will have the head correctly set to the
            //  initial height and will ignore blocks before that, but we cannot check nor enforce this assumption
            //  in this module due to the generic Peano queue type needed for simulation testing support.
            if (lastAcknowledgedBlockNumber.forall(orderedBlockNumber > _)) {
              val isBlockLastInEpoch = orderedBlockData.orderedBlockForOutput.isLastInEpoch
              // We tick the topology even during state transfer;
              //  it ensures that the newly onboarded sequencer sequences (and stores) the same events
              //  as the other sequencers, which in turn makes counters (and snapshots) consistent,
              //  avoiding possible future problems e.g. with pruning and/or BFT onboarding from multiple
              //  sequencer snapshots.
              val tickTopology = isBlockLastInEpoch && epochCouldAlterOrderingTopology

              val blockTraceContext = blockSpanMap
                .remove(orderedBlockNumber)
                .map { case (span, traceContext) =>
                  span.end()
                  traceContext
                }
                .getOrElse(traceContext)

              // Being able to correlate the trace contexts of submission requests with
              // the block containing them can be useful for troubleshooting issues.
              val traceIdsString =
                orderedBlockData.requestsView.flatMap(_.traceContext.traceId).mkString(",")
              logger.debug(
                s"Block $orderedBlockNumber being output contains requests " +
                  s"with the following trace IDs: [$traceIdsString]"
              )(blockTraceContext)
              logger.debug(
                s"Sending block $orderedBlockNumber " +
                  s"(current epoch = $epochNumber, " +
                  s"block's BFT time = $orderedBlockBftTime, " +
                  s"block size = ${orderedBlockData.requestsView.size}, " +
                  s"is last in epoch = $isBlockLastInEpoch, " +
                  s"could alter sequencing topology = $epochCouldAlterOrderingTopology, " +
                  s"tick topology = $tickTopology) " +
                  "to sequencer subscription"
              )(blockTraceContext)

              val fullyAssembledBlock =
                BlockFormat.Block(
                  orderedBlockNumber,
                  orderedBlockBftTime.toMicros,
                  blockDataToOrderedRequests(orderedBlockData, orderedBlockBftTime),
                  tickTopologyAtMicrosFromEpoch = Option.when(tickTopology)(
                    BftTime
                      .epochEndBftTime(orderedBlockBftTime, orderedBlockData)
                      .toMicros
                  ),
                )

              blockSubscription.receiveBlock(fullyAssembledBlock)(blockTraceContext, mc)
            }

          case UpdateLeaderSelection(topologyFetched) =>
            logger.debug(s"Saving leader selection state for ${topologyFetched.newEpochNumber}")
            pipeToSelf(
              leaderSelectionPolicy
                .saveStateFor(topologyFetched.newEpochNumber, topologyFetched.orderingTopology)
            ) {
              case Failure(exception) =>
                abort(s"Failed to save leader selection state", exception)
              case Success(()) =>
                topologyFetched
            }

          case TopologyFetched(
                newEpochNumber,
                orderingTopology,
                cryptoProvider: CryptoProvider[E],
              ) =>
            logger.debug(s"Fetched topology $orderingTopology for new epoch $newEpochNumber")

            val membership =
              Membership(
                thisNode,
                orderingTopology,
                leaderSelectionPolicy.getLeaders(orderingTopology, newEpochNumber),
                leaderSelectionPolicy.getBlacklistedNodes(orderingTopology, newEpochNumber),
              )

            // We only store metadata for an epoch if it may alter the topology, i.e.,
            //  we never insert `false` and then change it; this avoids updates
            //  and allows leveraging idempotency for easier CFT support.
            if (orderingTopology.areTherePendingCantonTopologyChanges.exists(identity)) {
              val outputEpochMetadata =
                OutputEpochMetadata(newEpochNumber, couldAlterOrderingTopology = true)
              logger.debug(s"Storing $outputEpochMetadata")
              pipeToSelf(store.insertEpochIfMissing(outputEpochMetadata)) {
                case Failure(exception) =>
                  abort(s"Failed to store $outputEpochMetadata", exception)
                case Success(_) =>
                  MetadataStoredForNewEpoch(
                    newEpochNumber,
                    membership,
                    cryptoProvider,
                  )
              }
            } else {
              setupNewEpoch(
                newEpochNumber,
                Some(membership -> cryptoProvider),
                epochMetadataStored = false,
              )
            }

          case MetadataStoredForNewEpoch(
                newEpochNumber,
                membership,
                cryptoProvider: CryptoProvider[E],
              ) =>
            logger.debug(
              s"Metadata for new epoch $newEpochNumber successfully stored, setting up the new epoch"
            )
            setupNewEpoch(
              newEpochNumber,
              Some(membership -> cryptoProvider),
              epochMetadataStored = true,
            )

          case snapshotMessage: SequencerSnapshotMessage =>
            handleSnapshotMessage(snapshotMessage)

          case AddMessageChunkFromRestart(messages) =>
            blocksRecoveredFromConsensus.addMessages(messages)

          case AsyncException(exception) =>
            abort(s"Failed to retrieve new epoch's topology", exception)

          case NoTopologyAvailable =>
            logger.info("No topology snapshot available due to either shutting down or testing")
        }
    }

  private def scheduleBackpressureCheck(
      context: E#ActorContextT[Message[E]]
  )(implicit traceContext: TraceContext): Unit = {
    val interval = OutputModule.SequencerCoreSlowCheckInterval
    backPressureDelayedEvent.foreach { cancellableEvent =>
      if (cancellableEvent.cancel())
        logger.debug(s"Backpressure check was already scheduled, cancelled it")
    }
    logger.info(s"Scheduling backpressure check in $interval")
    backPressureDelayedEvent = Some(
      context
        .delayedEvent(
          interval,
          ProcessNewEpochTopologyMessagesIfPossible,
        )
    )
  }

  private def emitBackpressureMetrics(): Unit = {
    val now = Instant.now()
    locally {
      import metrics.output.*
      backPressureStartInstant.fold {
        currentSequencerCoreBackpressureDelayMillis.updateValue(0L)
      } { startInstant =>
        currentSequencerCoreBackpressureDelayMillis.updateValue(
          Duration.between(startInstant, now).toMillis
        )
      }
    }
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.output.Backpressure,
      startInstant = backPressureStartInstant,
      endInstant = now,
    )
  }

  private def handleAdminMessage(message: Output.Admin): Unit =
    message match {

      case Output.Admin.GetOrderingTopology(callback) =>
        callback(
          GetOrderingTopologyResponse(
            currentEpochNumber,
            currentMembership.orderingTopology.nodes,
            currentMembership.leaders,
            currentMembership.blacklistedNodes,
            currentMembership.orderingTopology.sequencingParameters,
          )
        )

      case Output.Admin.SetPerformanceMetricsEnabled(enabled) =>
        metrics.performance.enabled = enabled
    }

  private def processFetchedBlocks()(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val orderedBlocks =
      completedBlocksPeanoQueue.pollAvailable {
        case Some(completeBlockData) =>
          val blockEpochNumber =
            completeBlockData.orderedBlockForOutput.orderedBlock.metadata.epochNumber
          val processingEpoch =
            processingFetchedBlocksInEpoch match {
              case None =>
                processingFetchedBlocksInEpoch = Some(blockEpochNumber)
                blockEpochNumber
              case Some(epochNumber) =>
                epochNumber
            }
          blockEpochNumber <= processingEpoch
        case _ => false
      }

    logger.debug(
      s"Polled blocks from Peano queue ${orderedBlocks.map(_.orderedBlockForOutput.orderedBlock.metadata)}"
    )

    // This is the main processing loop where we process blocks in order,
    //  so it is generally safe to use the module's mutable state in it.
    orderedBlocks.foreach { orderedBlockData =>
      val orderedBlock = orderedBlockData.orderedBlockForOutput.orderedBlock
      val orderedBlockNumber = orderedBlock.metadata.blockNumber
      val orderedBlockEpochNumber = orderedBlock.metadata.epochNumber
      val orderedBlockBftTime = previousStoredBlock.computeBlockBftTime(orderedBlock)

      if (
        !currentEpochCouldAlterOrderingTopology && potentiallyAltersSequencersTopology(
          orderedBlockData
        )
      ) {
        logger.debug(
          s"Found potential changes of the sequencing topology in ordered block $orderedBlockNumber " +
            s"in epoch $orderedBlockEpochNumber"
        )
        currentEpochCouldAlterOrderingTopology = true
      }

      val outputBlockMetadata =
        OutputBlockMetadata(
          orderedBlockEpochNumber,
          orderedBlockNumber,
          orderedBlockBftTime,
        )

      logger.debug(
        s"Assigned BFT time $orderedBlockBftTime to block $orderedBlockNumber " +
          s"in epoch $orderedBlockEpochNumber, previous block was $previousStoredBlock"
      )

      previousStoredBlock.update(orderedBlockNumber, orderedBlockBftTime)

      // Capture and pass the relevant mutable state along to prevent
      //  that message handlers running after async calls race on it.
      val couldAlterOrderingTopology = currentEpochCouldAlterOrderingTopology

      // We start storing the metadata for fully-fetched blocks in order, but the completion of
      //  the storage operations can happen in any order; this allows to optimize performance
      //  and the Peano queue in `BlockSubscription` will ensure they are emitted them in the
      //  correct order.
      //  However, we cannot assume any ordering in the `BlockDataStored` handler below, so
      //  we must pass the value of any relevant mutable state along with the message.
      logger.debug(s"Storing $outputBlockMetadata")
      pipeToSelf(
        // We only store metadata for an epoch if it may alter the topology, i.e.,
        //  we never insert `false` and then change it; this avoids updates
        //  and allows leveraging idempotency for easier CFT support.
        if (
          couldAlterOrderingTopology && !epochsWithMetadataStoredCache
            .contains(orderedBlockEpochNumber)
        ) {
          val outputEpochMetadata =
            OutputEpochMetadata(orderedBlockEpochNumber, couldAlterOrderingTopology = true)
          logger.debug(s"Storing $outputEpochMetadata")
          // We store the epoch metadata before the block metadata to ensure that, in case of a crash
          //  and restart from the last stored block, we don't lose the information that the block
          //  could alter the topology.
          //  If the order was reversed, this information loss could happen in the case of a crash
          //  after the block metadata is stored but before the epoch metadata is stored.
          context.flatMapFuture(
            store.insertEpochIfMissing(outputEpochMetadata),
            PureFun.Const(store.insertBlockIfMissing(outputBlockMetadata)),
            orderingStage = Some("output-insert-block-and-epoch-metadata"),
          )
        } else {
          store.insertBlockIfMissing(outputBlockMetadata)
        }
      ) {
        case Failure(exception) =>
          abort(s"Failed to add block $orderedBlockNumber", exception)
        case Success(_) =>
          BlockDataStored(
            orderedBlockData,
            orderedBlockNumber,
            orderedBlockBftTime,
            couldAlterOrderingTopology,
          )
      }
    }
  }

  private def handleSnapshotMessage(
      message: SequencerSnapshotMessage
  )(implicit context: E#ActorContextT[Message[E]], traceContext: TraceContext): Unit =
    message match {
      case GetAdditionalInfo(timestamp, from) =>
        snapshotAdditionalInfoProvider.provide(
          timestamp,
          currentMembership.orderingTopology,
          leaderSelectionPolicy,
          from,
        )

      case AdditionalInfo(requester, info) =>
        requester.asyncSend(SequencerNode.SnapshotMessage.AdditionalInfo(info.toProto30))

      case AdditionalInfoRetrievalError(requester, errorMessage) =>
        requester.asyncSend(
          SequencerNode.SnapshotMessage.AdditionalInfoRetrievalError(errorMessage)
        )
    }

  private def potentiallyAltersSequencersTopology(
      orderedBlockData: CompleteBlockData
  ): Boolean = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.output.Inspection,
      () =>
        orderedBlockData.requestsView.zipWithIndex.toSeq.findLast {
          case (tracedOrderingRequest @ Traced(orderingRequest), idx) =>
            requestInspector.isRequestToAllMembersOfSynchronizer(
              orderedBlockData.orderedBlockForOutput.orderedBlock.metadata,
              idx,
              orderingRequest,
              currentMembership.orderingTopology.maxBytesToDecompress,
              logger,
              tracedOrderingRequest.traceContext,
            )
        }.isDefined,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  private def fetchNewEpochTopologyIfNeeded(
      lastBlockInEpoch: CompleteBlockData,
      epochLastBlockBftTime: CantonTimestamp,
      epochCouldAlterOrderingTopology: Boolean,
  )(implicit context: E#ActorContextT[Message[E]], traceContext: TraceContext): Unit = {
    val lastBlockForOutput = lastBlockInEpoch.orderedBlockForOutput
    val blockMetadata = lastBlockForOutput.orderedBlock.metadata

    if (!lastBlockForOutput.isLastInEpoch)
      abort(s"Block ${blockMetadata.blockNumber} not last in epoch ${blockMetadata.epochNumber}")

    val completedEpochNumber = blockMetadata.epochNumber
    val lastCompletedBlockNumber = blockMetadata.blockNumber
    logger.debug(
      s"Last ordered block $lastCompletedBlockNumber in epoch $completedEpochNumber fully processed"
    )

    val epochEndBftTime = BftTime.epochEndBftTime(epochLastBlockBftTime, lastBlockInEpoch)

    val lastBlockMode = lastBlockForOutput.orderingMode
    val newEpochNumber = EpochNumber(completedEpochNumber + 1)

    maybeNewEpochTopologyMessagePeanoQueue
      .putIfAbsent(new PeanoQueue(newEpochNumber)(abort))
      .discard

    if (epochCouldAlterOrderingTopology) {
      logger.debug(
        s"Completed epoch $completedEpochNumber that could alter sequencing topology: " +
          s"last block mode = $lastBlockMode; querying for an updated Canton topology effective after ticking " +
          s"the topology processor with epoch's last sequencing time $epochEndBftTime)"
      )
      // Once a topology processor observes (processes) a sequenced request with sequencing time `t`,
      //  which is considered the "end-of-epoch" sequencing time, the topology processor can safely serve
      //  topology snapshots, at a minimum when the delay is 0, up to effective time `t.immediateSuccessor`.
      //  We want the ordering layer to observe topology changes timely, so we can safely
      //  query for a topology snapshot at effective time `t.immediateSuccessor`.
      //  When the topology change delay is 0, this allows running a subsequent epoch
      //  using an ordering topology that includes the potential effects a topology transaction
      //  that was successfully sequenced as the last request in the preceding epoch
      //  (and successfully processed and applied by the topology processor).
      pipeToSelf(
        orderingTopologyProvider.getOrderingTopologyAt(
          activationTime = Some(TopologyActivationTime(epochEndBftTime.immediateSuccessor)),
          checkPendingChanges = true,
        ),
        metrics.topology.queryLatency,
      ) {
        case Failure(exception) => AsyncException(exception)
        case Success(Some((orderingTopology, cryptoProvider))) =>
          UpdateLeaderSelection(
            TopologyFetched(
              newEpochNumber,
              orderingTopology,
              cryptoProvider,
            )
          )
        case Success(None) =>
          NoTopologyAvailable
      }
    } else {
      logger.debug(s"Completed epoch $completedEpochNumber that did not change the topology")
      pipeToSelfOpt(
        leaderSelectionPolicy.saveStateFor(newEpochNumber, currentMembership.orderingTopology)
      ) {
        case Failure(exception) =>
          abort(s"Failed to save leader selection state", exception)
        case Success(()) =>
          setupNewEpoch(
            newEpochNumber,
            newMembershipAndCryptoProvider = None,
            epochMetadataStored = false,
          )
          None
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def setupNewEpoch(
      newEpochNumber: EpochNumber,
      newMembershipAndCryptoProvider: Option[(Membership, CryptoProvider[E])],
      epochMetadataStored: Boolean,
  )(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val membership =
      newMembershipAndCryptoProvider.fold(currentMembership)(_._1)
    val orderingTopology = membership.orderingTopology
    val newEpochLeaders = leaderSelectionPolicy.getLeaders(orderingTopology, newEpochNumber)
    val newEpochBlacklisted =
      leaderSelectionPolicy.getBlacklistedNodes(orderingTopology, newEpochNumber)
    val newMembership = Membership(thisNode, orderingTopology, newEpochLeaders, newEpochBlacklisted)
    val cryptoProvider =
      newMembershipAndCryptoProvider.fold(currentEpochCryptoProvider)(_._2)

    if (epochMetadataStored)
      setEpochMetadataStoredCache(newEpochNumber)
    cleanupEpochMetadataStoredCache(newEpochNumber)

    logger.debug(
      s"Inserting NewEpochTopology message for epoch $newEpochNumber into Peano queue, " +
        s"(head=$newEpochTopologyMessagePeanoQueue)"
    )
    newEpochTopologyMessagePeanoQueue.insert(
      newEpochNumber,
      Consensus.NewEpochMembership(newEpochNumber, newMembership, cryptoProvider),
    )

    processNewEpochTopologyMessagesIfPossible()
  }

  private def processNewEpochTopologyMessagesIfPossible()(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    emitBackpressureMetrics()
    // We check directly the subscriptions state, rather than using a notification mechanism through actor messages,
    //  because the subscription state update is multithreaded and pause/resume messages may be reordered due
    //  to thread scheduling, potentially causing a deadlock.
    //  In addition to reading the state, we also get notified by the subscription when processing may be able to
    //  be resumed via `ProcessNewEpochTopologyMessagesIfPossible` messages; this allows to always and timely
    //  resume ordering.
    val isSequencerCoreSlow = blockSubscription.isSequencerCoreSlow
    val backpressureBufferSize = blockSubscription.bufferSize
    if (
      isSequencerCoreSlow && backpressureBufferSize > OutputModule.BackpressureBufferResumeThreshold
    ) {
      backPressureStartInstant.fold {
        logger.info(
          s"Not processing new epoch topology messages because the sequencer core is slow to consume blocks " +
            s"and the backpressure buffer size is $backpressureBufferSize, " +
            s"which is above the resume threshold of ${OutputModule.BackpressureBufferResumeThreshold}"
        )
        backPressureStartInstant = Some(Instant.now())
      } { startInstant =>
        val duration = Duration.between(startInstant, Instant.now())
        logger.info(
          s"The sequencer core is still slow after $duration, not processing new epoch topology messages yet"
        )
      }
    } else {
      if (isSequencerCoreSlow)
        logger.info(
          "The subscription reported that the sequencer core is slow but " +
            "the buffer size is below our resume threshold, processing new epoch topology messages regardless"
        )

      if (backPressureStartInstant.isDefined) {
        logger.info(
          s"The sequencer core has caught up enough, processing new epoch topology messages"
        )
        backPressureStartInstant = None
      }

      // Not using the accessor because this gets called periodically and may not be set
      //  for a period of time after init.
      val newEpochTopologyMessages =
        maybeNewEpochTopologyMessagePeanoQueue.get.fold(Seq.empty[NewEpochMembership[E]])(
          _.pollAvailable()
        )
      logger.debug(
        s"Polled NewEpochTopology messages: $newEpochTopologyMessages from Peano queue"
      )

      logger.info(
        s"Processing ${newEpochTopologyMessages.size} new epoch topology messages"
      )

      newEpochTopologyMessages.foreach { newEpochTopologyMessage =>
        // It is safe to use and change epoch-related mutable state in this block because:
        //  - New epoch messages are processed sequentially and in order.
        //  - Ordered blocks processing, which uses and changes epoch-related mutable state:
        //    - Also happens sequentially and in order.
        //    - Furthermore, only blocks for the current epoch are processed.
        val newEpochNumber = newEpochTopologyMessage.epochNumber
        logger.debug(s"Setting up new epoch $newEpochNumber")
        currentEpochCouldAlterOrderingTopology = false
        processingFetchedBlocksInEpoch = Some(newEpochNumber)
        currentEpochNumber = newEpochNumber
        currentMembership = newEpochTopologyMessage.membership
        metrics.topology.update(currentMembership)
        currentEpochCryptoProvider = newEpochTopologyMessage.cryptoProvider
        val pendingTopologyChanges =
          currentMembership.orderingTopology.areTherePendingCantonTopologyChanges
        logger.debug(
          s"Pending topology changes in new ordering topology = $pendingTopologyChanges"
        )
        currentEpochCouldAlterOrderingTopology = pendingTopologyChanges.exists(identity)

        metrics.topology.validators.updateValue(currentMembership.orderingTopology.nodes.size)
        logger.debug(
          s"Sending topology $currentMembership of a new epoch $newEpochNumber " +
            s"to a consensus behavior (epochLength= ${newEpochTopologyMessage.membership.orderingTopology.epochLength})"
        )

        consensus.asyncSend(newEpochTopologyMessage)
        epochChecker.check(
          thisNode,
          newEpochNumber,
          newEpochTopologyMessage.membership,
        )
        blocksRecoveredFromConsensus.releaseMessagesForEpoch(newEpochNumber)

        processFetchedBlocks()
      }
    }
  }

  private def blockDataToOrderedRequests(
      blockData: CompleteBlockData,
      blockBftTime: CantonTimestamp,
  ): Seq[Traced[OrderedRequest]] =
    blockData.requestsView.zipWithIndex.map {
      case (tracedRequest @ Traced(OrderingRequest(tag, _, body, _)), index) =>
        val timestamp = BftTime.requestBftTime(blockBftTime, index)
        // "You [were supposed to] propose for ordering, you are responsible for the traffic" policy: all
        //  requests in a block are marked, for accounting purposes, as having gone through ordering because of the
        //  block's originally assigned consensus leader ordering node (which is also the disseminator of the batches
        //  in the block).
        Traced(
          OrderedRequest(
            timestamp.toMicros,
            tag,
            body,
            blockData.orderedBlockForOutput.originalLeader,
          )
        )(
          tracedRequest.traceContext
        )
    }.toSeq

  private def setEpochMetadataStoredCache(newEpochNumber: EpochNumber): Unit =
    epochsWithMetadataStoredCache.add(newEpochNumber).discard

  private def cleanupEpochMetadataStoredCache(newEpochNumber: EpochNumber): Unit =
    // Cleanup old epoch bookkeeping state to avoid OOMs; keep the last two epochs to
    //  spare epoch metadata inserts for blocks that finish saving after an epoch switch
    this.epochsWithMetadataStoredCache
      .filterInPlace(_ >= newEpochNumber - 1)
      .discard

  private def emitFetchLatency(start: Instant): Unit = {
    import metrics.performance.orderingStageLatency.*
    emitOrderingStageLatency(
      labels.stage.values.output.Fetch,
      Some(start),
    )
  }
}

object OutputModule {

  final case class StartupState[E <: Env[E]](
      thisNode: BftNodeId,
      initialHeightToProvide: BlockNumber,
      initialEpochWeHaveLeaderSelectionStateFor: EpochNumber,
      previousBftTimeForOnboarding: Option[CantonTimestamp],
      onboardingEpochCouldAlterOrderingTopology: Boolean,
      initialCryptoProvider: CryptoProvider[E],
      initialMembership: Membership,
      initialLowerBound: Option[(EpochNumber, BlockNumber)],
      initialLeaderSelectionPolicy: LeaderSelectionPolicy[E],
  )

  private[bftordering] final class PreviousStoredBlock {

    private val initialBlockAndBftTimeRef =
      new AtomicReference[Option[(BlockNumber, CantonTimestamp)]](None)

    private val blockNumberAndBftTimeRef =
      new AtomicReference[Option[(BlockNumber, CantonTimestamp)]](None)

    private[bftordering] def getInitialBlockNumberAndBftTime
        : Option[(BlockNumber, CantonTimestamp)] =
      initialBlockAndBftTimeRef.get()

    private[bftordering] def getBlockNumberAndBftTime: Option[(BlockNumber, CantonTimestamp)] =
      blockNumberAndBftTimeRef.get()

    override def toString: String =
      blockNumberAndBftTimeRef
        .get()
        .map(b => s"(block number = ${b._1}, BFT time = ${b._2})")
        .getOrElse("undefined")

    def setInitial(blockNumber: BlockNumber, blockBftTime: CantonTimestamp): Unit =
      initialBlockAndBftTimeRef.set(Some(blockNumber -> blockBftTime))

    @VisibleForTesting
    private[output] def update(blockNumber: BlockNumber, blockBftTime: CantonTimestamp): Unit =
      blockNumberAndBftTimeRef.set(Some(blockNumber -> blockBftTime))

    private[OutputModule] def computeBlockBftTime(orderedBlock: OrderedBlock): CantonTimestamp =
      initialBlockAndBftTimeRef
        .get()
        .filter(_._1 == orderedBlock.metadata.blockNumber)
        .map(_._2)
        .getOrElse(
          BftTime.blockBftTime(
            orderedBlock.canonicalCommitSet,
            previousBlockBftTime =
              blockNumberAndBftTimeRef.get().map(_._2).getOrElse(CantonTimestamp.Epoch),
          )
        )
  }

  trait RequestInspector {

    def isRequestToAllMembersOfSynchronizer(
        blockMetadata: BlockMetadata,
        requestNumber: Int,
        request: OrderingRequest,
        maxBytesToDecompress: MaxBytesToDecompress,
        logger: TracedLogger,
        traceContext: TraceContext,
    )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean
  }

  object DefaultRequestInspector extends RequestInspector {

    override def isRequestToAllMembersOfSynchronizer(
        blockMetadata: BlockMetadata,
        requestNumber: Int,
        request: OrderingRequest,
        maxBytesToDecompress: MaxBytesToDecompress,
        logger: TracedLogger,
        traceContext: TraceContext,
    )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean =
      // TODO(#21615) we should avoid a further deserialization downstream
      deserializeSignedSubmissionRequest(
        synchronizerProtocolVersion,
        DecompressionPolicy.forProtocolVersion(synchronizerProtocolVersion, maxBytesToDecompress),
      )(
        request.payload
      ) match {
        case Right(signedSubmissionRequest) =>
          signedSubmissionRequest.content.batch.allRecipients
            .contains(AllMembersOfSynchronizer)
            .tap(result =>
              logger.debug(
                s"BFT ordering request at index $requestNumber in output block $blockMetadata with message ID ${signedSubmissionRequest.content.messageId} is to all members of synchronizer: $result"
              )(traceContext)
            )
        case Left(error) =>
          logger.debug(
            s"Skipping BFT ordering ordering request in output while looking for sequencer events as it failed to deserialize: $error"
          )(traceContext)
          false
      }
  }

  class FixedResultRequestInspector(result: Boolean) extends RequestInspector {

    override def isRequestToAllMembersOfSynchronizer(
        blockMetadata: BlockMetadata,
        requestNumber: Int,
        request: OrderingRequest,
        maxBytesToDecompress: MaxBytesToDecompress,
        logger: TracedLogger,
        traceContext: TraceContext,
    )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean =
      result
  }

  class BlocksRecoveredFromConsensusMessages[E <: Env[E]](
      epochStoreReader: EpochStoreReader[E],
      limit: Int,
      override val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    private val blocksToRelease = mutable.Map.empty[EpochNumber, Seq[OrderedBlockForOutput]]
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var targetEpochO: Option[EpochNumber] = None
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var nextLoadPointO: Option[LoadPoint] = None
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var highestReleaseEpoch: Option[EpochNumber] = None

    def setTargetEpoch(
        target: EpochNumber,
        nextWhenToLoad: EpochNumber,
        nextWhereToLoadFrom: EpochNumber,
    ): Unit = {
      require(targetEpochO.isEmpty)
      require(nextWhenToLoad <= nextWhereToLoadFrom)
      require(nextWhereToLoadFrom <= target)
      targetEpochO = Some(target)
      nextLoadPointO = Some(
        LoadPoint(nextWhenToLoad, nextWhereToLoadFrom)
      )
    }

    def addMessages(messages: Seq[OrderedBlockForOutput])(implicit
        context: E#ActorContextT[Output.Message[E]],
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit = {
      val epochToMessageMap = messages.groupBy(_.orderedBlock.metadata.epochNumber)
      epochToMessageMap.foreach { case (epochNumber, orderedBlocks) =>
        blocksToRelease
          .updateWith(epochNumber) {
            case Some(previousBlocks) => Some(orderedBlocks ++ previousBlocks)
            case None => Some(orderedBlocks)
          }
          .discard
      }
      highestReleaseEpoch.foreach(epochNumber => releaseMessagesForEpoch(epochNumber))
    }

    def releaseMessagesForEpoch(
        epochNumber: EpochNumber
    )(implicit
        context: E#ActorContextT[Output.Message[E]],
        traceContext: TraceContext,
        metricsContext: MetricsContext,
    ): Unit = {
      val orderedBlocksToProcess = blocksToRelease.getOrElse(epochNumber, Seq.empty)
      blocksToRelease.remove(epochNumber).discard
      orderedBlocksToProcess.foreach(orderedBlockForOutput =>
        context.self.asyncSend(BlockOrdered(orderedBlockForOutput))
      )
      highestReleaseEpoch = Some(epochNumber)

      nextLoadPointO.foreach { nextLoadPoint =>
        if (nextLoadPoint.whenToLoad == epochNumber) {
          nextLoadPointO = nextLoadPoint.computeNextLoadPoint(targetEpochO, limit)
          context.pipeToSelf(
            epochStoreReader
              .loadOrderedBlocks(nextLoadPoint.loadFrom, limit)
          ) {
            case Failure(exception) =>
              logger.error("Could not load blocks", exception)
              context.abort(exception)
            case Success(value) =>
              Some(AddMessageChunkFromRestart(value))
          }
        }
      }
    }

    @VisibleForTesting
    private[output] def nextLoadPoint: Option[LoadPoint] = nextLoadPointO
  }
  object BlocksRecoveredFromConsensusMessages {

    private[output] final case class LoadPoint(whenToLoad: EpochNumber, loadFrom: EpochNumber) {

      def computeNextLoadPoint(targetEpochO: Option[EpochNumber], limit: Int): Option[LoadPoint] = {
        val nextWhenLoad = bumpWithLimit(whenToLoad, limit)
        val nextLoadFrom = bumpWithLimit(loadFrom, limit)
        if (targetEpochO.exists(nextLoadFrom <= _)) {
          Some(LoadPoint(nextWhenLoad, nextLoadFrom))
        } else {
          None
        }
      }

      private def bumpWithLimit(epochNumber: EpochNumber, limit: Int): EpochNumber =
        EpochNumber(epochNumber + limit)
    }
  }

  private val SequencerCoreSlowCheckInterval = 10.seconds
  private val BackpressureBufferResumeThreshold = 1_000
}
