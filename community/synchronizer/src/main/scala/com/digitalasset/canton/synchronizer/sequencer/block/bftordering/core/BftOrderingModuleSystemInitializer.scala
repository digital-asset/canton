// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.{
  BftOrderingStores,
  BootstrapTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.{
  CryptoProvider,
  DelegationCryptoProvider,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Bootstrap.BootstrapEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  Bootstrap,
  EpochStore,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  PreIssConsensusModule,
  SegmentModuleRefFactoryImpl,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool.{
  MempoolModule,
  MempoolModuleConfig,
  MempoolState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.RequestInspector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  BlacklistLeaderSelectionPolicyState,
  LeaderSelectionInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.{
  EpochChecker,
  OutputModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.{
  P2PNetworkInModule,
  P2PNetworkOutModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.{
  PartitionManager,
  PruningModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Module.{
  SystemInitializationResult,
  SystemInitializer,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.OrderingModuleSystemInitializer.ModuleFactories
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.{
  AvailabilityModuleDependencies,
  ConsensusModuleDependencies,
  P2PNetworkOutModuleDependencies,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.util.Random

/** A module system initializer for the concrete Canton BFT ordering system.
  */
private[bftordering] class BftOrderingModuleSystemInitializer[
    E <: Env[E],
    P2PNetworkManagerT <: P2PNetworkManager[E, BftOrderingMessage],
](
    node: BftNodeId,
    config: BftBlockOrdererConfig,
    sequencerSubscriptionInitialBlockNumber: BlockNumber,
    stores: BftOrderingStores[E],
    orderingTopologyProvider: OrderingTopologyProvider[E],
    blockSubscription: BlockSubscription,
    sequencerSnapshotAdditionalInfo: Option[SequencerSnapshotAdditionalInfo],
    p2pNetworkOutModuleStateFactory: Membership => P2PNetworkOutModule.State,
    clock: Clock,
    random: Random,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    requestInspector: RequestInspector =
      OutputModule.DefaultRequestInspector, // Only set by simulation tests
    epochChecker: EpochChecker = EpochChecker.DefaultEpochChecker, // Only set by simulation tests
    outputPreviousStoredBlock: OutputModule.PreviousStoredBlock =
      new OutputModule.PreviousStoredBlock,
)(implicit synchronizerProtocolVersion: ProtocolVersion, mc: MetricsContext, tracer: Tracer)
    extends SystemInitializer[
      E,
      P2PNetworkManagerT,
      BftOrderingMessage,
      Mempool.Message,
    ]
    with NamedLogging {

  override def initialize(
      moduleSystem: ModuleSystem[E],
      createP2PNetworkManager: (
          P2PConnectionEventListener,
          ModuleRef[BftOrderingMessage],
      ) => P2PNetworkManagerT,
  )(implicit traceContext: TraceContext): SystemInitializationResult[
    E,
    P2PNetworkManagerT,
    BftOrderingMessage,
    Mempool.Message,
  ] = {
    implicit val c: BftBlockOrdererConfig = config
    val leaderSelectionPolicyFactory = LeaderSelectionInitializer.create[E](
      node,
      synchronizerProtocolVersion,
      stores.outputStore,
      config.initQueryTimeout,
      msg => context => failBootstrap(msg)(context),
      metrics,
      loggerFactory,
    )
    val (initialTopologyEpochNumber, bootstrapTopologyInfo, blacklistLeaderSelectionState) =
      fetchBootstrapTopologyInfo(moduleSystem, leaderSelectionPolicyFactory)

    val thisNodeFirstKnownAt =
      sequencerSnapshotAdditionalInfo.flatMap(_.nodeActiveAt.get(bootstrapTopologyInfo.thisNode))
    val firstBlockNumberInOnboardingEpoch =
      thisNodeFirstKnownAt.flatMap(_.firstBlockNumberInStartEpoch)
    val previousBftTimeForOnboarding = thisNodeFirstKnownAt.flatMap(_.previousBftTime)

    val initialLowerBound = thisNodeFirstKnownAt.flatMap { data =>
      for {
        epoch <- data.startEpochNumber
        blockNumber <- data.firstBlockNumberInStartEpoch
      } yield (epoch, blockNumber)
    }
    epochChecker.check(
      bootstrapTopologyInfo.thisNode,
      initialTopologyEpochNumber,
      bootstrapTopologyInfo.currentMembership,
    )

    val onboardingEpochCouldAlterOrderingTopology =
      thisNodeFirstKnownAt
        .flatMap(_.startEpochCouldAlterOrderingTopology)
        .exists(identity)
    val currentMembership = bootstrapTopologyInfo.currentMembership
    val outputModuleStartupState =
      OutputModule.StartupState(
        bootstrapTopologyInfo.thisNode,
        initialHeightToProvide =
          firstBlockNumberInOnboardingEpoch.getOrElse(sequencerSubscriptionInitialBlockNumber),
        initialTopologyEpochNumber,
        previousBftTimeForOnboarding,
        onboardingEpochCouldAlterOrderingTopology,
        bootstrapTopologyInfo.currentCryptoProvider,
        currentMembership,
        initialLowerBound,
        leaderSelectionPolicyFactory.leaderSelectionPolicy(
          blacklistLeaderSelectionState,
          currentMembership.orderingTopology,
        ),
      )
    new OrderingModuleSystemInitializer[E, P2PNetworkManagerT](
      ModuleFactories(
        mempool = { availabilityRef =>
          val cfg = MempoolModuleConfig(
            config.maxMempoolQueueSize,
            config.maxRequestPayloadBytes,
            config.maxRequestsInBatch,
            config.minRequestsInBatch,
            config.maxBatchCreationInterval,
            checkTags = config.standalone.isEmpty,
          )
          new MempoolModule(
            cfg,
            new MempoolState(currentMembership.orderingTopology.weakQuorum),
            metrics,
            availabilityRef,
            loggerFactory,
            timeouts,
          )
        },
        p2pNetworkIn = (availabilityRef, consensusRef) =>
          new P2PNetworkInModule(
            metrics,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
          ),
        p2pNetworkOut = (
            p2pNetworkInRef,
            mempoolRef,
            availabilityRef,
            consensusRef,
            outputRef,
            pruningRef,
        ) => {
          val dependencies = P2PNetworkOutModuleDependencies(
            createP2PNetworkManager,
            p2pNetworkInRef,
            mempoolRef,
            availabilityRef,
            consensusRef,
            outputRef,
            pruningRef,
          )
          val p2pNetworkOutModule = new P2PNetworkOutModule(
            bootstrapTopologyInfo.thisNode,
            isGenesis = initialTopologyEpochNumber == Bootstrap.BootstrapEpochNumber,
            p2pNetworkOutModuleStateFactory(bootstrapTopologyInfo.currentMembership),
            random,
            clock,
            stores.p2pEndpointsStore,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
            config.blockingDbReadTimeout,
            config.sendBlacklistTtl,
          )
          (p2pNetworkOutModule, p2pNetworkOutModule.p2pNetworkManager)
        },
        availability = (mempoolRef, networkOutRef, consensusRef, outputRef) => {
          val dependencies = AvailabilityModuleDependencies[E](
            mempoolRef,
            networkOutRef,
            consensusRef,
            outputRef,
          )
          new AvailabilityModule[E](
            bootstrapTopologyInfo.currentMembership,
            initialTopologyEpochNumber,
            bootstrapTopologyInfo.currentCryptoProvider,
            stores.availabilityStore,
            clock,
            random,
            metrics,
            dependencies,
            loggerFactory,
            timeouts,
            // In standalone mode don't check tag, as they are used for request IDs
            checkTags = config.standalone.isEmpty,
          )()
        },
        consensus = (p2pNetworkOutRef, availabilityRef, outputRef) => {
          val dependencies = ConsensusModuleDependencies(
            availabilityRef,
            outputRef,
            p2pNetworkOutRef,
          )

          val segmentModuleRefFactory =
            new SegmentModuleRefFactoryImpl(
              storePbftMessages = true,
              stores.epochStore,
              dependencies,
              config.consensusEmptyBlockCreationTimeout,
              loggerFactory,
              timeouts,
              metrics,
            )

          new PreIssConsensusModule(
            bootstrapTopologyInfo,
            stores.epochStore,
            sequencerSnapshotAdditionalInfo,
            clock,
            metrics,
            segmentModuleRefFactory,
            dependencies,
            loggerFactory,
            timeouts,
          )
        },
        output = (availabilityRef, consensusRef) =>
          new OutputModule(
            outputModuleStartupState,
            orderingTopologyProvider,
            leaderSelectionPolicyFactory,
            stores.outputStore,
            stores.epochStoreReader,
            blockSubscription,
            metrics,
            availabilityRef,
            consensusRef,
            loggerFactory,
            timeouts,
            requestInspector,
            epochChecker,
            previousStoredBlock = outputPreviousStoredBlock,
            stores.partitionManager.map(_._1),
          ),
        pruning = () =>
          new PruningModule(
            stores,
            clock,
            loggerFactory,
            timeouts,
            stores.partitionManager.map(_._2),
          ),
      )
    ).initialize(moduleSystem, createP2PNetworkManager)
  }

  private def fetchBootstrapTopologyInfo(
      moduleSystem: ModuleSystem[E],
      leaderSelectionPolicyFactory: LeaderSelectionInitializer[E],
  )(implicit
      traceContext: TraceContext
  ): (EpochNumber, OrderingTopologyInfo[E], BlacklistLeaderSelectionPolicyState) = {

    val bti @ BootstrapTopologyInfo(
      initialTopologyEpochNumber,
      initialTopologyQueryTimestampO,
      previousTopologyQueryTimestampO,
      maybeOnboardingTopologyQueryTimestamp,
    ) =
      getInitialAndPreviousTopologyQueryTimestamps(moduleSystem)

    logger.info(s"Retrieved bootstrap topologies timestamps: $bti")

    val (initialTopology, initialCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, initialTopologyQueryTimestampO, "initial")

    val leaderSelectionPolicyState =
      leaderSelectionPolicyFactory.stateForInitial(
        moduleSystem,
        sequencerSnapshotAdditionalInfo,
        initialTopologyEpochNumber,
      )
    val initialLeaders =
      leaderSelectionPolicyFactory.leadersFromState(
        leaderSelectionPolicyState,
        initialTopology,
      )
    val initialBlacklistedNodes =
      leaderSelectionPolicyFactory.blacklistedNodesFromState(
        leaderSelectionPolicyState,
        initialTopology,
      )

    val (previousTopology, previousCryptoProvider) =
      getOrderingTopologyAt(moduleSystem, previousTopologyQueryTimestampO, "previous")
    // the previousLeaders is not really used unless we are doing onboarding, in that case we should use previousTopology
    // to compute the "currentLeaders"
    val previousLeaders =
      leaderSelectionPolicyFactory.leadersFromState(
        leaderSelectionPolicyState,
        previousTopology,
      )
    val previousBlacklistedNodes =
      leaderSelectionPolicyFactory.blacklistedNodesFromState(
        leaderSelectionPolicyState,
        previousTopology,
      )

    val effectiveOnboardingTopologyQueryTimestamp =
      maybeOnboardingTopologyQueryTimestamp.orElse {
        Option
          .when(!initialTopology.contains(node))(reconstructOwnActivationTime(moduleSystem))
          .flatten
      }

    val maybeOnboardingTopologyAndCryptoProvider = effectiveOnboardingTopologyQueryTimestamp
      .map(onboardingTopologyQueryTimestamp =>
        getOrderingTopologyAt(moduleSystem, Some(onboardingTopologyQueryTimestamp), "onboarding")
      )

    (
      initialTopologyEpochNumber,
      OrderingTopologyInfo(
        node,
        // Use the previous topology (not containing this node) as current topology when onboarding.
        //  This prevents relying on newly onboarded nodes for state transfer.
        currentTopology = initialTopology,
        currentCryptoProvider =
          maybeOnboardingTopologyAndCryptoProvider.fold(initialCryptoProvider) {
            case (_, onboardingCryptoProvider) =>
              DelegationCryptoProvider(
                // Note that, when onboarding, the signing crypto provider corresponds to the onboarding node activation
                //  timestamp (so that its signing key is present), the verification will use the one at the start of epoch.
                signer = onboardingCryptoProvider,
                verifier = initialCryptoProvider,
              )
          },
        currentLeaders = initialLeaders,
        currentBlacklistedNodes = initialBlacklistedNodes,
        previousTopology, // for canonical commit set verification
        previousCryptoProvider, // for canonical commit set verification
        previousLeaders,
        previousBlacklistedNodes,
      ),
      leaderSelectionPolicyState,
    )
  }

  private def getInitialAndPreviousTopologyQueryTimestamps(
      moduleSystem: ModuleSystem[E]
  )(implicit traceContext: TraceContext) =
    sequencerSnapshotAdditionalInfo match {
      case Some(additionalInfo) =>
        // We assume that, if a sequencer snapshot has been provided, then we're onboarding; in that case, we use
        //  topology information from the sequencer snapshot, else we fetch the latest topology from the DB.
        val thisNodeActiveAt = additionalInfo.nodeActiveAt.getOrElse(
          node,
          failBootstrap("Activation information is required when onboarding but it's empty"),
        )
        val epochNumber = thisNodeActiveAt.startEpochNumber.getOrElse(
          failBootstrap("Start epoch information is required when onboarding but it's empty")
        )
        val initialTopologyQueryTimestamp =
          thisNodeActiveAt.startEpochTopologyQueryTimestamp.getOrElse(
            failBootstrap(
              "Start epoch topology query timestamp is required when onboarding but it's empty"
            )
          )
        val previousTopologyQueryTimestamp =
          thisNodeActiveAt.previousEpochTopologyQueryTimestamp.getOrElse {
            // If the start epoch is immediately after the genesis epoch
            initialTopologyQueryTimestamp
          }
        val onboardingTopologyQueryTimestamp = thisNodeActiveAt.timestamp
        BootstrapTopologyInfo(
          epochNumber,
          Some(initialTopologyQueryTimestamp),
          Some(previousTopologyQueryTimestamp),
          Some(onboardingTopologyQueryTimestamp),
        )

      case _ =>
        // Regular (i.e., non-onboarding) startup; it can be a crash recovery or a clean slate (either genesis or
        //  the target physical synchronizer of an LSU).
        //  If it's a crash recovery, fetching the latest epoch object will return an activation time that is
        //  different from the default bootstrap topology activation time.
        val (initialTopologyEpochNumberO, initialTopologyQueryTimestampO) =
          fetchLatestEpoch(
            moduleSystem,
            includeInProgress = true,
          ).map(epoch => epoch.info.number -> epoch.info.topologyActivationTime).unzip
        // TODO(#24262) if restarted just after completing an epoch, the previous and initial topology might be the same
        val previousTopologyQueryTimestampO =
          fetchLatestEpoch(moduleSystem, includeInProgress = false).map(
            _.info.topologyActivationTime
          )
        BootstrapTopologyInfo(
          initialTopologyEpochNumberO.getOrElse(BootstrapEpochNumber),
          initialTopologyQueryTimestampO,
          previousTopologyQueryTimestampO,
        )
    }

  private def getOrderingTopologyAt(
      moduleSystem: ModuleSystem[E],
      topologyQueryTimestampO: Option[TopologyActivationTime],
      topologyDesignation: String,
  )(implicit traceContext: TraceContext): (OrderingTopology, CryptoProvider[E]) =
    awaitFuture(
      moduleSystem,
      orderingTopologyProvider.getOrderingTopologyAt(
        topologyQueryTimestampO,
        checkPendingChanges = false,
      ),
      s"Fetching $topologyDesignation ordering topology for bootstrap",
    ).getOrElse(failBootstrap(s"Failed to fetch $topologyDesignation ordering topology"))

  private def reconstructOwnActivationTime(
      moduleSystem: ModuleSystem[E]
  )(implicit traceContext: TraceContext): Option[TopologyActivationTime] = {
    val (headTopology, _) = getOrderingTopologyAt(moduleSystem, None, "head")
    awaitFuture(
      moduleSystem,
      orderingTopologyProvider.getFirstKnownAt(headTopology.activationTime),
      "Fetching this node's activation time for onboarding crash recovery",
    ).flatMap(_.get(node))
  }

  private def fetchLatestEpoch(
      moduleSystem: ModuleSystem[E],
      includeInProgress: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Option[EpochStore.Epoch] =
    awaitFuture(
      moduleSystem,
      stores.epochStore.latestEpoch(includeInProgress),
      s"Fetching latest${if (includeInProgress) " in-progress " else " "}epoch",
    )

  private def failBootstrap(msg: String)(implicit traceContext: TraceContext) = {
    logger.error(msg)
    sys.error(msg)
  }

  private def awaitFuture[X](
      moduleSystem: ModuleSystem[E],
      future: E#FutureUnlessShutdownT[X],
      description: String,
  )(implicit traceContext: TraceContext): X = {
    logger.info(description)
    moduleSystem.rootActorContext.blockingAwait(
      future,
      config.initQueryTimeout.underlying,
    )
  }
}

object BftOrderingModuleSystemInitializer {

  final case class BftOrderingStores[E <: Env[E]](
      p2pEndpointsStore: P2PEndpointsStore[E],
      availabilityStore: AvailabilityStore[E],
      epochStore: EpochStore[E],
      epochStoreReader: EpochStoreReader[E],
      outputStore: OutputMetadataStore[E],
      pruningSchedulerStore: BftOrdererPruningSchedulerStore[E],
      partitionManager: Option[
        (PartitionManager.PartitionCreator[E], PartitionManager.PartitionPruner[E])
      ],
  )

  /** In case of onboarding, the topology query timestamps look as follows:
    * {{{
    * ───|────────────|─────────────────────|──────────────────────────|──────────> time
    *   Previous     Initial topology ts   Onboarding topology ts     (Topology ts, where
    *   topology ts  (start epoch)         (node active in topology)  node is active in consensus)
    * }}}
    *
    * @param initialTopologyEpochNumber
    *   A start epoch number.
    * @param initialTopologyQueryTimestamp
    *   A timestamp to get an initial topology (and a crypto provider) for signing and validation.
    *   `None` if starting from a bootstrap topology.
    * @param previousTopologyQueryTimestamp
    *   A timestamp to get a topology (and a crypto provider) for canonical commit set validation at
    *   the first epoch boundary. `None` if starting from a bootstrap topology.
    * @param onboardingTopologyQueryTimestamp
    *   An optional timestamp to get a topology (and a crypto provider) for signing state transfer
    *   requests for onboarding.
    */
  final case class BootstrapTopologyInfo(
      initialTopologyEpochNumber: EpochNumber,
      initialTopologyQueryTimestamp: Option[TopologyActivationTime],
      previousTopologyQueryTimestamp: Option[TopologyActivationTime],
      onboardingTopologyQueryTimestamp: Option[TopologyActivationTime] = None,
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[BootstrapTopologyInfo] =
      prettyOfClass(
        param("initialTopologyEpochNumber", _.initialTopologyEpochNumber),
        param("initialTopologyQueryTimestamp", _.initialTopologyQueryTimestamp.map(_.value)),
        param("previousTopologyQueryTimestamp", _.previousTopologyQueryTimestamp.map(_.value)),
        param("onboardingTopologyQueryTimestamp", _.onboardingTopologyQueryTimestamp.map(_.value)),
      )
  }
}
