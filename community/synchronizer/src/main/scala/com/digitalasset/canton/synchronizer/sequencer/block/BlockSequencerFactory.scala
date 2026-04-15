// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.EitherT
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager
import com.digitalasset.canton.synchronizer.block.data.{BlockEphemeralState, SequencerBlockStore}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.time.{
  DisasterRecoverySequencingTimeUpperBound,
  LsuSequencingBounds,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.{
  TrafficConsumedStore,
  TrafficPurchasedStore,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.{
  SequencerRateLimitManagerImpl,
  TrafficPurchasedManager,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

import BlockSequencerFactory.OrderingTimeFixMode

abstract class BlockSequencerFactory(
    health: Option[SequencerHealthConfig],
    blockSequencerConfig: BlockSequencerConfig,
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: SequencerNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
    metrics: SequencerMetrics,
)(implicit ec: ExecutionContextExecutor)
    extends DatabaseSequencerFactory(
      blockSequencerConfig.toDatabaseSequencerConfig,
      storage,
      nodeParameters.cachingConfigs,
      nodeParameters.batchingConfig,
      nodeParameters.processingTimeouts,
      protocolVersion,
      sequencerId,
      blockSequencerMode = true,
      metrics,
    )
    with NamedLogging {

  private val store = SequencerBlockStore(
    storage,
    protocolVersion,
    sequencerStore,
    nodeParameters.processingTimeouts,
    loggerFactory,
    nodeParameters.batchingConfig,
  )

  private val trafficPurchasedStore = TrafficPurchasedStore(
    storage,
    nodeParameters.processingTimeouts,
    loggerFactory,
    nodeParameters.batchingConfig.aggregator,
    sequencerStore,
  )

  private val trafficConsumedStore = TrafficConsumedStore(
    storage,
    nodeParameters.processingTimeouts,
    loggerFactory,
    nodeParameters.batchingConfig,
    sequencerStore,
  )

  protected val name: String

  protected val orderingTimeFixMode: OrderingTimeFixMode

  protected def createBlockOrderer(
      cryptoApi: SynchronizerCryptoClient,
      clock: Clock,
      initialBlockHeight: Option[Long],
      sequencerSnapshot: Option[SequencerSnapshot],
      authenticationServices: Option[AuthenticationServices],
      synchronizerLoggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockOrderer

  protected def createBlockSequencer(
      blockOrderer: BlockOrderer,
      name: String,
      cryptoApi: SynchronizerCryptoClient,
      stateManager: BlockSequencerStateManager,
      store: SequencerBlockStore,
      balanceStore: TrafficPurchasedStore,
      storage: Storage,
      futureSupervisor: FutureSupervisor,
      health: Option[SequencerHealthConfig],
      clock: Clock,
      rateLimitManager: SequencerRateLimitManager,
      orderingTimeFixMode: OrderingTimeFixMode,
      synchronizerLoggerFactory: NamedLoggerFactory,
      lsuSequencingBounds: Option[LsuSequencingBounds],
      runtimeReady: FutureUnlessShutdown[Unit],
  )(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer

  override final def initialize(
      snapshot: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(s"Storing sequencers initial state: $snapshot")
    for {
      _ <- super[DatabaseSequencerFactory].initialize(
        snapshot,
        sequencerId,
      ) // Members are stored in the DBS
      _ <- EitherT
        .right(
          store.setInitialState(snapshot, snapshot.initialTopologyEffectiveTimestamp)
        )
      _ <- EitherT
        .right(
          snapshot.snapshot.trafficPurchased.parTraverse_(trafficPurchasedStore.store)
        )
      _ <- EitherT
        .right(trafficConsumedStore.store(snapshot.snapshot.trafficConsumed))
      _ = logger.debug(
        s"from snapshot: ticking traffic purchased entry manager with ${snapshot.latestSequencerEventTimestamp}"
      )
      _ <- EitherT
        .right(
          snapshot.latestSequencerEventTimestamp
            .traverse(ts => trafficPurchasedStore.setInitialTimestamp(ts))
        )
    } yield ()
  }

  @VisibleForTesting
  protected def makeRateLimitManager(
      trafficPurchasedManager: TrafficPurchasedManager,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      protocolVersion: ProtocolVersion,
      trafficConfig: SequencerTrafficConfig,
  ): SequencerRateLimitManager =
    new SequencerRateLimitManagerImpl(
      trafficPurchasedManager,
      trafficConsumedStore,
      loggerFactory,
      nodeParameters.processingTimeouts,
      nodeParameters.batchingConfig,
      metrics,
      synchronizerSyncCryptoApi,
      protocolVersion,
      trafficConfig,
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )

  @nowarn("cat=deprecation")
  private def getHeadState(
      synchronizerSyncCryptoApi: SynchronizerCryptoClient
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[BlockEphemeralState]] =
    for {
      // first read the head block info, to get the latest sequenced timestamp
      headBlockInfo <- store.readHeadBlockInfo()
      headStateO <- headBlockInfo
        .map(_.lastTs)
        .flatTraverse[FutureUnlessShutdown, BlockEphemeralState] { lastTs =>
          for {
            // we can use the head snapshot, because we load the dynamic parameter history and then
            // compute the highest max sequencing time
            parameterChanges <- synchronizerSyncCryptoApi.headSnapshot.ipsSnapshot
              .listDynamicSynchronizerParametersChanges()
            maxSequencingTimeBound = SequencerUtils.maxSequencingTimeUpperBoundAt(
              lastTs,
              parameterChanges,
            )
            initialHeadO <- store.readHead(maxSequencingTimeBound)
          } yield initialHeadO
        }

    } yield headStateO

  override final def create(
      sequencerId: SequencerId,
      clock: Clock,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
      lsuSequencingBounds: Option[LsuSequencingBounds],
      drSequencingTimeUpperBound: Option[DisasterRecoverySequencingTimeUpperBound],
      runtimeReady: FutureUnlessShutdown[Unit],
      sequencerSnapshot: Option[SequencerSnapshot] = None,
      authenticationServices: Option[AuthenticationServices] = None,
  )(implicit
      traceContext: TraceContext,
      tracer: trace.Tracer,
      actorMaterializer: Materializer,
  ): FutureUnlessShutdown[Sequencer] =
    for {
      initialHeadO <- getHeadState(synchronizerSyncCryptoApi)
      initialBlockHeight = initialHeadO.map(_.latestBlock.height + 1)
      _ = logger.info(s"Creating $name sequencer at block height $initialBlockHeight")

      balanceManager = new TrafficPurchasedManager(
        trafficPurchasedStore,
        trafficConfig,
        futureSupervisor,
        metrics,
        nodeParameters.processingTimeouts,
        lsuSequencingBounds,
        loggerFactory,
      )
      rateLimitManager = makeRateLimitManager(
        balanceManager,
        synchronizerSyncCryptoApi,
        protocolVersion,
        trafficConfig,
      )
      _ <- balanceManager.initialize
    } yield {
      val synchronizerLoggerFactory =
        loggerFactory.append("psid", synchronizerSyncCryptoApi.psid.toString)
      val stateManager =
        BlockSequencerStateManager.create(
          initialHeadO,
          store,
          trafficConsumedStore,
          nodeParameters.asyncWriter,
          nodeParameters.enableAdditionalConsistencyChecks,
          nodeParameters.processingTimeouts,
          futureSupervisor,
          synchronizerLoggerFactory,
          blockSequencerConfig.streamInstrumentation,
          metrics.block,
        )

      val blockOrderer = createBlockOrderer(
        synchronizerSyncCryptoApi,
        clock,
        initialBlockHeight,
        sequencerSnapshot,
        authenticationServices,
        synchronizerLoggerFactory,
      )
      val sequencer = createBlockSequencer(
        blockOrderer,
        name,
        synchronizerSyncCryptoApi,
        stateManager,
        store,
        trafficPurchasedStore,
        storage,
        futureSupervisor,
        health,
        clock,
        rateLimitManager,
        orderingTimeFixMode,
        synchronizerLoggerFactory,
        lsuSequencingBounds,
        runtimeReady,
      )
      testingInterceptor
        .map(_(clock)(sequencer)(ec))
        .getOrElse(sequencer)
    }

  override def onClosed(): Unit =
    LifeCycle.close(store)(logger)
}

object BlockSequencerFactory {

  /** Whether a sequencer implementation requires `BlockUpdateGenerator` to adjust ordering
    * timestamps to ensure they are strictly increasing or just validate that they are.
    */
  sealed trait OrderingTimeFixMode

  object OrderingTimeFixMode {

    /** Ordering timestamps are not necessarily unique or increasing. Clients should adjust
      * timestamps to enforce that.
      */
    final case object MakeStrictlyIncreasing extends OrderingTimeFixMode

    /** Ordering timestamps are strictly monotonically increasing. */
    final case object ValidateOnly extends OrderingTimeFixMode
  }
}
