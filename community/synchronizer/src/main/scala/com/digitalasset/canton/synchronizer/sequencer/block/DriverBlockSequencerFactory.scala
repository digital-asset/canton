// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager
import com.digitalasset.canton.synchronizer.block.data.SequencerBlockStore
import com.digitalasset.canton.synchronizer.block.update.BlockProcessingParameters
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.time.LsuSequencingBounds
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.synchronizer.sequencer.{
  AuthenticationServices,
  BlockSequencerConfig,
  SequencerHealthConfig,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  ReferenceSequencerDriver,
  ReferenceSequencerDriverFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.ProtocolVersion
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContextExecutor

import BlockSequencerFactory.OrderingTimeFixMode

class DriverBlockSequencerFactory(
    config: ReferenceSequencerDriver.Config[StorageConfig],
    blockSequencerConfig: BlockSequencerConfig,
    health: Option[SequencerHealthConfig],
    storage: Storage,
    protocolVersion: ProtocolVersion,
    sequencerId: SequencerId,
    nodeParameters: SequencerNodeParameters,
    metrics: SequencerMetrics,
    override val loggerFactory: NamedLoggerFactory,
    testingInterceptor: Option[TestingInterceptor],
)(implicit ec: ExecutionContextExecutor)
    extends BlockSequencerFactory(
      health: Option[SequencerHealthConfig],
      blockSequencerConfig,
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      loggerFactory,
      testingInterceptor,
      metrics,
    ) {

  override protected final val name: String = "reference"
  private val sequencerDriverFactory = new ReferenceSequencerDriverFactory()

  override protected final lazy val orderingTimeFixMode: OrderingTimeFixMode =
    OrderingTimeFixMode.MakeStrictlyIncreasing

  override protected def createBlockOrderer(
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
  ): UnlessShutdown[BlockOrderer] =
    UnlessShutdown.Outcome(
      new DriverBlockOrderer(
        sequencerDriverFactory.create(
          config,
          nodeParameters.nonStandardConfig,
          clock,
          initialBlockHeight,
          cryptoApi.psid.toString,
          sequencerId.toProtoPrimitive,
          synchronizerLoggerFactory,
        ),
        orderingTimeFixMode,
      )
    )

  override protected final def createBlockSequencer(
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
      lsuSequencingBounds: Option[LsuSequencingBounds],
      synchronizerLoggerFactory: NamedLoggerFactory,
      runtimeReady: FutureUnlessShutdown[Unit],
  )(implicit
      ec: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
  ): BlockSequencer =
    new BlockSequencer(
      blockOrderer,
      name,
      cryptoApi,
      sequencerId,
      stateManager,
      store,
      sequencerStore,
      blockSequencerConfig,
      balanceStore,
      storage,
      futureSupervisor,
      health,
      clock,
      rateLimitManager,
      BlockProcessingParameters(
        orderingTimeFixMode,
        lsuSequencingBounds,
        nodeParameters,
      ),
      nodeParameters,
      metrics,
      synchronizerLoggerFactory,
      runtimeReady = runtimeReady,
    )
}

object DriverBlockSequencerFactory extends LazyLogging {

  def apply(
      config: ReferenceSequencerDriver.Config[StorageConfig],
      blockSequencerConfig: BlockSequencerConfig,
      health: Option[SequencerHealthConfig],
      storage: Storage,
      protocolVersion: ProtocolVersion,
      sequencerId: SequencerId,
      nodeParameters: SequencerNodeParameters,
      metrics: SequencerMetrics,
      loggerFactory: NamedLoggerFactory,
      testingInterceptor: Option[TestingInterceptor],
  )(implicit
      ec: ExecutionContextExecutor
  ): DriverBlockSequencerFactory =
    new DriverBlockSequencerFactory(
      config,
      blockSequencerConfig,
      health,
      storage,
      protocolVersion,
      sequencerId,
      nodeParameters,
      metrics,
      loggerFactory,
      testingInterceptor,
    )
}
