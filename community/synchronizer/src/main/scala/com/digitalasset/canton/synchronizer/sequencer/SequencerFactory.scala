// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.daml.metrics.ExecutorServiceMetrics
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.config.PublicServerConfig
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.{
  BftSequencer,
  DeprecatedSequencerConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.time.{
  DisasterRecoverySequencingTimeUpperBound,
  LsuSequencingBounds,
}
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait SequencerFactory extends FlagCloseable with HasCloseContext {

  def initialize(
      initialState: SequencerInitialState,
      sequencerId: SequencerId,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]

  def create(
      sequencerId: SequencerId,
      clock: Clock,
      synchronizerSyncCryptoApi: SynchronizerCryptoClient,
      futureSupervisor: FutureSupervisor,
      trafficConfig: SequencerTrafficConfig,
      lsuSequencingBounds: Option[LsuSequencingBounds],
      drSequencingTimeUpperBound: Option[DisasterRecoverySequencingTimeUpperBound],
      runtimeReady: FutureUnlessShutdown[Unit],
      sequencerSnapshot: Option[SequencerSnapshot],
      authenticationServices: Option[AuthenticationServices],
  )(implicit
      traceContext: TraceContext,
      tracer: Tracer,
      actorMaterializer: Materializer,
  ): FutureUnlessShutdown[Sequencer]
}

object SequencerMetaFactory {

  def createFactory(
      protocolVersion: ProtocolVersion,
      health: Option[SequencerHealthConfig],
      metrics: SequencerMetrics,
      executorServiceMetrics: ExecutorServiceMetrics,
      storage: Storage,
      sequencerId: SequencerId,
      nodeParameters: SequencerNodeParameters,
      publicApi: PublicServerConfig,
      loggerFactory: NamedLoggerFactory,
  )(
      sequencerConfig: SequencerConfig
  )(implicit executionContext: ExecutionContextExecutor): SequencerFactory =
    sequencerConfig match {
      case BftSequencer(blockSequencerConfig, config) =>
        new BftSequencerFactory(
          config,
          publicApi,
          blockSequencerConfig,
          health,
          storage,
          protocolVersion,
          sequencerId,
          nodeParameters,
          metrics,
          executorServiceMetrics,
          loggerFactory,
          blockSequencerConfig.testingInterceptor,
        )

      case DeprecatedSequencerConfig(sequencerType) =>
        sys.error(
          s"Sequencer of type $sequencerType is no longer supported. Please use a supported sequencer type instead."
        )
    }
}
