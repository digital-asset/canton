// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import akka.actor.ActorSystem
import cats.data.EitherT
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.config.DomainNodeParameters
import com.digitalasset.canton.domain.mediator.{DomainNodeMediatorFactory, MediatorRuntime}
import com.digitalasset.canton.domain.metrics.DomainMetrics
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.SequencerClientFactory
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, DomainTimeTrackerConfig}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.{MediatorId, NodeId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContextExecutorService, Future}

object EmbeddedMediatorInitialization {

  def apply(
      id: DomainId,
      nodeId: NodeId,
      cantonParameterConfig: DomainNodeParameters,
      clock: Clock,
      crypto: Crypto,
      mediatorTopologyStore: TopologyStore,
      timeTrackerConfig: DomainTimeTrackerConfig,
      storage: Storage,
      sequencerClientFactoryFactory: DomainTopologyClientWithInit => SequencerClientFactory,
      metrics: DomainMetrics,
      domainNodeMediatorFactory: DomainNodeMediatorFactory,
      indexedStringStore: IndexedStringStore,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutorService,
      tracer: Tracer,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[Future, String, MediatorRuntime] = {

    val factory = domainNodeMediatorFactory.mediatorRuntimeFactory
    val mediatorId = MediatorId(id) // The embedded mediator always has the same ID as the domain
    val sendTrackerStore = SendTrackerStore(storage)
    for {

      mediatorDiscriminator <- EitherT.right(
        SequencerClientDiscriminator.fromDomainMember(mediatorId, indexedStringStore)
      )
      sequencedEventStore = SequencedEventStore(
        storage,
        mediatorDiscriminator,
        cantonParameterConfig.processingTimeouts,
        loggerFactory,
      )
      // The mediator has its own sequencer client subscription and therefore needs a separate sequencer counter tracker store
      mediatorSequencerCounterTrackerStore = SequencerCounterTrackerStore(
        storage,
        mediatorDiscriminator,
        loggerFactory,
      )

      processorAndClient <- EitherT.right(
        TopologyTransactionProcessor.createProcessorAndClientForDomain(
          mediatorTopologyStore,
          id,
          crypto.pureCrypto,
          Map(),
          cantonParameterConfig,
          clock,
          futureSupervisor,
          loggerFactory,
        )
      )
      (topologyProcessor, topologyClient) = processorAndClient
      syncCrypto =
        new DomainSyncCryptoClient(
          mediatorId,
          id,
          topologyClient,
          crypto,
          cantonParameterConfig.cachingConfigs,
          loggerFactory,
        )

      sequencerClient <- sequencerClientFactoryFactory(topologyClient)(
        mediatorId,
        sequencedEventStore,
        sendTrackerStore,
      )

      identityClientHandler = topologyProcessor.createHandler(id)
      mediatorRuntime <- factory
        .create(
          mediatorId,
          id,
          storage,
          mediatorSequencerCounterTrackerStore,
          sequencedEventStore,
          sequencerClient,
          syncCrypto,
          topologyClient,
          identityClientHandler,
          timeTrackerConfig,
          cantonParameterConfig,
          clock,
          metrics.mediator,
          futureSupervisor,
          loggerFactory,
        )
      _ <- EitherT.right[String](mediatorRuntime.mediator.start())
    } yield mediatorRuntime
  }
}
