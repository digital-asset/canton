// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import akka.actor.ActorSystem
import cats.syntax.option._
import com.digitalasset.canton.config.{
  LocalNodeParameters,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.domain.admin.v0.EnterpriseSequencerAdministrationServiceGrpc
import com.digitalasset.canton.domain.config.DomainConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommunitySequencerConfig,
  SequencerFactory,
}
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.TopologyStore
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContextExecutor

trait SequencerRuntimeFactory {
  def create(
      domainId: DomainId,
      crypto: Crypto,
      sequencedTopologyStore: TopologyStore,
      domainClient: DomainTopologyClientWithInit,
      storage: Storage,
      clock: Clock,
      domainConfig: DomainConfig,
      staticDomainParameters: StaticDomainParameters,
      testingConfig: TestingConfigInternal,
      processingTimeout: ProcessingTimeout,
      auditLogger: TracedLogger,
      agreementManager: Option[ServiceAgreementManager],
      localParameters: LocalNodeParameters,
      metrics: SequencerMetrics,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
      logger: TracedLogger,
  )(implicit
      executionContext: ExecutionContextExecutor,
      tracer: Tracer,
      system: ActorSystem,
  ): SequencerRuntime
}

object SequencerRuntimeFactory {
  class Community(sequencerConfig: CommunitySequencerConfig.Database)
      extends SequencerRuntimeFactory {
    override def create(
        domainId: DomainId,
        crypto: Crypto,
        sequencedTopologyStore: TopologyStore,
        domainClient: DomainTopologyClientWithInit,
        storage: Storage,
        clock: Clock,
        domainConfig: DomainConfig,
        staticDomainParameters: StaticDomainParameters,
        testingConfig: TestingConfigInternal,
        processingTimeout: ProcessingTimeout,
        auditLogger: TracedLogger,
        agreementManager: Option[ServiceAgreementManager],
        localParameters: LocalNodeParameters,
        metrics: SequencerMetrics,
        indexedStringStore: IndexedStringStore,
        loggerFactory: NamedLoggerFactory,
        logger: TracedLogger,
    )(implicit
        executionContext: ExecutionContextExecutor,
        tracer: Tracer,
        system: ActorSystem,
    ): SequencerRuntime =
      new SequencerRuntime(
        SequencerFactory.database(sequencerConfig, loggerFactory),
        staticDomainParameters,
        localParameters,
        domainConfig.timeTracker,
        testingConfig,
        metrics,
        domainId,
        crypto,
        sequencedTopologyStore,
        domainClient,
        storage,
        clock,
        auditLogger,
        None,
        SequencerAuthenticationConfig(
          agreementManager,
          domainConfig.publicApi.nonceExpirationTime,
          domainConfig.publicApi.tokenExpirationTime,
        ),
        None,
        _ =>
          StaticGrpcServices
            .notSupportedByCommunity(EnterpriseSequencerAdministrationServiceGrpc.SERVICE, logger)
            .some,
        registerSequencerMember =
          false, // the community sequencer is always an embedded single sequencer
        indexedStringStore,
        agreementManager,
        loggerFactory,
      )
  }
}
