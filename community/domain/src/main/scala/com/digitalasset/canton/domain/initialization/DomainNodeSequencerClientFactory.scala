// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.Domain
import com.digitalasset.canton.domain.metrics.DomainMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SequencerClient,
  SequencerClientFactory,
  SequencerClientTransportFactory,
}
import com.digitalasset.canton.store.{SendTrackerStore, SequencedEventStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersionCompatibility
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContextExecutor, Future}

// customize the default sequencer-client-factory to allow passing in separate metrics and a customized logger factory
// to be able to distinguish between the mediator and topology manager when running in the same node
class DomainNodeSequencerClientFactory(
    id: DomainId,
    metrics: DomainMetrics,
    topologyClient: DomainTopologyClientWithInit,
    sequencerConnection: SequencerConnection,
    cantonNodeParameters: CantonNodeParameters,
    crypto: Crypto,
    domainParameters: StaticDomainParameters,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
) extends SequencerClientFactory
    with SequencerClientTransportFactory
    with NamedLogging {

  override def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      requestSigner: RequestSigner,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClient] =
    factory(member).create(member, sequencedEventStore, sendTrackerStore, requestSigner)

  override def makeTransport(connection: SequencerConnection, member: Member)(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClientTransport] =
    factory(member).makeTransport(connection, member)

  private def factory(member: Member)(implicit
      executionContext: ExecutionContextExecutor
  ): SequencerClientFactory with SequencerClientTransportFactory = {
    val (clientMetrics, clientName) = member match {
      case MediatorId(_) => (metrics.mediator.sequencerClient, "mediator")
      case DomainTopologyManagerId(_) =>
        (metrics.topologyManager.sequencerClient, "topology-manager")
      case other => sys.error(s"Unexpected sequencer client in Domain node: $other")
    }

    val sequencerId: SequencerId = SequencerId(id)

    val clientLoggerFactory = loggerFactory.append("client", clientName)

    val sequencerClientSyncCrypto =
      new DomainSyncCryptoClient(
        sequencerId,
        id,
        topologyClient,
        crypto,
        cantonNodeParameters.cachingConfigs,
        cantonNodeParameters.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

    SequencerClient(
      sequencerConnection,
      id,
      sequencerId,
      sequencerClientSyncCrypto,
      crypto,
      None,
      cantonNodeParameters.sequencerClient,
      cantonNodeParameters.tracing.propagation,
      testingConfig,
      domainParameters,
      cantonNodeParameters.processingTimeouts,
      clock,
      topologyClient,
      futureSupervisor,
      member =>
        Domain.recordSequencerInteractions
          .get()
          .lift(member)
          .map(Domain.setMemberRecordingPath(member)),
      member =>
        Domain.replaySequencerConfig.get().lift(member).map(Domain.defaultReplayPath(member)),
      clientMetrics,
      cantonNodeParameters.loggingConfig,
      clientLoggerFactory,
      supportedProtocolVersions = ProtocolVersionCompatibility
        .supportedProtocolsDomain(includeUnstableVersions = cantonNodeParameters.devVersionSupport),
      minimumProtocolVersion = None,
    )
  }
}
