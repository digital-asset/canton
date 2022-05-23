// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import akka.stream.Materializer
import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton._
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain._
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateFactory,
}
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyDispatcher,
  ParticipantTopologyManagerError,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreFactory, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future}

/** Domain registry used to connect to domains over GRPC
  *
  * @param participantId The participant id from which we connect to domains.
  * @param participantNodeParameters General set of parameters that control Canton
  * @param ec ExecutionContext used by the sequencer client
  * @param trustDomain a call back handle to the participant topology manager to issue a domain trust certificate
  */
class GrpcDomainRegistry(
    val participantId: ParticipantId,
    agreementService: AgreementService,
    topologyDispatcher: ParticipantTopologyDispatcher,
    val aliasManager: DomainAliasManager,
    cryptoApiProvider: SyncCryptoApiProvider,
    cryptoConfig: CryptoConfig,
    topologyStoreFactory: TopologyStoreFactory,
    clock: Clock,
    val participantNodeParameters: ParticipantNodeParameters,
    testingConfig: TestingConfigInternal,
    recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    trustDomain: (
        DomainId,
        StaticDomainParameters,
        TraceContext,
    ) => FutureUnlessShutdown[Either[ParticipantTopologyManagerError, Unit]],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    metrics: DomainAlias => SyncDomainMetrics,
    protected val futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContextExecutor, val materializer: Materializer, val tracer: Tracer)
    extends DomainRegistry
    with DomainRegistryHelpers
    with FlagCloseable
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  private class GrpcDomainHandle(
      val domainId: DomainId,
      val domainAlias: DomainAlias,
      val staticParameters: StaticDomainParameters,
      sequencer: SequencerClient,
      val topologyClient: DomainTopologyClientWithInit,
      val topologyStore: TopologyStore[TopologyStoreId.DomainStore],
      val domainPersistentState: SyncDomainPersistentState,
      override protected val timeouts: ProcessingTimeout,
  ) extends DomainHandle
      with FlagCloseableAsync
      with NamedLogging {

    override val sequencerClient: SequencerClient = sequencer
    override def loggerFactory: NamedLoggerFactory = GrpcDomainRegistry.this.loggerFactory

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
      import TraceContext.Implicits.Empty._
      List[AsyncOrSyncCloseable](
        SyncCloseable(
          "topologyOutbox",
          topologyDispatcher.domainDisconnected(domainAlias),
        ),
        SyncCloseable("agreementService", agreementService.close()),
        SyncCloseable("sequencerClient", sequencerClient.close()),
      )
    }
  }

  override def connect(
      config: DomainConnectionConfig,
      syncDomainPersistentStateFactory: SyncDomainPersistentStateFactory,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, DomainHandle]] = {
    val sequencerConnection = config.sequencerConnection

    val runE = for {
      sequencerConnectClient <- SequencerConnectClient(
        config,
        cryptoApiProvider.crypto,
        timeouts,
        participantNodeParameters.tracing.propagation,
        loggerFactory,
      ).mapK(FutureUnlessShutdown.outcomeK)

      agreementClient = new AgreementClient(agreementService, sequencerConnection, loggerFactory)

      domainHandle <- getDomainHandle(
        config,
        participantNodeParameters.protocolConfig,
        sequencerConnection,
        syncDomainPersistentStateFactory,
      )(
        topologyDispatcher.manager.store,
        cryptoApiProvider,
        cryptoConfig,
        topologyStoreFactory,
        clock,
        testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        trustDomain,
        packageDependencies,
        metrics,
        agreementClient,
        sequencerConnectClient,
      ).thereafter(_ => sequencerConnectClient.close())
    } yield new GrpcDomainHandle(
      domainHandle.domainId,
      domainHandle.alias,
      domainHandle.staticParameters,
      domainHandle.sequencer,
      domainHandle.topologyClient,
      domainHandle.topologyStore,
      domainHandle.domainPersistentState,
      domainHandle.timeouts,
    )

    runE.value
  }
}
