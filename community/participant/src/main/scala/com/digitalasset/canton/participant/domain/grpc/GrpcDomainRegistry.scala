// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import akka.stream.Materializer
import cats.Eval
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.common.domain.SequencerConnectClient.{
  DomainClientBootstrapInfo,
  TopologyRequestAddressX,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout, TestingConfigInternal}
import com.digitalasset.canton.crypto.SyncCryptoApiProvider
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.DomainRegistryHelpers.SequencerAggregatedInfo
import com.digitalasset.canton.participant.domain.*
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.store.{
  ParticipantSettingsLookup,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.participant.topology.{
  ParticipantTopologyDispatcherCommon,
  TopologyComponentFactory,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.version.ProtocolVersionCompatibility
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
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    participantSettings: Eval[ParticipantSettingsLookup],
    agreementService: AgreementService,
    topologyDispatcher: ParticipantTopologyDispatcherCommon,
    val aliasManager: DomainAliasManager,
    cryptoApiProvider: SyncCryptoApiProvider,
    cryptoConfig: CryptoConfig,
    clock: Clock,
    val participantNodeParameters: ParticipantNodeParameters,
    testingConfig: TestingConfigInternal,
    recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
    metrics: DomainAlias => SyncDomainMetrics,
    override protected val futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerInfoLoadParallelism: NonNegativeInt = NonNegativeInt.tryCreate(4),
)(implicit
    val ec: ExecutionContextExecutor,
    val materializer: Materializer,
    val tracer: Tracer,
) extends DomainRegistry
    with DomainRegistryHelpers
    with FlagCloseable
    with HasFutureSupervision
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = participantNodeParameters.processingTimeouts

  private class GrpcDomainHandle(
      override val domainId: DomainId,
      override val domainAlias: DomainAlias,
      override val staticParameters: StaticDomainParameters,
      sequencer: SequencerClient,
      override val topologyClient: DomainTopologyClientWithInit,
      override val topologyFactory: TopologyComponentFactory,
      override val topologyRequestAddress: Option[TopologyRequestAddressX],
      override val domainPersistentState: SyncDomainPersistentState,
      override protected val timeouts: ProcessingTimeout,
  ) extends DomainHandle
      with FlagCloseableAsync
      with NamedLogging {

    override val sequencerClient: SequencerClient = sequencer
    override def loggerFactory: NamedLoggerFactory = GrpcDomainRegistry.this.loggerFactory

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
      import TraceContext.Implicits.Empty.*
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

  def sequencerConnectClientBuilder: SequencerConnectClient.Builder = {
    (config: SequencerConnection) =>
      SequencerConnectClient(
        config,
        participantNodeParameters.processingTimeouts,
        participantNodeParameters.tracing.propagation,
        loggerFactory,
      )
  }

  private def extractSingleError(errors: Seq[DomainRegistryError])(implicit
      traceContext: TraceContext
  ): DomainRegistryError = {
    require(errors.nonEmpty, "Non-empty list of errors is expected")
    val nonEmptyResult = NonEmptyUtil.fromUnsafe(errors)
    if (nonEmptyResult.size == 1) nonEmptyResult.head1
    else {
      val message = nonEmptyResult.map(_.cause).mkString(",")
      DomainRegistryError.ConnectionErrors.FailedToConnectToSequencers.Error(message)
    }
  }

  private def aggregateBootstrapInfo(
      result: Seq[(SequencerAlias, (DomainClientBootstrapInfo, StaticDomainParameters))]
  )(implicit
      traceContext: TraceContext
  ): Either[DomainRegistryError, SequencerAggregatedInfo] = {
    require(result.nonEmpty, "Non-empty list of sequencerId-to-endpoint pair is expected")
    val nonEmptyResult = NonEmptyUtil.fromUnsafe(result)
    val infoResult = nonEmptyResult.map(_._2)
    val domainIds = infoResult.map(_._1).map(_.domainId).toSet
    val topologyRequestAddresses = infoResult.map(_._1).map(_.topologyRequestAddress).toSet
    val staticDomainParameters = infoResult.map(_._2).toSet
    val expectedSequencers = NonEmptyUtil.fromUnsafe(
      nonEmptyResult.groupBy(_._1).view.mapValues(_.map(_._2._1.sequencerId).head1).toMap
    )
    if (domainIds.sizeIs > 1) {
      DomainRegistryError.ConfigurationErrors.SequencersFromDifferentDomainsAreConfigured
        .Error(
          s"Non-unique domain ids received by connecting to sequencers: [${domainIds.mkString(",")}]"
        )
        .asLeft
    } else if (topologyRequestAddresses.sizeIs > 1) {
      DomainRegistryError.ConfigurationErrors.MisconfiguredTopologyRequestAddress
        .Error(
          s"Non-unique topologyRequestAddress received by connecting to sequencers: [${topologyRequestAddresses
              .mkString(",")}]"
        )
        .asLeft
    } else if (staticDomainParameters.sizeIs > 1) {
      DomainRegistryError.ConfigurationErrors.MisconfiguredStaticDomainParameters
        .Error(s"Non-unique static domain parameters received by connecting to sequencers")
        .asLeft
    } else
      SequencerAggregatedInfo(
        domainId = domainIds.head1,
        topologyRequestAddress = topologyRequestAddresses.head1,
        staticDomainParameters = staticDomainParameters.head1,
        expectedSequencers = expectedSequencers,
      ).asRight
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, (DomainClientBootstrapInfo, StaticDomainParameters)] = {
    for {
      bootstrapInfo <- client
        .getDomainClientBootstrapInfo(domainAlias)
        .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))

      _ <- performHandshake(client, domainAlias, sequencerAlias, bootstrapInfo.domainId)

      domainParameters <- client
        .getDomainParameters(domainAlias)
        .leftMap(DomainRegistryHelpers.toDomainRegistryError(domainAlias))
    } yield (bootstrapInfo, domainParameters)
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias
  )(connection: SequencerConnection)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[
    DomainRegistryError
  ], (SequencerAlias, (DomainClientBootstrapInfo, StaticDomainParameters))] =
    connection match {
      case grpc: GrpcSequencerConnection =>
        (for {
          client <- sequencerConnectClientBuilder(grpc).leftMap(
            DomainRegistryHelpers.toDomainRegistryError(domainAlias)
          )
          bootstrapInfoDomainParameters <- getBootstrapInfoDomainParameters(
            domainAlias,
            grpc.sequencerAlias,
            client,
          )
            .thereafter(_ => client.close())
        } yield connection.sequencerAlias -> bootstrapInfoDomainParameters).leftMap(x => Seq(x))
    }

  private def performHandshake(
      sequencerConnectClient: SequencerConnectClient,
      alias: DomainAlias,
      sequencerAlias: SequencerAlias,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, Unit] =
    for {
      success <- sequencerConnectClient
        .handshake(
          alias,
          HandshakeRequest(
            ProtocolVersionCompatibility.supportedProtocolsParticipant(participantNodeParameters),
            participantNodeParameters.protocolConfig.minimumProtocolVersion,
          ),
          participantNodeParameters.protocolConfig.dontWarnOnDeprecatedPV,
        )
        .leftMap(DomainRegistryHelpers.toDomainRegistryError(alias))
        .subflatMap {
          case success: HandshakeResponse.Success => success.asRight
          case HandshakeResponse.Failure(_, reason) =>
            DomainRegistryError.HandshakeErrors.HandshakeFailed.Error(reason).asLeft
        }
      _ <- aliasManager
        .processHandshake(alias, domainId)
        .leftMap(toDomainRegistryError)
    } yield {
      logger.info(
        s"Version handshake with sequencer ${sequencerAlias} and domain using protocol version ${success.serverProtocolVersion} succeeded."
      )
      ()
    }

  private def loadSequencerEndpoints(
      domainAlias: DomainAlias,
      sequencerConnections: SequencerConnections,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SequencerAggregatedInfo] =
    MonadUtil
      .parTraverseWithLimit(
        parallelism = sequencerInfoLoadParallelism.unwrap
      )(sequencerConnections.connections)(getBootstrapInfoDomainParameters(domainAlias))
      .leftMap(extractSingleError)
      .subflatMap(aggregateBootstrapInfo)

  override def connect(
      config: DomainConnectionConfig
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[DomainRegistryError, DomainHandle]] = {

    val sequencerConnections: SequencerConnections =
      config.sequencerConnections

    val agreementClient = new AgreementClient(
      agreementService,
      sequencerConnections,
      loggerFactory,
    )

    val runE = for {
      sequencerConnectClient <- sequencerConnectClientBuilder(sequencerConnections.default)
        // TODO(i12076): Multiple sequencers should be used rather than `default` one
        .leftMap(err =>
          DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(err.message)
        )
        .mapK(
          FutureUnlessShutdown.outcomeK
        )

      info <- loadSequencerEndpoints(config.domain, sequencerConnections).mapK(
        FutureUnlessShutdown.outcomeK
      )

      domainHandle <- getDomainHandle(
        config,
        sequencerConnections,
        syncDomainPersistentStateManager,
        info,
      )(
        cryptoApiProvider,
        cryptoConfig,
        clock,
        testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        topologyDispatcher,
        packageDependencies,
        metrics,
        agreementClient,
        sequencerConnectClient,
        participantSettings,
      ).thereafter(_ => sequencerConnectClient.close())
    } yield new GrpcDomainHandle(
      domainHandle.domainId,
      domainHandle.alias,
      domainHandle.staticParameters,
      domainHandle.sequencer,
      domainHandle.topologyClient,
      domainHandle.topologyFactory,
      domainHandle.topologyRequestAddress,
      domainHandle.domainPersistentState,
      domainHandle.timeouts,
    )

    runE.value
  }
}
