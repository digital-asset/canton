// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.Eval
import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.CantonRequireTypes
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.HealthReporting.ComponentStatus
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.health.{GrpcHealthReporter, HealthReporting}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.CommunityParticipantFactoryCommon
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.{
  PackageInspectionOps,
  PackageService,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config.{LocalParticipantConfig, PartyNotificationConfig}
import com.digitalasset.canton.participant.domain.DomainAliasResolution
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.scheduler.ParticipantSchedulersParameters
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  CantonSyncService,
  ParticipantEventPublisher,
  SyncDomainPersistentStateManager,
  SyncDomainPersistentStateManagerX,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.scheduler.SchedulersWithPruning
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ServerServiceDefinition

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class ParticipantNodeBootstrapX(
    arguments: CantonNodeBootstrapCommonArguments[
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ],
    engine: Engine,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    setStartableStoppableLedgerApiAndCantonServices: (
        StartableStoppableLedgerApiServer,
        StartableStoppableLedgerApiDependentServices,
    ) => Unit,
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithPruning] = _ =>
      Future.successful(SchedulersWithPruning.noop),
    private[canton] val persistentStateFactory: ParticipantNodePersistentStateFactory,
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    skipRecipientsCheck: Boolean,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapX[
      ParticipantNodeX,
      LocalParticipantConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments)
    with ParticipantNodeBootstrapCommon {

  // TODO(#12946) clean up to remove SingleUseCell
  private val cantonSyncService = new SingleUseCell[CantonSyncService]

  override protected def sequencedTopologyStores: Seq[TopologyStoreX[TopologyStoreId]] =
    cantonSyncService.get.toList.flatMap(
      _.syncDomainPersistentStateManager.getAll.values.map(_.topologyStore).collect {
        case s: TopologyStoreX[TopologyStoreId] => s
      }
    )
  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      manager: TopologyManagerX,
      healthReporter: GrpcHealthReporter,
      healthService: HealthReporting.HealthService,
  ): BootstrapStageOrLeaf[ParticipantNodeX] =
    new StartupNode(storage, crypto, nodeId, manager, healthReporter, healthService)
  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      nodeId: UniqueIdentifier,
      topologyManager: TopologyManagerX,
      healthReporter: GrpcHealthReporter,
      healthService: HealthReporting.HealthService,
  ) extends BootstrapStage[ParticipantNodeX, RunningNode[ParticipantNodeX]](
        description = "Startup participant node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    private val participantId = ParticipantId(nodeId)
    private val componentFactory = new ParticipantComponentBootstrapFactory {

      override def createSyncDomainAndTopologyDispatcher(
          aliasResolution: DomainAliasResolution,
          indexedStringStore: IndexedStringStore,
      ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcherCommon) = {
        val manager = new SyncDomainPersistentStateManagerX(
          aliasResolution,
          storage,
          indexedStringStore,
          parameters,
          crypto.pureCrypto,
          clock,
          futureSupervisor,
          loggerFactory,
        )

        val topologyDispatcher =
          new ParticipantTopologyDispatcherX(
            topologyManager,
            participantId,
            manager,
            crypto,
            clock,
            parameterConfig.processingTimeouts,
            loggerFactory,
          )

        (manager, topologyDispatcher)
      }

      override def createPackageInspectionOps(
          manager: SyncDomainPersistentStateManager,
          crypto: SyncCryptoApiProvider,
      ): PackageInspectionOps = new PackageInspectionOps() {
        // TODO(#11255) the package inspection ops requires some serious refactoring in order to remove the circular dependency
        //    however, this is just used for package removal which we can punt for the moment
        override def packageVetted(packageId: PackageId)(implicit
            tc: TraceContext
        ): EitherT[Future, PackageRemovalErrorCode.PackageVetted, Unit] = ???
        override def packageUnused(packageId: PackageId)(implicit
            tc: TraceContext
        ): EitherT[Future, PackageRemovalErrorCode.PackageInUse, Unit] = ???
        override def runTx(tx: TopologyTransaction[TopologyChangeOp], force: Boolean)(implicit
            tc: TraceContext
        ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = ???
        override def genRevokePackagesTx(packages: List[LfPackageId])(implicit
            tc: TraceContext
        ): EitherT[Future, ParticipantTopologyManagerError, TopologyTransaction[TopologyChangeOp]] =
          ???
        override protected def loggerFactory: NamedLoggerFactory = ???
      }
    }

    private val participantOps = new ParticipantTopologyManagerOps {
      override def vetPackages(packages: Seq[PackageId], synchronize: Boolean)(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
        // TODO(#11255) this vetting extension might fail on concurrent uploads of dars
        val currentMappingF = performUnlessClosingF(functionFullName)(
          topologyManager.store
            .findPositiveTransactions(
              asOf = CantonTimestamp.MaxValue,
              asOfInclusive = true,
              isProposal = false,
              types = Seq(VettedPackagesX.code),
              filterUid = Some(Seq(nodeId)),
              filterNamespace = None,
            )
            .map { result =>
              result
                .collectOfMapping[VettedPackagesX]
                .result
                .lastOption
            }
        )
        for {
          currentMapping <- EitherT.right(currentMappingF)
          currentPackages = currentMapping
            .map(_.transaction.transaction.mapping.packageIds)
            .getOrElse(Seq.empty)
          nextSerial = currentMapping.map(_.transaction.transaction.serial + PositiveInt.one)
          _ <- performUnlessClosingEitherUSF(functionFullName)(
            topologyManager
              .proposeAndAuthorize(
                TopologyChangeOpX.Replace,
                VettedPackagesX(
                  participantId = participantId,
                  domainId = None,
                  (currentPackages ++ packages).distinct,
                ),
                serial = nextSerial,
                // TODO(#11255) auto-determine signing keys
                signingKeys = Seq(participantId.uid.namespace.fingerprint),
                parameters.initialProtocolVersion,
                expectFullAuthorization = true,
              )
              .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
              .map(_ => ())
          )
        } yield ()
      }

      override def allocateParty(
          validatedSubmissionId: CantonRequireTypes.String255,
          partyId: PartyId,
          participantId: ParticipantId,
          protocolVersion: ProtocolVersion,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = {
        // TODO(#11255) make this "extend" / not replace
        //    this will also be potentially racy!
        performUnlessClosingEitherUSF(functionFullName)(
          topologyManager
            .proposeAndAuthorize(
              TopologyChangeOpX.Replace,
              PartyToParticipantX(
                partyId,
                None,
                threshold = 1,
                participants =
                  Seq(HostingParticipant(participantId, ParticipantPermissionX.Submission)),
                groupAddressing = false,
              ),
              serial = None,
              // TODO(#11255) auto-determine signing keys
              signingKeys = Seq(partyId.uid.namespace.fingerprint),
              protocolVersion,
              expectFullAuthorization = true,
            )
        )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
          .map(_ => ())
      }

    }

    override def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[ParticipantNodeX]]] = {
      val indexedStringStore =
        IndexedStringStore.create(
          storage,
          parameterConfig.cachingConfigs.indexedStrings,
          timeouts,
          loggerFactory,
        )
      addCloseable(indexedStringStore)
      val partyMetadataStore =
        PartyMetadataStore(storage, parameterConfig.processingTimeouts, loggerFactory)
      addCloseable(partyMetadataStore)
      val adminToken = CantonAdminToken.create(crypto.pureCrypto)
      // upstream party information update generator

      val partyNotifierFactory = (eventPublisher: ParticipantEventPublisher) => {
        val partyNotifier = new LedgerServerPartyNotifier(
          participantId,
          eventPublisher,
          partyMetadataStore,
          clock,
          arguments.futureSupervisor,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )
        // Notify at participant level if eager notification is configured, else rely on notification via domain.
        if (parameterConfig.partyChangeNotification == PartyNotificationConfig.Eager) {
          topologyManager.addObserver(partyNotifier.attachToIdentityManagerX())
        }
        partyNotifier
      }

      createParticipantServices(
        participantId,
        crypto,
        storage,
        persistentStateFactory,
        engine,
        ledgerApiServerFactory,
        indexedStringStore,
        cantonSyncServiceFactory,
        setStartableStoppableLedgerApiAndCantonServices,
        resourceManagementServiceFactory,
        replicationServiceFactory,
        createSchedulers,
        partyNotifierFactory,
        adminToken,
        participantOps,
        componentFactory,
        skipRecipientsCheck,
      ).map {
        case (
              partyNotifier,
              sync,
              ephemeralState,
              ledgerApiServer,
              ledgerApiDependentServices,
              schedulers,
              topologyDispatcher,
            ) =>
          if (cantonSyncService.putIfAbsent(sync).nonEmpty) {
            sys.error("should not happen")
          }
          addCloseable(partyNotifier)
          addCloseable(ephemeralState.participantEventPublisher)
          addCloseable(topologyDispatcher)
          addCloseable(schedulers)
          addCloseable(sync)
          addCloseable(ledgerApiServer)
          addCloseable(ledgerApiDependentServices)
          val node = new ParticipantNodeX(
            participantId,
            arguments.metrics,
            config,
            parameterConfig,
            storage,
            clock,
            crypto.pureCrypto,
            topologyDispatcher,
            ips,
            sync,
            adminToken,
            recordSequencerInteractions,
            replaySequencerConfig,
            schedulers,
            loggerFactory,
            healthService.dependencies.map(_.toComponentStatus),
          )
          addCloseable(node)
          Some(new RunningNode(bootstrapStageCallback, node))
      }
    }
  }

  override protected def member(uid: UniqueIdentifier): Member = ParticipantId(uid)
  override protected def mkNodeHealthService(storage: Storage): HealthReporting.HealthService =
    HealthReporting.HealthService(
      "participant",
      criticalDependencies = Seq(storage),
      // The sync service won't be reporting Ok until the node is initialized, but that shouldn't prevent traffic from
      // reaching the node
      Seq(syncDomainHealth, syncDomainEphemeralHealth, syncDomainSequencerClientHealth),
    )

  override protected def setPostInitCallbacks(
      sync: CantonSyncService,
      packageService: PackageService,
  ): Unit = {
    // TODO(#11255) implement me

  }
}

object ParticipantNodeBootstrapX {

  object CommunityParticipantFactory
      extends CommunityParticipantFactoryCommon[ParticipantNodeBootstrapX] {

    override protected def createNode(
        arguments: Arguments,
        engine: Engine,
        ledgerApiServerFactory: CantonLedgerApiServerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): ParticipantNodeBootstrapX =
      new ParticipantNodeBootstrapX(
        arguments,
        createEngine(arguments),
        CantonSyncService.DefaultFactory,
        (_ledgerApi, _ledgerApiDependentServices) => (),
        createResourceService(arguments),
        createReplicationServiceFactory(arguments),
        persistentStateFactory = ParticipantNodePersistentStateFactory,
        ledgerApiServerFactory = ledgerApiServerFactory,
        skipRecipientsCheck = true,
      )
  }
}

class ParticipantNodeX(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    val nodeParameters: ParticipantNodeParameters,
    storage: Storage,
    override protected val clock: Clock,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcherCommon,
    private[canton] val ips: IdentityProvidingServiceClient,
    private[canton] val sync: CantonSyncService,
    val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val schedulers: SchedulersWithPruning,
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends ParticipantNodeCommon {

  override def close(): Unit = () // closing is done in the bootstrap class

  def readyDomains: Map[DomainId, Boolean] =
    sync.readyDomains.values.toMap

  override def status: Future[ParticipantStatus] = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port)
    val domains = readyDomains
    val topologyQueues = identityPusher.queueStatus
    Future.successful(
      ParticipantStatus(
        id.uid,
        uptime(),
        ports,
        domains,
        sync.isActive(),
        topologyQueues,
        healthData,
      )
    )
  }

  override def isActive: Boolean = storage.isActive
}

object ParticipantNodeX {}
