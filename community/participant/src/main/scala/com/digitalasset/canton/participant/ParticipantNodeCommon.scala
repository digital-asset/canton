// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics as LedgerApiServerMetrics
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, H2DbConfig}
import com.digitalasset.canton.crypto.{Crypto, SyncCryptoApiProvider}
import com.digitalasset.canton.domain.api.v0.DomainTimeServiceGrpc
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrapCommon}
import com.digitalasset.canton.health.HealthReporting.{
  BaseMutableHealthComponent,
  MutableHealthComponent,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.admin.{
  DomainConnectivityService,
  PackageInspectionOps,
  PackageService,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.domain.grpc.GrpcDomainRegistry
import com.digitalasset.canton.participant.domain.{
  AgreementService,
  DomainAliasManager,
  DomainAliasResolution,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.{
  IndexerLockIds,
  LedgerApiServerState,
}
import com.digitalasset.canton.participant.ledger.api.*
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.scheduler.ParticipantSchedulersParameters
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcherCommon,
  ParticipantTopologyManagerOps,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.scheduler.SchedulersWithPruning
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ReleaseProtocolVersion
import io.grpc.{BindableService, ServerServiceDefinition}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class CantonLedgerApiServerFactory(
    engine: Engine,
    clock: Clock,
    testingTimeService: TestingTimeService,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    meteringReportKey: MeteringReportKey,
    additionalGrpcServices: (
        CantonSyncService,
        Eval[ParticipantNodePersistentState],
    ) => List[BindableService] = (_, _) => Nil,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  def create(
      name: InstanceName,
      ledgerId: String,
      participantId: LedgerParticipantId,
      sync: CantonSyncService,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      config: LocalParticipantConfig,
      parameters: ParticipantNodeParameters,
      metrics: LedgerApiServerMetrics,
      tracerProvider: TracerProvider,
      adminToken: CantonAdminToken,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      traceContext: TraceContext,
      actorSystem: ActorSystem,
  ): EitherT[FutureUnlessShutdown, String, CantonLedgerApiServerWrapper.LedgerApiServerState] = {

    val ledgerTestingTimeService = (config.testingTime, clock) match {
      case (Some(TestingTimeServiceConfig.MonotonicTime), clock) =>
        Some(new CantonTimeServiceBackend(clock, testingTimeService, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, simClock: SimClock) =>
        Some(new CantonExternalClockBackend(simClock, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, remoteClock: RemoteClock) =>
        Some(new CantonExternalClockBackend(remoteClock, loggerFactory))
      case _ => None
    }

    for {
      // For participants with append-only schema enabled, we allocate lock IDs for the indexer
      indexerLockIds <-
        config.storage match {
          case _: H2DbConfig =>
            // For H2 the non-unique indexer lock ids are sufficient.
            logger.debug("Not allocating indexer lock IDs on H2 config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
          case dbConfig: DbConfig =>
            allocateIndexerLockIds(dbConfig)
              .leftMap { err =>
                s"Failed to allocated lock IDs for indexer: $err"
              }
              .toEitherT[FutureUnlessShutdown]
          case _ =>
            logger.debug("Not allocating indexer lock IDs on non-DB config")
            EitherT.rightT[FutureUnlessShutdown, String](None)
        }

      ledgerApiServer <- CantonLedgerApiServerWrapper
        .initialize(
          CantonLedgerApiServerWrapper.Config(
            config.ledgerApi,
            parameters.ledgerApiServerParameters.indexer,
            indexerLockIds,
            ledgerId,
            participantId,
            engine,
            sync,
            config.storage,
            parameters,
            ledgerTestingTimeService,
            adminToken,
            loggerFactory,
            tracerProvider,
            metrics,
            meteringReportKey,
          ),
          // start ledger API server iff participant replica is active
          startLedgerApiServer = sync.isActive(),
          createExternalServices =
            () => additionalGrpcServices(sync, participantNodePersistentState),
          futureSupervisor = futureSupervisor,
        )(executionContext, actorSystem)
        .leftMap { err =>
          // The MigrateOnEmptySchema exception is private, thus match on the expected message
          val errMsg =
            if (
              Option(err.cause).nonEmpty && err.cause.getMessage.contains("migrate-on-empty-schema")
            )
              s"${err.cause.getMessage} Please run `$name.db.migrate` to apply pending migrations"
            else s"$err"
          s"Ledger API server failed to start: $errMsg"
        }
    } yield ledgerApiServer
  }
}

/** custom components that differ between x-nodes and 2.x nodes */
private[this] trait ParticipantComponentBootstrapFactory {

  def createSyncDomainAndTopologyDispatcher(
      aliasResolution: DomainAliasResolution,
      indexedStringStore: IndexedStringStore,
  ): (SyncDomainPersistentStateManager, ParticipantTopologyDispatcherCommon)

  def createPackageInspectionOps(
      manager: SyncDomainPersistentStateManager,
      crypto: SyncCryptoApiProvider,
  ): PackageInspectionOps

}

trait ParticipantNodeBootstrapCommon {
  this: CantonNodeBootstrapCommon[
    _,
    LocalParticipantConfig,
    ParticipantNodeParameters,
    ParticipantMetrics,
  ] =>

  override def config: LocalParticipantConfig = arguments.config

  /** If set to `Some(path)`, every sequencer client will record all received events to the directory `path`.
    */
  protected val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]] =
    new AtomicReference(None)
  protected val replaySequencerConfig: AtomicReference[Option[ReplayConfig]] = new AtomicReference(
    None
  )

  lazy val syncDomainHealth: MutableHealthComponent = BaseMutableHealthComponent(
    loggerFactory,
    SyncDomain.healthName,
    timeouts,
  )
  lazy val syncDomainEphemeralHealth: MutableHealthComponent =
    BaseMutableHealthComponent(
      loggerFactory,
      SyncDomainEphemeralState.healthName,
      timeouts,
    )
  lazy val syncDomainSequencerClientHealth: MutableHealthComponent =
    BaseMutableHealthComponent(
      loggerFactory,
      SequencerClient.healthName,
      timeouts,
    )

  protected def setPostInitCallbacks(sync: CantonSyncService, packageService: PackageService): Unit

  protected def createParticipantServices(
      participantId: ParticipantId,
      crypto: Crypto,
      storage: Storage,
      persistentStateFactory: ParticipantNodePersistentStateFactory,
      engine: Engine,
      ledgerApiServerFactory: CantonLedgerApiServerFactory,
      indexedStringStore: IndexedStringStore,
      cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
      setStartableStoppableLedgerApiAndCantonServices: (
          StartableStoppableLedgerApiServer,
          StartableStoppableLedgerApiDependentServices,
      ) => Unit,
      resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
      replicationServiceFactory: Storage => ServerServiceDefinition,
      createSchedulers: ParticipantSchedulersParameters => Future[SchedulersWithPruning],
      createPartyNotifierAndSubscribe: ParticipantEventPublisher => LedgerServerPartyNotifier,
      adminToken: CantonAdminToken,
      topologyManager: ParticipantTopologyManagerOps,
      componentFactory: ParticipantComponentBootstrapFactory,
      skipRecipientsCheck: Boolean,
  )(implicit executionSequencerFactory: ExecutionSequencerFactory): EitherT[
    FutureUnlessShutdown,
    String,
    (
        LedgerServerPartyNotifier,
        CantonSyncService,
        ParticipantNodeEphemeralState,
        LedgerApiServerState,
        StartableStoppableLedgerApiDependentServices,
        SchedulersWithPruning,
        ParticipantTopologyDispatcherCommon,
    ),
  ] = {
    val syncCrypto = new SyncCryptoApiProvider(
      participantId,
      ips,
      crypto,
      config.caching,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    // closed in DomainAliasManager
    val registeredDomainsStore = RegisteredDomainsStore(storage, timeouts, loggerFactory)

    // closed in grpc domain registriy
    val agreementService = {
      // store is cleaned up as part of the agreement service
      val acceptedAgreements = ServiceAgreementStore(storage, timeouts, loggerFactory)
      new AgreementService(acceptedAgreements, parameterConfig, loggerFactory)
    }

    for {
      domainConnectionConfigStore <- EitherT
        .right(
          DomainConnectionConfigStore.create(
            storage,
            ReleaseProtocolVersion.latest,
            timeouts,
            loggerFactory,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      domainAliasManager <- EitherT
        .right[String](
          DomainAliasManager
            .create(domainConnectionConfigStore, registeredDomainsStore, loggerFactory)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      (syncDomainPersistentStateManager, topologyDispatcher) =
        componentFactory.createSyncDomainAndTopologyDispatcher(
          domainAliasManager,
          indexedStringStore,
        )

      persistentState <- EitherT.right(
        persistentStateFactory.create(
          syncDomainPersistentStateManager,
          storage,
          clock,
          config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
          config.init.parameters.uniqueContractKeys.some,
          parameterConfig.stores,
          ReleaseProtocolVersion.latest,
          arguments.metrics,
          indexedStringStore,
          parameterConfig.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
      )

      ephemeralState = ParticipantNodeEphemeralState(
        participantId,
        persistentState,
        clock,
        maxDeduplicationDuration = persistentState.map(
          _.settingsStore.settings.maxDeduplicationDuration
            .getOrElse(
              ErrorUtil.internalError(
                new RuntimeException("Max deduplication duration is not available")
              )
            )
        ),
        timeouts = parameterConfig.processingTimeouts,
        futureSupervisor,
        loggerFactory,
      )

      // Package Store and Management
      packageService = {
        val packageStore = storage match {
          case _: MemoryStorage =>
            new InMemoryDamlPackageStore(loggerFactory)
          case pool: DbStorage =>
            new DbDamlPackageStore(
              parameterConfig.stores.maxItemsInSqlClause,
              pool,
              parameterConfig.processingTimeouts,
              futureSupervisor,
              loggerFactory,
            )
        }
        // TODO(#11255) refactor package service such that we can break the cycle between manager / package service
        new PackageService(
          engine,
          packageStore,
          ephemeralState.participantEventPublisher,
          syncCrypto.pureCrypto,
          topologyManager,
          componentFactory.createPackageInspectionOps(syncDomainPersistentStateManager, syncCrypto),
          arguments.metrics,
          parameterConfig.processingTimeouts,
          loggerFactory,
        )
      }

      domainRegistry = new GrpcDomainRegistry(
        participantId,
        syncDomainPersistentStateManager,
        persistentState.map(_.settingsStore),
        agreementService,
        topologyDispatcher,
        domainAliasManager,
        syncCrypto,
        config.crypto,
        clock,
        parameterConfig,
        arguments.testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        packageId => packageService.packageDependencies(List(packageId)),
        arguments.metrics.domainMetrics,
        futureSupervisor,
        loggerFactory,
      )

      syncDomainEphemeralStateFactory = new SyncDomainEphemeralStateFactoryImpl(
        parameterConfig.processingTimeouts,
        arguments.testingConfig,
        loggerFactory,
        futureSupervisor,
      )

      partyNotifier <- EitherT
        .rightT[Future, String](
          createPartyNotifierAndSubscribe(ephemeralState.participantEventPublisher)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      // Initialize the SyncDomain persistent states before participant recovery so that pruning recovery can re-invoke
      // an interrupted prune after a shutdown or crash, which touches the domain stores.
      _ <- EitherT
        .right[String](
          syncDomainPersistentStateManager.initializePersistentStates()
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      ledgerId = participantId.uid.id.unwrap

      resourceManagementService = resourceManagementServiceFactory(
        persistentState.map(_.settingsStore)
      )

      schedulers <-
        EitherT
          .liftF(
            createSchedulers(
              ParticipantSchedulersParameters(
                isActive,
                persistentState.map(_.multiDomainEventLog),
                storage,
                adminToken,
                parameterConfig.stores,
              )
            )
          )
          .mapK(FutureUnlessShutdown.outcomeK)

      // Sync Service
      sync = cantonSyncServiceFactory.create(
        participantId,
        domainRegistry,
        domainConnectionConfigStore,
        domainAliasManager,
        persistentState,
        ephemeralState,
        syncDomainPersistentStateManager,
        packageService,
        topologyManager,
        topologyDispatcher,
        partyNotifier,
        syncCrypto,
        ledgerId,
        engine,
        syncDomainEphemeralStateFactory,
        storage,
        clock,
        resourceManagementService,
        parameterConfig,
        indexedStringStore,
        schedulers,
        arguments.metrics,
        arguments.futureSupervisor,
        loggerFactory,
        skipRecipientsCheck,
      )

      _ = {
        setPostInitCallbacks(sync, packageService)
        syncDomainHealth.set(sync.syncDomainHealth)
        syncDomainEphemeralHealth.set(sync.ephemeralHealth)
        syncDomainSequencerClientHealth.set(sync.sequencerClientHealth)
      }

      ledgerApiServer <- ledgerApiServerFactory
        .create(
          name,
          ledgerId = ledgerId,
          participantId = participantId.toLf,
          sync = sync,
          participantNodePersistentState = persistentState,
          arguments.config,
          arguments.parameterConfig,
          arguments.metrics.ledgerApiServer,
          tracerProvider,
          adminToken,
        )

    } yield {
      val ledgerApiDependentServices =
        new StartableStoppableLedgerApiDependentServices(
          config,
          parameterConfig,
          packageService,
          sync,
          participantId,
          syncCrypto.pureCrypto,
          clock,
          adminServerRegistry,
          adminToken,
          futureSupervisor,
          loggerFactory,
          tracerProvider,
        )

      setStartableStoppableLedgerApiAndCantonServices(
        ledgerApiServer.startableStoppableLedgerApi,
        ledgerApiDependentServices,
      )

      val stateService = new DomainConnectivityService(
        sync,
        domainAliasManager,
        agreementService,
        domainRegistry.sequencerConnectClientBuilder,
        parameterConfig.processingTimeouts,
        loggerFactory,
      )

      adminServerRegistry
        .addServiceU(
          PartyNameManagementServiceGrpc.bindService(
            new GrpcPartyNameManagementService(partyNotifier),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          DomainConnectivityServiceGrpc
            .bindService(new GrpcDomainConnectivityService(stateService), executionContext)
        )
      adminServerRegistry
        .addServiceU(
          TransferServiceGrpc.bindService(
            new GrpcTransferService(sync.transferService, participantId),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          InspectionServiceGrpc.bindService(
            new GrpcInspectionService(sync.stateInspection),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          ResourceManagementServiceGrpc.bindService(
            new GrpcResourceManagementService(resourceManagementService, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          DomainTimeServiceGrpc.bindService(
            GrpcDomainTimeService.forParticipant(sync.lookupDomainTimeTracker, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry.addServiceU(replicationServiceFactory(storage))
      adminServerRegistry
        .addServiceU(
          PruningServiceGrpc.bindService(
            new GrpcPruningService(sync, () => schedulers.getPruningScheduler, loggerFactory),
            executionContext,
          )
        )
      adminServerRegistry
        .addServiceU(
          ParticipantRepairServiceGrpc.bindService(
            new GrpcParticipantRepairService(
              sync,
              domainAliasManager,
              parameterConfig.processingTimeouts,
              loggerFactory,
            ),
            executionContext,
          )
        )
      // return values
      (
        partyNotifier,
        sync,
        ephemeralState,
        ledgerApiServer,
        ledgerApiDependentServices,
        schedulers,
        topologyDispatcher,
      )
    }
  }

}

abstract class ParticipantNodeCommon extends CantonNode with NamedLogging with HasUptime {}
