// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import cats.Eval
import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.option.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoPureApi,
  SyncCryptoApiParticipantProvider,
  SynchronizerCrypto,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.health.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, CantonMutableHandlerRegistry}
import com.digitalasset.canton.participant.ParticipantNodeBootstrap.ParticipantServices
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.grpc.*
import com.digitalasset.canton.participant.config.*
import com.digitalasset.canton.participant.health.admin.ParticipantStatus
import com.digitalasset.canton.participant.ledger.api.{
  AcsCommitmentPublicationPostProcessor,
  LedgerApiIndexer,
  LedgerApiIndexerConfig,
  StartableStoppableLedgerApiDependentServices,
  StartableStoppableLedgerApiServer,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.submission.{
  CommandDeduplicatorImpl,
  InFlightSubmissionTracker,
}
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  PruningProcessor,
  SortedReconciliationIntervalsProviderFactory,
}
import com.digitalasset.canton.participant.scheduler.ParticipantPruningScheduler
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.Active
import com.digitalasset.canton.participant.sync.*
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer.SubmissionReady
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.participant.synchronizer.grpc.GrpcSynchronizerRegistry
import com.digitalasset.canton.participant.topology.*
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.*
import com.digitalasset.canton.scheduler.{Schedulers, SchedulersImpl}
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig, SequencerClient}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.*
import com.digitalasset.canton.time.admin.v30.SynchronizerTimeServiceGrpc
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.PSIdLookup
import com.digitalasset.canton.topology.client.{
  StoreBasedTopologySnapshot,
  SynchronizerTopologyClient,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, SynchronizerStore}
import com.digitalasset.canton.topology.store.{PartyMetadataStore, TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseProtocolVersion,
  ReleaseVersion,
}
import com.digitalasset.daml.lf.engine.Engine
import io.grpc.ServerServiceDefinition
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class ParticipantNodeBootstrap(
    arguments: CantonNodeBootstrapCommonArguments[
      ParticipantNodeConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ],
    engine: Engine,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    resourceManagementServiceFactory: Eval[ParticipantSettingsStore] => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    ledgerApiServerFactory: CantonLedgerApiServerFactory,
    setInitialized: ParticipantServices => Unit,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapImpl[
      ParticipantNode,
      ParticipantNodeConfig,
      ParticipantNodeParameters,
      ParticipantMetrics,
    ](arguments) {

  private val cantonSyncService = new SingleUseCell[CantonSyncService]
  private val packageDependencyResolver = new SingleUseCell[PackageDependencyResolver]
  override def metrics: ParticipantMetrics = arguments.metrics

  override protected val adminTokenConfig: Option[String] =
    config.ledgerApi.adminToken.orElse(config.adminApi.adminToken)

  private def tryGetPackageDependencyResolver(): PackageDependencyResolver =
    packageDependencyResolver.getOrElse(
      sys.error("packageDependencyResolver should be defined")
    )

  override protected def sequencedTopologyStores: Seq[TopologyStore[SynchronizerStore]] =
    cantonSyncService.get.toList
      .flatMap(_.syncPersistentStateManager.getAll.values)
      .map(_.topologyStore)

  override protected def sequencedTopologyManagers: Seq[SynchronizerTopologyManager] =
    cantonSyncService.get.toList
      .flatMap(_.syncPersistentStateManager.getAll.values)
      .map(_.topologyManager)

  override protected def lookupTopologyClient(
      storeId: TopologyStoreId
  ): Option[SynchronizerTopologyClient] =
    storeId match {
      case SynchronizerStore(synchronizerId) =>
        cantonSyncService.get.flatMap(_.lookupTopologyClient(synchronizerId))
      case _ => None
    }

  override protected lazy val lookupActivePSId: PSIdLookup =
    synchronizerId =>
      cantonSyncService.get.flatMap(
        _.activePSIdForLSId(synchronizerId)
      )

  override protected def customNodeStages(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      manager: AuthorizedTopologyManager,
      healthReporter: GrpcHealthReporter,
      healthService: DependenciesHealthService,
  ): BootstrapStageOrLeaf[ParticipantNode] =
    new StartupNode(
      storage,
      crypto,
      adminServerRegistry,
      adminToken,
      nodeId,
      manager,
      healthService,
    )

  override protected def createAuthorizedTopologyManager(
      nodeId: UniqueIdentifier,
      crypto: Crypto,
      authorizedStore: TopologyStore[AuthorizedStore],
      storage: Storage,
  ): AuthorizedTopologyManager = {
    val resolver = new PackageDependencyResolver(
      damlPackageStore = DamlPackageStore(
        storage,
        arguments.futureSupervisor,
        arguments.parameterConfig,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        loggerFactory,
      ),
      timeouts = arguments.parameterConfig.processingTimeouts,
      loggerFactory = loggerFactory,
      fetchPackageParallelism = arguments.parameterConfig.general.batchingConfig.parallelism,
      packageDependencyCacheConfig =
        arguments.parameterConfig.general.cachingConfigs.packageDependencyCache,
    )

    packageDependencyResolver.putIfAbsent(resolver).discard

    def acsInspectionPerSynchronizer(): Map[SynchronizerId, AcsInspection] =
      cantonSyncService.get
        .map(_.syncPersistentStateManager.getAllLogical.view.mapValues(_.acsInspection).toMap)
        .getOrElse(Map.empty)

    def reassignmentStore(): Map[SynchronizerId, ReassignmentStore] =
      cantonSyncService.get
        .map(_.syncPersistentStateManager.getAllLogical.view.mapValues(_.reassignmentStore).toMap)
        .getOrElse(Map.empty)

    def ledgerEnd(): FutureUnlessShutdown[Option[ParameterStorageBackend.LedgerEnd]] =
      cantonSyncService.get
        .traverse(_.ledgerApiIndexer.asEval.value.ledgerApiStore.value.ledgerEnd)
        .map(_.flatten)

    val topologyManager = new AuthorizedTopologyManager(
      nodeId,
      clock,
      crypto,
      authorizedStore,
      exitOnFatalFailures = parameters.exitOnFatalFailures,
      bootstrapStageCallback.timeouts,
      futureSupervisor,
      bootstrapStageCallback.loggerFactory,
    ) with ParticipantTopologyValidation {

      override def validatePackageVetting(
          currentlyVettedPackages: Set[LfPackageId],
          nextPackageIds: Set[LfPackageId],
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        validatePackageVetting(
          currentlyVettedPackages,
          nextPackageIds,
          resolver,
          acsInspections = () => acsInspectionPerSynchronizer(),
          forceFlags,
        )

      override def checkCannotDisablePartyWithActiveContracts(
          partyId: PartyId,
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        checkCannotDisablePartyWithActiveContracts(
          partyId,
          forceFlags,
          () => acsInspectionPerSynchronizer(),
        )

      override def checkInsufficientSignatoryAssigningParticipantsForParty(
          partyId: PartyId,
          currentThreshold: PositiveInt,
          nextThreshold: Option[PositiveInt],
          nextConfirmingParticipants: Seq[HostingParticipant],
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        checkInsufficientSignatoryAssigningParticipantsForParty(
          partyId,
          currentThreshold,
          nextThreshold,
          nextConfirmingParticipants,
          forceFlags,
          () => reassignmentStore(),
          () => ledgerEnd(),
        )

      override def checkInsufficientParticipantPermissionForSignatoryParty(
          party: PartyId,
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        checkInsufficientParticipantPermissionForSignatoryParty(
          party,
          forceFlags,
          () => acsInspectionPerSynchronizer(),
        )

    }
    topologyManager
  }

  private class StartupNode(
      storage: Storage,
      crypto: Crypto,
      adminServerRegistry: CantonMutableHandlerRegistry,
      adminToken: CantonAdminToken,
      nodeId: UniqueIdentifier,
      topologyManager: AuthorizedTopologyManager,
      healthService: DependenciesHealthService,
  ) extends BootstrapStage[ParticipantNode, RunningNode[ParticipantNode]](
        description = "Startup participant node",
        bootstrapStageCallback,
      )
      with HasCloseContext {

    override def getAdminToken: Option[String] = Some(adminToken.secret)
    private val participantId = ParticipantId(nodeId)

    override protected def attempt()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, Option[RunningNode[ParticipantNode]]] =
      createParticipantServices(
        participantId,
        crypto,
        adminServerRegistry,
        storage,
        engine,
        topologyManager,
        tryGetPackageDependencyResolver(),
      ).map { participantServices =>
        if (cantonSyncService.putIfAbsent(participantServices.cantonSyncService).nonEmpty) {
          sys.error("should not happen")
        }
        val node = new ParticipantNode(
          participantId,
          arguments.metrics,
          config,
          parameters,
          storage,
          clock,
          crypto.pureCrypto,
          participantServices.participantTopologyDispatcher,
          participantServices.cantonSyncService,
          adminToken,
          recordSequencerInteractions,
          replaySequencerConfig,
          loggerFactory,
          healthService.dependencies.map(_.toComponentStatus),
        )
        addCloseable(node)
        setInitialized(participantServices)
        Some(new RunningNode(bootstrapStageCallback, node))
      }

    private def createPackageOps(
        manager: SyncPersistentStateManager
    ): PackageOps = {
      val authorizedTopologyStoreClient = new StoreBasedTopologySnapshot(
        CantonTimestamp.MaxValue,
        topologyManager.store,
        tryGetPackageDependencyResolver(),
        loggerFactory,
      )
      val packageOps = new PackageOpsImpl(
        participantId = participantId,
        headAuthorizedTopologySnapshot = authorizedTopologyStoreClient,
        stateManager = manager,
        topologyManager = topologyManager,
        nodeId = nodeId,
        initialProtocolVersion = ProtocolVersion.latest,
        loggerFactory = ParticipantNodeBootstrap.this.loggerFactory,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
      )

      addCloseable(packageOps)
      packageOps
    }

    private def createParticipantServices(
        participantId: ParticipantId,
        crypto: Crypto,
        adminServerRegistry: CantonMutableHandlerRegistry,
        storage: Storage,
        engine: Engine,
        authorizedTopologyManager: AuthorizedTopologyManager,
        packageDependencyResolver: PackageDependencyResolver,
    )(implicit executionSequencerFactory: ExecutionSequencerFactory): EitherT[
      FutureUnlessShutdown,
      String,
      ParticipantServices,
    ] = {
      val syncCryptoSignerWithSessionKeys =
        new SyncCryptoApiParticipantProvider(
          participantId,
          ips,
          crypto,
          cryptoConfig,
          parameters.batchingConfig.parallelism,
          parameters.cachingConfigs.publicKeyConversionCache,
          timeouts,
          futureSupervisor,
          loggerFactory,
        )
      // closed in SynchronizerAliasManager
      val registeredSynchronizersStore =
        RegisteredSynchronizersStore(storage, timeouts, loggerFactory)
      val indexedStringStore = IndexedStringStore.create(
        storage,
        parameters.cachingConfigs.indexedStrings,
        timeouts,
        loggerFactory,
      )

      for {
        synchronizerAliasManager <- EitherT
          .right[String](
            SynchronizerAliasManager
              .create(
                registeredSynchronizersStore,
                loggerFactory,
              )
          )

        synchronizerConnectionConfigStore <- EitherT
          .right(
            SynchronizerConnectionConfigStore.create(
              storage,
              ReleaseProtocolVersion.latest,
              synchronizerAliasManager,
              timeouts,
              loggerFactory,
            )
          )

        persistentStateContainer = new LifeCycleContainer[ParticipantNodePersistentState](
          stateName = "persistent-state",
          create = () =>
            ParticipantNodePersistentState.create(
              storage,
              config.storage,
              config.init.ledgerApi.maxDeduplicationDuration.toInternal.some,
              parameters,
              ReleaseProtocolVersion.latest,
              arguments.metrics,
              participantId.toLf,
              config.ledgerApi,
              futureSupervisor,
              loggerFactory,
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right(persistentStateContainer.initializeNext())
        persistentState = persistentStateContainer.asEval

        syncPersistentStateManager = new SyncPersistentStateManager(
          participantId,
          synchronizerAliasManager,
          storage,
          indexedStringStore,
          persistentState.map(_.acsCounterParticipantConfigStore).value,
          parameters,
          (staticSynchronizerParameters: StaticSynchronizerParameters) =>
            SynchronizerCrypto(crypto, staticSynchronizerParameters),
          clock,
          tryGetPackageDependencyResolver(),
          persistentState.map(_.ledgerApiStore),
          persistentState.map(_.contractStore),
          futureSupervisor,
          loggerFactory,
        )

        topologyDispatcher = new ParticipantTopologyDispatcher(
          authorizedTopologyManager,
          participantId,
          syncPersistentStateManager,
          config.topology,
          crypto,
          clock,
          config,
          parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )

        commandDeduplicator = new CommandDeduplicatorImpl(
          persistentState.map(_.commandDeduplicationStore),
          clock,
          persistentState.map(
            _.ledgerApiStore
              .ledgerEndCache()
              .map(_.lastPublicationTime)
              .getOrElse(CantonTimestamp.MinValue)
          ),
          loggerFactory,
        )

        inFlightSubmissionTracker = new InFlightSubmissionTracker(
          persistentState.map(_.inFlightSubmissionStore),
          synchronizerConnectionConfigStore,
          commandDeduplicator,
          loggerFactory,
        )

        commandProgressTracker =
          if (parameters.commandProgressTracking.enabled)
            new CommandProgressTrackerImpl(parameters.commandProgressTracking, clock, loggerFactory)
          else CommandProgressTracker.NoOp

        connectedSynchronizersLookupContainer = new ConnectedSynchronizersLookupContainer
        sequentialPostProcessor = new AcsCommitmentPublicationPostProcessor(
          connectedSynchronizersLookupContainer,
          loggerFactory,
        )

        ledgerApiIndexerContainer = new LifeCycleContainer[LedgerApiIndexer](
          stateName = "indexer",
          create = () =>
            FutureUnlessShutdown.outcomeF(
              LedgerApiIndexer.initialize(
                metrics = arguments.metrics.ledgerApiServer,
                clock = clock,
                commandProgressTracker = commandProgressTracker,
                ledgerApiStore = persistentState.map(_.ledgerApiStore),
                ledgerApiIndexerConfig = LedgerApiIndexerConfig(
                  storageConfig = config.storage,
                  processingTimeout = parameters.processingTimeouts,
                  serverConfig = config.ledgerApi,
                  indexerConfig = config.parameters.ledgerApiServer.indexer,
                  indexerHaConfig = ledgerApiServerFactory.createHaConfig(config),
                  ledgerParticipantId = participantId.toLf,
                  onlyForTestingEnableInMemoryTransactionStore =
                    arguments.testingConfig.enableInMemoryTransactionStoreForParticipants,
                ),
                reassignmentOffsetPersistence = ReassignmentStore.reassignmentOffsetPersistenceFor(
                  syncPersistentStateManager
                ),
                postProcessor = inFlightSubmissionTracker
                  .processPublications(_)(_)
                  .failOnShutdownTo(
                    // This will be throw in the Indexer pekko-stream pipeline, and handled gracefully there
                    new RuntimeException("Post processing aborted due to shutdown")
                  ),
                sequentialPostProcessor = sequentialPostProcessor,
                loggerFactory = loggerFactory,
              )
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right {
          // only initialize indexer if storage is available
          if (storage.isActive) {
            ledgerApiIndexerContainer.initializeNext()
          } else {
            logger.info("Ledger API Indexer is not initialized due to inactive storage")
            FutureUnlessShutdown.unit
          }
        }

        ephemeralState = ParticipantNodeEphemeralState(
          ledgerApiIndexerContainer.asEval,
          inFlightSubmissionTracker,
          clock,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          timeouts = parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )

        packageServiceContainer = new LifeCycleContainer[PackageService](
          stateName = "package-service",
          create = () =>
            PackageService.createAndInitialize(
              clock = clock,
              engine = engine,
              packageDependencyResolver = packageDependencyResolver,
              enableUpgradeValidation = !parameters.disableUpgradeValidation,
              enableStrictDarValidation = parameters.enableStrictDarValidation,
              futureSupervisor = futureSupervisor,
              loggerFactory = loggerFactory,
              metrics = arguments.metrics,
              exitOnFatalFailures = parameters.exitOnFatalFailures,
              packageMetadataViewConfig = config.parameters.packageMetadataView,
              packageOps = createPackageOps(syncPersistentStateManager),
              timeouts = parameters.processingTimeouts,
            ),
          loggerFactory = loggerFactory,
        )
        _ <- EitherT.right(packageServiceContainer.initializeNext())

        sequencerInfoLoader = new SequencerInfoLoader(
          parameters.processingTimeouts,
          parameters.tracing.propagation,
          ProtocolVersionCompatibility.supportedProtocols(parameters),
          parameters.protocolConfig.minimumProtocolVersion,
          parameters.protocolConfig.dontWarnOnDeprecatedPV,
          loggerFactory,
        )

        partyMetadataStore =
          PartyMetadataStore(storage, parameters.processingTimeouts, loggerFactory)

        partyNotifier = new LedgerServerPartyNotifier(
          participantId,
          ephemeralState.participantEventPublisher,
          partyMetadataStore,
          clock,
          arguments.futureSupervisor,
          mustTrackSubmissionIds = true,
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          parameters.batchingConfig.maxItemsInBatch,
          parameters.processingTimeouts,
          loggerFactory,
        )

        synchronizerRegistry = new GrpcSynchronizerRegistry(
          participantId,
          syncPersistentStateManager,
          topologyDispatcher,
          syncCryptoSignerWithSessionKeys,
          config.crypto,
          clock,
          parameters,
          synchronizerAliasManager,
          arguments.testingConfig,
          recordSequencerInteractions,
          replaySequencerConfig,
          packageDependencyResolver,
          arguments.metrics.connectedSynchronizerMetrics,
          sequencerInfoLoader,
          partyNotifier,
          futureSupervisor,
          loggerFactory,
        )

        syncEphemeralStateFactory = new SyncEphemeralStateFactoryImpl(
          exitOnFatalFailures = parameters.exitOnFatalFailures,
          parameters.processingTimeouts,
          loggerFactory,
          futureSupervisor,
          clock,
        )

        // Initialize the ConnectedSynchronizer persistent states before participant recovery so that pruning recovery can re-invoke
        // an interrupted prune after a shutdown or crash, which touches the synchronizer stores.
        _ <- EitherT
          .right[String](
            syncPersistentStateManager.initializePersistentStates()
          )

        resourceManagementService = resourceManagementServiceFactory(
          persistentState.map(_.settingsStore)
        )

        pruningProcessor = new PruningProcessor(
          persistentState,
          syncPersistentStateManager,
          new SortedReconciliationIntervalsProviderFactory(
            syncPersistentStateManager,
            futureSupervisor,
            loggerFactory,
          ),
          parameters.batchingConfig.maxPruningBatchSize,
          arguments.metrics.pruning,
          exitOnFatalFailures = arguments.parameterConfig.exitOnFatalFailures,
          synchronizerConnectionConfigStore,
          parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
        pruningScheduler = new ParticipantPruningScheduler(
          pruningProcessor,
          arguments.clock,
          arguments.metrics,
          arguments.config.ledgerApi.clientConfig,
          persistentState,
          storage,
          adminToken,
          parameters.stores,
          arguments.parameterConfig.processingTimeouts,
          arguments.loggerFactory,
        )

        schedulers <-
          EitherT
            .liftF(
              {
                val schedulers =
                  new SchedulersImpl(
                    Map("pruning" -> pruningScheduler),
                    arguments.loggerFactory,
                  )
                if (isActive) {
                  schedulers.start().map(_ => schedulers)
                } else {
                  Future.successful(schedulers)
                }
              }
            )
            .mapK(FutureUnlessShutdown.outcomeK)

        /*
        Returns the topology manager corresponding to an active configuration. Restricting to active is fine since
        the topology manager is used for party allocation which would fail for inactive configurations.
         */
        activeTopologyManagerGetter = (id: PhysicalSynchronizerId) =>
          synchronizerConnectionConfigStore
            .get(id)
            .toOption
            .filter(_.status == Active)
            .flatMap(_.configuredPSId.toOption)
            .flatMap(syncPersistentStateManager.get)
            .map(_.topologyManager)

        // Sync Service
        sync = cantonSyncServiceFactory.create(
          participantId,
          synchronizerRegistry,
          synchronizerConnectionConfigStore,
          synchronizerAliasManager,
          persistentState,
          ephemeralState,
          syncPersistentStateManager,
          packageServiceContainer.asEval,
          new PartyOps(activeTopologyManagerGetter, loggerFactory),
          topologyDispatcher,
          partyNotifier,
          syncCryptoSignerWithSessionKeys,
          engine,
          commandProgressTracker,
          syncEphemeralStateFactory,
          storage,
          clock,
          resourceManagementService,
          parameters,
          pruningProcessor,
          schedulers,
          arguments.metrics,
          exitOnFatalFailures = arguments.parameterConfig.exitOnFatalFailures,
          sequencerInfoLoader,
          arguments.futureSupervisor,
          loggerFactory,
          arguments.testingConfig,
          ledgerApiIndexerContainer,
          connectedSynchronizersLookupContainer,
          () => triggerDeclarativeChange(),
        )

        _ = {
          connectedSynchronizerHealth.set(sync.connectedSynchronizerHealth)
          connectedSynchronizerEphemeralHealth.set(sync.ephemeralHealth)
          connectedSynchronizerSequencerClientHealth.set(sync.sequencerClientHealth)
          connectedSynchronizerAcsCommitmentProcessorHealth.set(sync.acsCommitmentProcessorHealth)
        }

        ledgerApiServer <- ledgerApiServerFactory
          .create(
            name,
            participantId = participantId.toLf,
            sync = sync,
            participantNodePersistentState = persistentState,
            ledgerApiIndexer = ledgerApiIndexerContainer.asEval,
            arguments.config,
            arguments.parameterConfig,
            arguments.metrics.ledgerApiServer,
            arguments.metrics.httpApiServer,
            tracerProvider,
            adminToken,
          )

      } yield {
        val ledgerApiDependentServices =
          new StartableStoppableLedgerApiDependentServices(
            config,
            parameters,
            packageServiceContainer.asEval,
            sync,
            participantId,
            clock,
            adminServerRegistry,
            adminToken,
            futureSupervisor,
            loggerFactory,
            tracerProvider,
          )

        adminServerRegistry
          .addServiceU(
            v30.TrafficControlServiceGrpc.bindService(
              new GrpcTrafficControlService(sync, loggerFactory),
              executionContext,
            )
          )
        adminServerRegistry
          .addServiceU(
            v30.SynchronizerConnectivityServiceGrpc
              .bindService(
                new GrpcSynchronizerConnectivityService(
                  sync,
                  synchronizerAliasManager,
                  parameters.processingTimeouts,
                  sequencerInfoLoader,
                  loggerFactory,
                ),
                executionContext,
              )
          )
        adminServerRegistry
          .addServiceU(
            v30.ParticipantInspectionServiceGrpc.bindService(
              new GrpcParticipantInspectionService(
                sync.stateInspection,
                ips,
                indexedStringStore,
                synchronizerAliasManager,
                loggerFactory,
              ),
              executionContext,
            )
          )
        adminServerRegistry
          .addServiceU(
            v30.ResourceManagementServiceGrpc.bindService(
              new GrpcResourceManagementService(resourceManagementService, loggerFactory),
              executionContext,
            )
          )
        adminServerRegistry
          .addServiceU(
            SynchronizerTimeServiceGrpc.bindService(
              GrpcSynchronizerTimeService
                .forParticipant(sync.lookupSynchronizerTimeTracker, loggerFactory),
              executionContext,
            )
          )
        adminServerRegistry.addServiceU(replicationServiceFactory(storage))
        adminServerRegistry
          .addServiceU(
            v30.PruningServiceGrpc.bindService(
              new GrpcPruningService(participantId, sync, pruningScheduler, loggerFactory),
              executionContext,
            )
          )
        adminServerRegistry
          .addServiceU(
            v30.ParticipantRepairServiceGrpc.bindService(
              new GrpcParticipantRepairService(
                sync,
                parameters,
                loggerFactory,
              ),
              executionContext,
            )
          )
        adminServerRegistry
          .addServiceU(
            ApiInfoServiceGrpc.bindService(
              new GrpcApiInfoService(CantonGrpcUtil.ApiName.AdminApi),
              executionContext,
            )
          )

        addCloseable(sync)
        addCloseable(synchronizerConnectionConfigStore)
        addCloseable(synchronizerAliasManager)
        addCloseable(syncPersistentStateManager)
        addCloseable(synchronizerRegistry)
        addCloseable(resourceManagementService)
        addCloseable(partyMetadataStore)
        persistentState.map(addCloseable).discard
        addCloseable((() => packageServiceContainer.closeCurrent()): AutoCloseable)
        addCloseable(indexedStringStore)
        addCloseable(partyNotifier)
        addCloseable(ephemeralState.participantEventPublisher)
        addCloseable(topologyDispatcher)
        addCloseable(schedulers)
        // TODO(#25118) clean up cache metrics on shutdown wherever they are initialized
        addCloseable(new AutoCloseable {
          override def close(): Unit =
            Seq(
              metrics.ledgerApiServer.execution.cache.keyState.stateCache,
              metrics.ledgerApiServer.execution.cache.contractState.stateCache,
              metrics.ledgerApiServer.identityProviderConfigStore.verifierCache,
              metrics.ledgerApiServer.identityProviderConfigStore.idpConfigCache,
              metrics.ledgerApiServer.userManagement.cache,
            ).foreach(_.closeAcquired())
        })
        addCloseable(ledgerApiServer)
        addCloseable(ledgerApiDependentServices)
        addCloseable(packageDependencyResolver)

        // return values
        ParticipantServices(
          persistentStateContainer = persistentStateContainer,
          packageServiceContainer = packageServiceContainer,
          ledgerApiIndexerContainer = ledgerApiIndexerContainer,
          cantonSyncService = sync,
          schedulers = schedulers,
          startableStoppableLedgerApiServer = ledgerApiServer.startableStoppableLedgerApi,
          startableStoppableLedgerApiDependentServices = ledgerApiDependentServices,
          participantTopologyDispatcher = topologyDispatcher,
        )
      }
    }
  }

  override protected def member(uid: UniqueIdentifier): Member = ParticipantId(uid)

  override protected def mkNodeHealthService(
      storage: Storage
  ): (DependenciesHealthService, LivenessHealthService) = {
    val readiness = DependenciesHealthService(
      "participant",
      logger,
      timeouts,
      criticalDependencies = storage +: crypto.toList,
      // The sync service won't be reporting Ok until the node is initialized, but that shouldn't prevent traffic from
      // reaching the node
      Seq(
        connectedSynchronizerHealth,
        connectedSynchronizerEphemeralHealth,
        connectedSynchronizerSequencerClientHealth,
        connectedSynchronizerAcsCommitmentProcessorHealth,
      ),
    )
    val liveness = LivenessHealthService.alwaysAlive(logger, timeouts)
    (readiness, liveness)
  }

  override protected def bindNodeStatusService(): ServerServiceDefinition =
    v30.ParticipantStatusServiceGrpc.bindService(
      new GrpcParticipantStatusService(getNodeStatus, loggerFactory),
      executionContext,
    )

  override def config: ParticipantNodeConfig = arguments.config

  /** If set to `Some(path)`, every sequencer client will record all received events to the
    * directory `path`.
    */
  protected val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]] =
    new AtomicReference(None)
  protected val replaySequencerConfig: AtomicReference[Option[ReplayConfig]] = new AtomicReference(
    None
  )

  lazy val connectedSynchronizerHealth: MutableHealthComponent = MutableHealthComponent(
    loggerFactory,
    ConnectedSynchronizer.healthName,
    timeouts,
  )
  private lazy val connectedSynchronizerEphemeralHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      SyncEphemeralState.healthName,
      timeouts,
    )
  private lazy val connectedSynchronizerSequencerClientHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      SequencerClient.healthName,
      timeouts,
    )

  private lazy val connectedSynchronizerAcsCommitmentProcessorHealth: MutableHealthComponent =
    MutableHealthComponent(
      loggerFactory,
      AcsCommitmentProcessor.healthName,
      timeouts,
    )
}

object ParticipantNodeBootstrap {
  val LoggerFactoryKeyName: String = "participant"

  final case class ParticipantServices(
      persistentStateContainer: LifeCycleContainer[ParticipantNodePersistentState],
      packageServiceContainer: LifeCycleContainer[PackageService],
      ledgerApiIndexerContainer: LifeCycleContainer[LedgerApiIndexer],
      cantonSyncService: CantonSyncService,
      schedulers: Schedulers,
      startableStoppableLedgerApiServer: StartableStoppableLedgerApiServer,
      startableStoppableLedgerApiDependentServices: StartableStoppableLedgerApiDependentServices,
      participantTopologyDispatcher: ParticipantTopologyDispatcher,
  )
}

class ParticipantNode(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: ParticipantNodeConfig,
    nodeParameters: ParticipantNodeParameters,
    val storage: Storage,
    override protected val clock: Clock,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcher,
    private[canton] val sync: CantonSyncService,
    override val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val loggerFactory: NamedLoggerFactory,
    healthData: => Seq[ComponentStatus],
) extends CantonNode
    with NamedLogging
    with HasUptime {

  override type Status = ParticipantStatus

  override def close(): Unit = () // closing is done in the bootstrap class

  def readySynchronizers: Map[PhysicalSynchronizerId, SubmissionReady] =
    sync.readySynchronizers.values.toMap

  private def supportedProtocolVersions: Seq[ProtocolVersion] = {
    val supportedPvs = ProtocolVersionCompatibility.supportedProtocols(nodeParameters)
    nodeParameters.protocolConfig.minimumProtocolVersion match {
      case Some(pv) => supportedPvs.filter(p => p >= pv)
      case None => supportedPvs
    }
  }

  override def status: ParticipantStatus = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port) ++
      config.httpLedgerApi.flatMap(_.server.port).flatMap(Port.create(_).toOption).map("json" -> _)
    val synchronizers = readySynchronizers
    val topologyQueues = identityPusher.queueStatus

    ParticipantStatus(
      id.uid,
      uptime(),
      ports,
      synchronizers,
      sync.isActive(),
      topologyQueues,
      healthData,
      version = ReleaseVersion.current,
      supportedProtocolVersions = supportedProtocolVersions,
    )

  }

  override def isActive: Boolean = storage.isActive

  /** @param isTriggeredManually
    *   True if the call of this method is triggered by an explicit call to the connectivity
    *   service, false if the call of this method is triggered by a node restart or transition to
    *   active
    */
  def reconnectSynchronizersIgnoreFailures(isTriggeredManually: Boolean)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SyncServiceError, Unit] =
    if (sync.isActive())
      sync
        .reconnectSynchronizers(
          ignoreFailures = true,
          isTriggeredManually = isTriggeredManually,
          mustBeActive = true,
        )
        .map(_ => ())
    else {
      logger.info("Not reconnecting to synchronizers as instance is passive")
      EitherTUtil.unitUS
    }

}
