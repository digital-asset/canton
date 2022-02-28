// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functorFilter._
import cats.syntax.option._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.lf.CantonOnly
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.InstanceName
import com.digitalasset.canton.config.RequireTypes.String185
import com.digitalasset.canton.config.{DbConfig, H2DbConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.{CryptoPureApi, KeyName, SyncCryptoApiProvider}
import com.digitalasset.canton.domain.api.v0.DomainTimeServiceGrpc
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrapBase}
import com.digitalasset.canton.health.admin.data.ParticipantStatus
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.participant.admin.grpc._
import com.digitalasset.canton.participant.admin.v0._
import com.digitalasset.canton.participant.admin.{
  DomainConnectivityService,
  PackageInspectionOpsImpl,
  PackageService,
  ResourceManagementService,
}
import com.digitalasset.canton.participant.config._
import com.digitalasset.canton.participant.domain.grpc.{GrpcDomainRegistry, GrpcDomainServiceClient}
import com.digitalasset.canton.participant.domain.{
  AgreementService,
  DomainAliasManager,
  DomainRegistryError,
  SequencerConnectClient,
  DomainConnectionConfig => CantonDomainConnectionConfig,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.participant.ledger.api._
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.store.db.{DbDamlPackageStore, DbServiceAgreementStore}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryDamlPackageStore,
  InMemoryServiceAgreementStore,
}
import com.digitalasset.canton.participant.sync.{
  CantonSyncService,
  ParticipantEventPublisher,
  SyncDomainPersistentStateManager,
  SyncServiceError,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManager.PostInitCallbacks
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyDispatcher,
  ParticipantTopologyManager,
}
import com.digitalasset.canton.resource._
import com.digitalasset.canton.sequencing.client.{RecordingConfig, ReplayConfig}
import com.digitalasset.canton.time._
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.admin.grpc.{
  GrpcTopologyAggregationService,
  GrpcTopologyManagerReadService,
  GrpcTopologyManagerWriteService,
}
import com.digitalasset.canton.topology.admin.v0.{
  TopologyAggregationServiceGrpc,
  TopologyManagerReadServiceGrpc,
  TopologyManagerWriteServiceGrpc,
}
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.{DomainId, LedgerParticipantId}
import io.grpc.ServerServiceDefinition

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class ParticipantNodeBootstrap(
    override val name: InstanceName,
    val config: LocalParticipantConfig,
    val cantonParameterConfig: ParticipantNodeParameters,
    val testingConfig: TestingConfigInternal,
    clock: Clock,
    engine: Engine,
    testingTimeService: TestingTimeService,
    cantonSyncServiceFactory: CantonSyncService.Factory[CantonSyncService],
    metrics: ParticipantMetrics,
    storageFactory: StorageFactory,
    setStartableStoppableIndexer: StartableStoppableIndexer => Unit,
    resourceManagementServiceFactory: ParticipantSettingsStore => ResourceManagementService,
    replicationServiceFactory: Storage => ServerServiceDefinition,
    allocateIndexerLockIds: DbConfig => Either[String, Option[IndexerLockIds]],
    isReplicated: Boolean,
    futureSupervisor: FutureSupervisor,
    parentLogger: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    scheduler: ScheduledExecutorService,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends CantonNodeBootstrapBase[
      ParticipantNode,
      LocalParticipantConfig,
      ParticipantNodeParameters,
    ](
      name,
      config,
      cantonParameterConfig,
      clock,
      metrics,
      storageFactory,
      parentLogger.append(ParticipantNodeBootstrap.LoggerFactoryKeyName, name.unwrap),
    ) {

  /** per session created admin token for in-process connections to ledger-api */
  val adminToken: CantonAdminToken = CantonAdminToken.create()

  // This absolutely must be a "def" as the superclass will use it during initialization.
  override protected def connectionPoolForParticipant: Boolean = true

  /** If set to `Some(path)`, every sequencer client will record all received events to the directory `path`.
    */
  private val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]] =
    new AtomicReference(None)

  private val replaySequencerConfig: AtomicReference[Option[ReplayConfig]] = new AtomicReference(
    None
  )

  private val ips = new IdentityProvidingServiceClient()

  private val topologyManager =
    new ParticipantTopologyManager(
      clock,
      topologyStoreFactory.forId(AuthorizedStore),
      crypto,
      cantonParameterConfig.processingTimeouts,
      loggerFactory,
    )

  // add participant node topology manager
  adminServerRegistry.addService(
    TopologyManagerWriteServiceGrpc
      .bindService(
        new GrpcTopologyManagerWriteService(
          topologyManager,
          topologyManager.store,
          crypto.cryptoPublicStore,
          loggerFactory,
        ),
        executionContext,
      )
  )
  adminServerRegistry.addService(
    TopologyManagerReadServiceGrpc
      .bindService(
        new GrpcTopologyManagerReadService(
          topologyStoreFactory.allNonDiscriminated,
          ips,
          loggerFactory,
        ),
        executionContext,
      )
  )

  private def createAndStartLedgerApiServer(
      ledgerId: String,
      participantId: LedgerParticipantId,
      sync: CantonSyncService,
  ): EitherT[Future, String, CantonLedgerApiServerWrapper.LedgerApiServerState] = {

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
        storageFactory.config match {
          case _: H2DbConfig =>
            // For H2 the non-unique indexer lock ids are sufficient.
            logger.debug("Not allocating indexer lock IDs on H2 config")
            EitherT.rightT[Future, String](None)
          case dbConfig: DbConfig =>
            allocateIndexerLockIds(dbConfig)
              .leftMap { err =>
                s"Failed to allocated lock IDs for indexer: $err"
              }
              .toEitherT[Future]
          case _ =>
            logger.debug("Not allocating indexer lock IDs on non-DB config")
            EitherT.rightT[Future, String](None)
        }

      ledgerApiServer <- CantonLedgerApiServerWrapper
        .start(
          CantonLedgerApiServerWrapper.Config(
            config.ledgerApi,
            cantonParameterConfig.indexer,
            indexerLockIds,
            ledgerId,
            participantId,
            engine,
            sync,
            config.storage,
            cantonParameterConfig,
            ledgerTestingTimeService,
            adminToken,
            loggerFactory,
            tracerProvider,
            metrics.ledgerApiServer,
          ),
          // start indexer iff participant replica is active
          startIndexer = sync.isActive(),
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

  override protected def autoInitializeIdentity(): EitherT[Future, String, Unit] =
    withNewTraceContext { implicit traceContext =>
      for {
        // create keys
        nsName <- EitherT.fromEither[Future](KeyName.create(s"$name-identity"))
        namespaceKey <- crypto
          .generateSigningKey(name = Some(nsName))
          .leftMap(err => s"Failed to generate key for namespace: $err")
        sgName <- EitherT.fromEither[Future](KeyName.create(s"$name-signing"))
        signingKey <- crypto
          .generateSigningKey(name = Some(sgName))
          .leftMap(err => s"Failed to generate key for signing: $err")
        encryptName <- EitherT.fromEither[Future](KeyName.create(s"$name-encryption"))
        encryptionKey <- crypto
          .generateEncryptionKey(name = Some(encryptName))
          .leftMap(err => s"Failed to generate key for encryption: $err")

        // init id
        identifier <- EitherT
          .fromEither[Future](Identifier.create(name.unwrap))
          .leftMap(err => s"Failed to convert participant name to identifier: $err")
        uid = UniqueIdentifier(
          identifier,
          Namespace(namespaceKey.fingerprint),
        )
        nodeId = NodeId(uid)
        _ <- storeId(nodeId)

        // init topology manager
        participantId = ParticipantId(uid)
        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          NamespaceDelegation(
            Namespace(namespaceKey.fingerprint),
            namespaceKey,
            isRootDelegation = true,
          ),
        )
        // avoid a race condition with admin-workflows and only kick off the start once the namespace certificate is registered
        _ <- initialize(nodeId)

        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          OwnerToKeyMapping(participantId, signingKey),
        )
        _ <- authorizeStateUpdate(
          topologyManager,
          namespaceKey,
          OwnerToKeyMapping(participantId, encryptionKey),
        )

        // initialize certificate
        _ <- new LegalIdentityInit(certificateGenerator, crypto)
          .initializeCertificate(uid, Seq(participantId), namespaceKey)(topologyManager)
      } yield ()
    }

  override def initialize(id: NodeId): EitherT[Future, String, Unit] = {
    ErrorUtil.requireState(!isInitialized, "Participant already initialized.")

    val participantId = ParticipantId(id.identity)
    topologyManager.setParticipantId(participantId)
    val ledgerApiParticipantId = participantId.toLf

    val identityPusher =
      new ParticipantTopologyDispatcher(
        topologyManager,
        cantonParameterConfig.processingTimeouts,
        loggerFactory,
      )

    // Crypto and Identity management

    val syncCrypto =
      new SyncCryptoApiProvider(participantId, ips, crypto, config.caching, loggerFactory)

    val registeredDomainsStore = RegisteredDomainsStore(storage, loggerFactory)

    val acceptedAgreements = storage match {
      case dbStorage: DbStorage => new DbServiceAgreementStore(dbStorage, loggerFactory)
      case _: MemoryStorage => new InMemoryServiceAgreementStore(loggerFactory)
    }

    val domainServiceClient =
      new GrpcDomainServiceClient(cantonParameterConfig.tracing.propagation, loggerFactory)

    val agreementService =
      new AgreementService(acceptedAgreements, domainServiceClient, loggerFactory)

    for {
      domainConnectionConfigStore <- EitherT.right(
        DomainConnectionConfigStore(storage, loggerFactory)
      )
      domainAliasManager <- EitherT.right[String](
        DomainAliasManager(registeredDomainsStore, loggerFactory)
      )
      syncDomainPersistentStateManager = new SyncDomainPersistentStateManager(
        domainAliasManager,
        loggerFactory,
      )

      persistentState <- EitherT.right(
        ParticipantNodePersistentState(
          syncDomainPersistentStateManager,
          storage,
          clock,
          config.ledgerApi.maxDeduplicationDuration.some,
          cantonParameterConfig.uniqueContractKeys.some,
          cantonParameterConfig.stores,
          metrics,
          indexedStringStore,
          cantonParameterConfig.processingTimeouts,
          loggerFactory,
        )
      )

      ephemeralState = ParticipantNodeEphemeralState(
        participantId,
        persistentState,
        clock,
        maxDeduplicationDuration = persistentState.settingsStore.settings.maxDeduplicationDuration
          .getOrElse(
            ErrorUtil.internalError(
              new RuntimeException("Max deduplication duration is not available")
            )
          ),
        timeouts = cantonParameterConfig.processingTimeouts,
        loggerFactory,
      )

      // Package Store and Management
      packageService = {
        val (packageStore) = storage match {
          case _: MemoryStorage =>
            new InMemoryDamlPackageStore(loggerFactory)
          case pool: DbStorage =>
            new DbDamlPackageStore(
              cantonParameterConfig.stores.maxItemsInSqlClause,
              pool,
              cantonParameterConfig.processingTimeouts,
              loggerFactory,
            )
        }

        new PackageService(
          engine,
          packageStore,
          ephemeralState.participantEventPublisher,
          syncCrypto.pureCrypto,
          _.withTraceContext { implicit traceContext =>
            { case (pkgs, vetting) =>
              topologyManager.vetPackages(pkgs, vetting)
            }
          },
          new PackageInspectionOpsImpl(
            participantId,
            clock,
            storage,
            domainAliasManager,
            syncDomainPersistentStateManager,
            syncCrypto,
            cantonParameterConfig.processingTimeouts,
            loggerFactory,
          ),
          cantonParameterConfig.processingTimeouts,
          loggerFactory,
        )
      }

      domainRegistry = new GrpcDomainRegistry(
        participantId,
        id,
        agreementService,
        identityPusher,
        domainAliasManager,
        syncCrypto,
        config.crypto,
        topologyStoreFactory,
        clock,
        cantonParameterConfig,
        testingConfig,
        recordSequencerInteractions,
        replaySequencerConfig,
        (domainId, traceContext) =>
          topologyManager.issueParticipantDomainStateCert(participantId, domainId)(traceContext),
        packageId => packageService.packageDependencies(List(packageId)),
        metrics.domainMetrics,
        futureSupervisor,
        loggerFactory,
      )

      syncDomainEphemeralStateFactory = new SyncDomainEphemeralStateFactoryImpl(
        cantonParameterConfig.processingTimeouts,
        testingConfig,
        cantonParameterConfig.enableCausalityTracking,
        loggerFactory,
      )

      // upstream party information update generator
      partyNotifier = new LedgerServerPartyNotifier(
        participantId,
        ephemeralState.participantEventPublisher,
        topologyStoreFactory.partyMetadataStore(),
        clock,
        cantonParameterConfig.processingTimeouts,
        loggerFactory,
      )

      // Notify at participant level if eager notification is configured, else rely on notification via domain.
      _ = if (cantonParameterConfig.partyChangeNotification == PartyNotificationConfig.Eager) {
        topologyManager.addObserver(partyNotifier.attachToIdentityManager())
      }

      // Initialize the SyncDomain persistent states before participant recovery so that pruning recovery can re-invoke
      // an interrupted prune after a shutdown or crash, which touches the domain stores.
      syncDomainPersistentStateFactory = new SyncDomainPersistentStateFactory(
        syncDomainPersistentStateManager,
        persistentState.settingsStore,
        storage,
        crypto.pureCrypto,
        indexedStringStore,
        cantonParameterConfig,
        loggerFactory,
      )
      _ <- EitherT.right[String](
        syncDomainPersistentStateFactory.initializePersistentStates(domainAliasManager)
      )

      multiDomainCausalityStore <- EitherT.liftF(
        MultiDomainCausalityStore(storage, indexedStringStore, loggerFactory)
      )

      ledgerId = participantId.uid.id.unwrap

      resourceManagementService = resourceManagementServiceFactory(persistentState.settingsStore)

      // Sync Service
      sync = cantonSyncServiceFactory.create(
        participantId,
        domainRegistry,
        domainConnectionConfigStore,
        domainAliasManager,
        persistentState,
        ephemeralState,
        syncDomainPersistentStateManager,
        syncDomainPersistentStateFactory,
        packageService,
        topologyStoreFactory,
        multiDomainCausalityStore,
        topologyManager,
        identityPusher,
        partyNotifier,
        syncCrypto,
        ledgerId,
        engine,
        syncDomainEphemeralStateFactory,
        storage,
        clock,
        resourceManagementService,
        cantonParameterConfig,
        indexedStringStore,
        metrics,
        futureSupervisor,
        loggerFactory,
      )

      // provide the idm a handle to synchronize package vettings
      _ = {

        topologyManager.setPostInitCallbacks(new PostInitCallbacks {

          override def clients(): Seq[DomainTopologyClient] =
            sync.readyDomains.values.toList.mapFilter { case (domainId, _) =>
              ips.forDomain(domainId)
            }

          override def packageDependencies(packages: List[PackageId])(implicit
              traceContext: TraceContext
          ): EitherT[Future, PackageId, Set[PackageId]] =
            packageService.packageDependencies(packages)
        })
      }

      ledgerApiServer <- createAndStartLedgerApiServer(
        ledgerId = ledgerId,
        participantId = ledgerApiParticipantId,
        sync = sync,
      )
    } yield {
      logger.debug("Starting ledger-api dependent canton services")
      val ledgerApiDependentServices =
        new LedgerApiDependentCantonServices(
          config,
          cantonParameterConfig,
          packageService,
          sync,
          participantId,
          syncCrypto.pureCrypto,
          clock,
          adminServerRegistry,
          adminToken,
          loggerFactory,
          tracerProvider,
        )
      setStartableStoppableIndexer(ledgerApiServer.indexer)

      val sequencerConnectClientBuilder: domain.DomainConnectionConfig => EitherT[
        Future,
        DomainRegistryError,
        SequencerConnectClient,
      ] =
        SequencerConnectClient(
          _,
          sync.syncCrypto.crypto,
          cantonParameterConfig.processingTimeouts,
          cantonParameterConfig.tracing.propagation,
          loggerFactory,
        )

      val stateService = new DomainConnectivityService(
        sync,
        domainAliasManager,
        agreementService,
        sequencerConnectClientBuilder,
        cantonParameterConfig.processingTimeouts,
        loggerFactory,
      )
      adminServerRegistry.addService(
        PartyNameManagementServiceGrpc.bindService(
          new GrpcPartyNameManagementService(partyNotifier),
          executionContext,
        )
      )
      adminServerRegistry.addService(
        DomainConnectivityServiceGrpc
          .bindService(new GrpcDomainConnectivityService(stateService), executionContext)
      )
      adminServerRegistry.addService(
        TopologyAggregationServiceGrpc
          .bindService(
            new GrpcTopologyAggregationService(
              topologyStoreFactory.allNonDiscriminated,
              ips,
              loggerFactory,
            ),
            executionContext,
          )
      )
      adminServerRegistry.addService(
        TransferServiceGrpc.bindService(
          new GrpcTransferService(sync.transferService),
          executionContext,
        )
      )
      adminServerRegistry.addService(
        InspectionServiceGrpc.bindService(
          new GrpcInspectionService(sync.stateInspection),
          executionContext,
        )
      )
      adminServerRegistry.addService(
        ResourceManagementServiceGrpc.bindService(
          new GrpcResourceManagementService(resourceManagementService),
          executionContext,
        )
      )
      adminServerRegistry.addService(
        DomainTimeServiceGrpc.bindService(
          GrpcDomainTimeService.forParticipant(sync.lookupDomainTimeTracker, loggerFactory),
          executionContext,
        )
      )
      adminServerRegistry.addService(replicationServiceFactory(storage))
      adminServerRegistry.addService(
        PruningServiceGrpc.bindService(
          new GrpcPruningService(sync, loggerFactory),
          executionContext,
        )
      )

      val participantNode = new ParticipantNode(
        participantId,
        metrics,
        config,
        cantonParameterConfig,
        storage,
        clock,
        topologyManager,
        crypto.pureCrypto,
        identityPusher,
        partyNotifier,
        ips,
        sync,
        ephemeralState.participantEventPublisher,
        ledgerApiServer,
        ledgerApiDependentServices,
        adminToken,
        recordSequencerInteractions,
        replaySequencerConfig,
        loggerFactory,
      )
      setInstance(participantNode)
    }
  }

  override def isActive: Boolean = storage.isActive
}

object ParticipantNodeBootstrap {
  val LoggerFactoryKeyName: String = "participant"

  trait Factory[PC <: LocalParticipantConfig] {
    def create(
        name: String,
        participantConfig: PC,
        participantNodeParameters: ParticipantNodeParameters,
        clock: Clock,
        testingTimeService: TestingTimeService,
        participantMetrics: ParticipantMetrics,
        testingConfig: TestingConfigInternal,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, ParticipantNodeBootstrap]
  }

  object CommunityParticipantFactory extends Factory[CommunityParticipantConfig] {

    override def create(
        name: String,
        participantConfig: CommunityParticipantConfig,
        participantNodeParameters: ParticipantNodeParameters,
        clock: Clock,
        testingTimeService: TestingTimeService,
        participantMetrics: ParticipantMetrics,
        testingConfigInternal: TestingConfigInternal,
        futureSupervisor: FutureSupervisor,
        loggerFactory: NamedLoggerFactory,
    )(implicit
        executionContext: ExecutionContextIdlenessExecutorService,
        scheduler: ScheduledExecutorService,
        actorSystem: ActorSystem,
        executionSequencerFactory: ExecutionSequencerFactory,
    ): Either[String, ParticipantNodeBootstrap] =
      String185
        .create(name)
        .map(
          new ParticipantNodeBootstrap(
            _,
            participantConfig,
            participantNodeParameters,
            testingConfigInternal,
            clock,
            CantonOnly.newDamlEngine(
              participantNodeParameters.uniqueContractKeys,
              participantNodeParameters.unsafeEnableDamlLfDevVersion,
            ),
            testingTimeService,
            CantonSyncService.DefaultFactory,
            participantMetrics,
            new CommunityStorageFactory(participantConfig.storage),
            _indexer => (),
            _ => new ResourceManagementService.CommunityResourceManagementService(),
            _ =>
              StaticGrpcServices
                .notSupportedByCommunity(
                  EnterpriseParticipantReplicationServiceGrpc.SERVICE,
                  loggerFactory,
                ),
            _dbConfig => Option.empty[IndexerLockIds].asRight,
            isReplicated = false,
            futureSupervisor,
            loggerFactory,
          )
        )
  }
}

/** A participant node in the system.
  *
  * The participant node can connect to a number of domains and offers:
  * - the ledger API to its application.
  * - the participant node admin API to its operator.
  *
  * @param id                               participant id
  * @param config                           Participant node configuration [[com.digitalasset.canton.participant.config.LocalParticipantConfig]] parsed
  *                                         * from config file.
  * @param storage                          participant node persistence
  * @param topologyManager                  topology manager
  * @param identityPusher                   identity pusher
  * @param ips                              identity client
  * @param sync                             synchronization service
  * @param eventPublisher                   participant level sync event log for non-domain events
  * @param ledgerApiServer                  ledger api server state
  * @param ledgerApiDependentCantonServices admin workflow services (ping, archive distribution)
  * @param adminToken the admin token required when JWT is enabled on the ledger api
  * @param recordSequencerInteractions If set to `Some(path)`, every sequencer client will record all sends requested and events received to the directory `path`.
  *                              A new recording starts whenever the participant is connected to a domain.
  * @param replaySequencerConfig If set to `Some(replayConfig)`, a sequencer client transport will be used enabling performance tests to replay previously recorded
  *                              requests and received events. See [[sequencing.client.ReplayConfig]] for more details.
  */
class ParticipantNode(
    val id: ParticipantId,
    val metrics: ParticipantMetrics,
    val config: LocalParticipantConfig,
    val nodeParameters: ParticipantNodeParameters,
    storage: Storage,
    override protected val clock: Clock,
    val topologyManager: ParticipantTopologyManager,
    val cryptoPureApi: CryptoPureApi,
    identityPusher: ParticipantTopologyDispatcher,
    partyNotifier: LedgerServerPartyNotifier,
    private[canton] val ips: IdentityProvidingServiceClient,
    private[canton] val sync: CantonSyncService,
    eventPublisher: ParticipantEventPublisher,
    ledgerApiServer: CantonLedgerApiServerWrapper.LedgerApiServerState,
    val ledgerApiDependentCantonServices: LedgerApiDependentCantonServices,
    val adminToken: CantonAdminToken,
    val recordSequencerInteractions: AtomicReference[Option[RecordingConfig]],
    val replaySequencerConfig: AtomicReference[Option[ReplayConfig]],
    val loggerFactory: NamedLoggerFactory,
) extends CantonNode
    with NamedLogging
    with HasUptime
    with NoTracing {

  def reconnectDomainsIgnoreFailures()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncServiceError, Unit] = {
    if (sync.isActive())
      sync.reconnectDomains(ignoreFailures = true).map(_ => ())
    else {
      logger.info("Not reconnecting to domains as instance is passive")
      EitherTUtil.unit
    }
  }

  /** helper utility used to auto-connect to local domains
    *
    * during startup, we first reconnect to existing domains.
    * subsequently, if requested via a cli argument, we also auto-connect to local domains.
    */
  def autoConnectLocalDomain(config: CantonDomainConnectionConfig)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SyncServiceError, Unit] = {
    if (sync.isActive()) {
      // check if we already know this domain
      sync.domainConnectionConfigStore
        .get(config.domain)
        .fold(
          _ =>
            for {
              _ <- sync.addDomain(config)
              _ <- sync.connectDomain(config.domain, keepRetrying = true)
            } yield (),
          _ => EitherTUtil.unit,
        )
    } else {
      logger.info("Not auto-connecting to local domains as instance is passive")
      EitherTUtil.unit
    }

  }

  def readyDomains: Map[DomainId, Boolean] =
    sync.readyDomains.values.toMap

  override def status: Future[ParticipantStatus] = {
    val ports = Map("ledger" -> config.ledgerApi.port, "admin" -> config.adminApi.port)
    val domains = readyDomains
    val topologyQueues = identityPusher.queueStatus
    Future.successful(
      ParticipantStatus(id.uid, uptime(), ports, domains, sync.isActive(), topologyQueues)
    )
  }

  override def close(): Unit = {
    logger.info("Stopping participant node")
    Lifecycle.close(
      ledgerApiDependentCantonServices,
      ledgerApiServer,
      identityPusher,
      partyNotifier,
      eventPublisher,
      topologyManager,
      sync,
      storage,
    )(logger)
  }

}
