// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import akka.actor.ActorSystem
import better.files._
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.error.ErrorGroup
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.RequireTypes.InstanceName
import com.digitalasset.canton.config.TestingConfigInternal
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0.{
  DomainServiceGrpc,
  SequencerInitializationServiceGrpc,
  SequencerVersionServiceGrpc,
}
import com.digitalasset.canton.domain.admin.{grpc => admingrpc}
import com.digitalasset.canton.domain.config._
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.initialization._
import com.digitalasset.canton.domain.mediator.{
  CommunityMediatorRuntimeFactory,
  MediatorRuntime,
  MediatorRuntimeFactory,
}
import com.digitalasset.canton.domain.metrics.DomainMetrics
import com.digitalasset.canton.domain.sequencing.admin._
import com.digitalasset.canton.domain.sequencing.admin.client.SequencerAdminClient
import com.digitalasset.canton.domain.sequencing.admin.protocol.InitRequest
import com.digitalasset.canton.domain.sequencing.service.{
  GrpcSequencerInitializationService,
  GrpcSequencerVersionService,
}
import com.digitalasset.canton.domain.sequencing.{
  SequencerKeyInitialization,
  SequencerRuntime,
  SequencerRuntimeFactory,
}
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.domain.topology._
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrapBase}
import com.digitalasset.canton.health.admin.data.{DomainStatus, TopologyQueueStatus}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableServer
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonMutableHandlerRegistry
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.resource.{CommunityStorageFactory, Storage, StorageFactory}
import com.digitalasset.canton.sequencing.client.{grpc => _, _}
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.time.{Clock, HasUptime}
import com.digitalasset.canton.topology.TopologyManagerError.DomainErrorGroup
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client._
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyStoreId.{AuthorizedStore, DomainStore}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{ExecutionContextExecutorService, Future, blocking}

/** Startup / Bootstrapping class for domain
  *
  * The domain startup has three stages:
  * (1) start core services, wait until domainId is initialized (first time)
  * (2) start domain topology manager, wait until essential state is seeded (sequencer, identity and mediator keys are set)
  * (3) start domain entities
  */
class DomainNodeBootstrap(
    override val name: InstanceName,
    val config: DomainConfig,
    testingConfig: TestingConfigInternal,
    parameters: DomainNodeParameters,
    clock: Clock,
    metrics: DomainMetrics,
    parentLogger: NamedLoggerFactory = NamedLoggerFactory.root,
    legalIdentityHook: X509Certificate => EitherT[Future, String, Unit],
    addMemberHook: DomainTopologyManager.AddMemberHook,
    sequencerRuntimeFactory: SequencerRuntimeFactory,
    mediatorFactory: MediatorRuntimeFactory,
    storageFactory: StorageFactory,
    futureSupervisor: FutureSupervisor,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
) extends CantonNodeBootstrapBase[Domain, DomainConfig, DomainNodeParameters](
      name,
      config,
      parameters,
      clock,
      metrics,
      storageFactory,
      parentLogger.append(DomainNodeBootstrap.LoggerFactoryKeyName, name.unwrap),
    )
    with DomainTopologyManagerIdentityInitialization {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var topologyManager: Option[DomainTopologyManager] = None
  private val protocolVersion = config.domainParameters.protocolVersion.unwrap

  override protected def autoInitializeIdentity(): EitherT[Future, String, Unit] =
    withNewTraceContext { implicit traceContext =>
      for {
        initialized <- initializeTopologyManagerIdentity(
          name,
          legalIdentityHook,
          DynamicDomainParameters.initialValues(clock),
          protocolVersion,
        )
        (nodeId, topologyManager, namespaceKey) = initialized
        domainId = DomainId(nodeId.identity)
        _ <- initializeMediator(domainId, namespaceKey, topologyManager)
        _ <- initializeSequencerServices
        _ <- initializeSequencer(domainId, topologyManager, namespaceKey)
        // finally, store the node id (which means we have completed initialisation)
        // as all methods above are idempotent, if we die during initialisation, we should come back here
        // and resume until we've stored the node id
        _ <- storeId(nodeId)
        _ <- startDomain(topologyManager)
      } yield ()
    }

  override def onClosed(): Unit = blocking {
    synchronized {
      logger.info("Stopping domain node")
      super.onClosed()
    }
  }

  private lazy val staticDomainParameters: Either[String, StaticDomainParameters] =
    config.domainParameters.toStaticDomainParameters(config.crypto)

  private def initializeMediator(
      domainId: DomainId,
      namespaceKey: SigningPublicKey,
      topologyManager: DomainTopologyManager,
  ): EitherT[Future, String, Unit] = {
    // In a domain without a dedicated DomainTopologyManager, the mediator always gets the same ID as the domain.
    val mediatorId = MediatorId(domainId)
    for {
      mediatorKey <- getOrCreateSigningKey(s"$name-mediator-signing")
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        OwnerToKeyMapping(mediatorId, mediatorKey),
        protocolVersion,
      )
      _ <- authorizeStateUpdate(
        topologyManager,
        namespaceKey,
        MediatorDomainState(RequestSide.Both, domainId, mediatorId),
        protocolVersion,
      )
    } yield ()
  }

  private def initializeSequencer(
      domainId: DomainId,
      topologyManager: DomainTopologyManager,
      namespaceKey: SigningPublicKey,
  ): EitherT[Future, String, PublicKey] = {
    def createAdminConnection(): EitherT[Future, String, SequencerAdminClient] =
      for {
        adminConnection <- config.adminApi.toSequencerConnectionConfig.toConnection
          .toEitherT[Future]

        staticDomainParameters <- EitherT.fromEither[Future](staticDomainParameters)

        adminClient <- SequencerAdminClient.create(
          adminConnection,
          staticDomainParameters,
          parameters.processingTimeouts,
          parameters.tracing.propagation,
          crypto,
          loggerFactory,
        )
      } yield adminClient

    val adminClientE = createAdminConnection()

    def closeAdminConnections(): Unit =
      parameters.processingTimeouts.shutdownNetwork
        .await("Closing the admin client connections")(adminClientE.map(_.close()).value)
        .valueOr(err => logger.error(s"Failed to close sequencer admin connection: $err"))

    val result = for {
      adminClient <- adminClientE
      staticDomainParameters <- EitherT.fromEither[Future](staticDomainParameters)
      request = InitRequest(domainId, StoredTopologyTransactions.empty, staticDomainParameters)
      key <- SequencerInitialization
        .attemptInitialization(
          name,
          parameters.sequencerClient,
          parameters.devVersionSupport,
          adminClient,
          authorizeStateUpdate(topologyManager, namespaceKey, _, protocolVersion),
          crypto.cryptoPublicStore,
          request,
          parameters.processingTimeouts,
          loggerFactory,
        )
    } yield key

    result.thereafter { _ =>
      closeAdminConnections()
    }
  }

  override protected def initializeIdentityManagerAndServices(
      nodeId: NodeId
  ): Either[String, DomainTopologyManager] = {
    // starts second stage
    ErrorUtil.requireState(topologyManager.isEmpty, "Topology manager is already initialized.")

    logger.debug("Starting domain topology manager")
    staticDomainParameters.map { staticDomainParameters =>
      val manager = new DomainTopologyManager(
        nodeId.identity,
        clock,
        topologyStoreFactory.forId(AuthorizedStore),
        addMemberHook,
        crypto,
        parameters.processingTimeouts,
        staticDomainParameters.protocolVersion,
        loggerFactory,
      )
      topologyManager = Some(manager)
      startTopologyManagementWriteService(manager, manager.store)
      manager
    }

  }

  /** If we're running a sequencer within the domain node itself, then locally start the sequencer initialization service */
  private def initializeSequencerServices: EitherT[Future, String, Unit] =
    for {
      versionService <- EitherT.rightT[Future, String](
        new GrpcSequencerVersionService(protocolVersion, loggerFactory)
      )
      initializationService = new GrpcSequencerInitializationService(
        SequencerKeyInitialization.ensureKeyExists(crypto),
        loggerFactory,
      )
      // register with the server
      _ = adminServerRegistry
        .addService(
          SequencerInitializationServiceGrpc.bindService(initializationService, executionContext)
        )
        .addService(SequencerVersionServiceGrpc.bindService(versionService, executionContext))
    } yield ()

  override protected def initialize(id: NodeId): EitherT[Future, String, Unit] = {
    val topologyManager = initializeIdentityManagerAndServices(id)

    EitherT.fromEither[Future](topologyManager).flatMap(startIfDomainManagerReadyOrDefer)
  }

  /** The Domain cannot be started until the domain manager has keys for all domain entities available. These keys
    * can be provided by console commands or external processes via the admin API so there are no guarantees for when they
    * arrive. If they are not immediately available, we add an observer to the topology manager which will be triggered after
    * every change to the topology manager. If after one of these changes we find that the domain manager has the keys it
    * requires to be initialized we will then start the domain.
    * TODO(error handling): if we defer startup of the domain the initialization check and eventual domain startup will
    *                       occur within the topology manager transaction observer. currently exceptions will bubble
    *                       up into the topology transaction processor however if a error is encountered it is just
    *                       logged here leaving the domain in a dangling state. Ideally this would trigger a managed
    *                       shutdown of some form allow allowing another startup attempt to be run if appropriate, however
    *                       I don't believe we currently have a means of doing this.
    */
  private def startIfDomainManagerReadyOrDefer(
      manager: DomainTopologyManager
  ): EitherT[Future, String, Unit] = {
    def deferStart: EitherT[Future, String, Unit] = {
      val attemptedStart = new AtomicBoolean(false)

      logger.info("Deferring domain startup until domain manager has been fully initialized")
      manager.addObserver(new DomainIdentityStateObserver {
        override def addedSignedTopologyTransaction(
            timestamp: CantonTimestamp,
            transaction: Seq[SignedTopologyTransaction[TopologyChangeOp]],
        )(implicit traceContext: TraceContext): Unit = {
          // we can't unsubscribe observers so instead just ignore transactions once we've attempted to start once
          if (!attemptedStart.get()) {
            val initTimeout = parameters.processingTimeouts.unbounded
            val managerInitialized =
              initTimeout.await(
                s"Domain startup waiting for the domain topology manager to be initialised"
              )(manager.isInitialized)
            if (managerInitialized) {
              if (attemptedStart.compareAndSet(false, true)) {
                // we're now the top level error handler of starting a domain so log appropriately
                val domainStarted =
                  initTimeout.await("Domain startup awaiting domain ready to handle requests")(
                    startDomain(manager).value
                  )
                domainStarted match {
                  case Left(error) =>
                    logger.error(s"Deferred domain startup failed with error: $error")
                  case Right(_) => // nothing to do
                }
              }
            }
          }
        }
      })

      EitherT.pure[Future, String](())
    }

    for {
      // if the domain is starting up after previously running its identity will have been stored and will be immediately available
      alreadyInitialized <- EitherT.right[String](manager.isInitialized)
      // if not, then create an observer of topology transactions that will check each time whether full identity has been generated
      _ <- if (alreadyInitialized) startDomain(manager) else deferStart
    } yield ()
  }

  /** Attempt to create the domain and only return and call setInstance once it is ready to handle requests */
  private def startDomain(manager: DomainTopologyManager): EitherT[Future, String, Unit] =
    startInstanceUnlessClosing {
      // store with all topology transactions which were timestamped and distributed via sequencer
      val domainId = DomainId(manager.id)
      val sequencedTopologyStore = topologyStoreFactory.forId(DomainStore(domainId))
      val publicSequencerConnectionEitherT =
        config.publicApi.toSequencerConnectionConfig.toConnection.toEitherT[Future]

      for {
        publicSequencerConnection <- publicSequencerConnectionEitherT
        managerDiscriminator <- EitherT.right(
          SequencerClientDiscriminator.fromDomainMember(manager.managerId, indexedStringStore)
        )
        topologyManagerSequencerCounterTrackerStore = SequencerCounterTrackerStore(
          storage,
          managerDiscriminator,
          timeouts,
          loggerFactory,
        )
        initialKeys <- EitherT.right(manager.getKeysForBootstrapping())
        processorAndClient <- EitherT.right(
          TopologyTransactionProcessor.createProcessorAndClientForDomain(
            sequencedTopologyStore,
            domainId,
            crypto.pureCrypto,
            SigningPublicKey.collect(initialKeys),
            parameters,
            clock,
            futureSupervisor,
            loggerFactory,
          )
        )
        (topologyProcessor, topologyClient) = processorAndClient

        staticDomainParameters <- EitherT.fromEither[Future](staticDomainParameters)

        sequencerClientFactoryFactory = (client: DomainTopologyClientWithInit) =>
          new DomainNodeSequencerClientFactory(
            domainId,
            metrics,
            client,
            publicSequencerConnection,
            parameters,
            crypto,
            staticDomainParameters,
            testingConfig,
            clock,
            futureSupervisor,
            loggerFactory,
          )

        auditLogger = ParticipantAuditor.factory(loggerFactory, config.auditLogging)

        // add audit logging to the domain manager
        _ = if (config.auditLogging) {
          manager.addObserver(new DomainIdentityStateObserver {
            override def willChangeTheParticipantState(
                participant: ParticipantId,
                attributes: ParticipantAttributes,
            ): Unit = {
              auditLogger.info(s"Updating participant $participant to $attributes")
            }
          })
        }

        syncCrypto: DomainSyncCryptoClient = {
          ips.add(topologyClient)
          new SyncCryptoApiProvider(
            manager.managerId,
            ips,
            crypto,
            parameters.cachingConfigs,
            timeouts,
            loggerFactory,
          )
            .tryForDomain(domainId)
        }

        // Setup the service agreement manager and its storage
        agreementManager <- config.serviceAgreement
          .traverse { agreementFile =>
            ServiceAgreementManager
              .create(agreementFile.toScala, storage, crypto.pureCrypto, timeouts, loggerFactory)
          }
          .toEitherT[Future]

        sequencerRuntime = sequencerRuntimeFactory
          .create(
            domainId,
            crypto,
            sequencedTopologyStore,
            topologyClient,
            topologyProcessor,
            storage,
            clock,
            config,
            staticDomainParameters,
            testingConfig,
            parameters.processingTimeouts,
            auditLogger,
            agreementManager,
            parameters,
            metrics.sequencer,
            indexedStringStore,
            loggerFactory,
            logger,
          )

        domainIdentityService = DomainTopologyManagerRequestService.create(
          config.topology,
          manager,
          topologyClient,
          clock,
          topologyStoreFactory,
          parameters.processingTimeouts,
          loggerFactory,
        )

        // must happen before the init of topology management since it will call the embedded sequencer's public api
        publicServer = PublicGrpcServerInitialization(
          config,
          metrics,
          parameters,
          loggerFactory,
          logger,
          sequencerRuntime,
          domainId,
          agreementManager,
          staticDomainParameters,
          syncCrypto,
        )

        topologyManagementArtefacts <- TopologyManagementInitialization(
          config,
          domainId,
          storage,
          clock,
          crypto,
          syncCrypto,
          sequencedTopologyStore,
          publicSequencerConnection,
          manager,
          domainIdentityService,
          topologyManagerSequencerCounterTrackerStore,
          topologyProcessor,
          topologyClient,
          initialKeys,
          sequencerClientFactoryFactory(topologyClient),
          parameters,
          indexedStringStore,
          loggerFactory,
        )

        mediatorRuntime <- EmbeddedMediatorInitialization(
          domainId,
          parameters,
          staticDomainParameters.protocolVersion,
          clock,
          crypto,
          topologyStoreFactory.forId(DomainStore(domainId, discriminator = "M")),
          config.timeTracker,
          storage,
          sequencerClientFactoryFactory,
          metrics,
          mediatorFactory,
          indexedStringStore,
          futureSupervisor,
          loggerFactory.append("node", "mediator"),
        )

        domain = {
          logger.debug("Starting domain services")
          new Domain(
            config,
            clock,
            staticDomainParameters,
            adminServerRegistry,
            manager,
            agreementManager,
            topologyManagementArtefacts,
            storage,
            sequencerRuntime,
            mediatorRuntime,
            publicServer,
            loggerFactory,
          )
        }
      } yield domain
    }

  override def isActive: Boolean = true
}

object DomainNodeBootstrap {
  val LoggerFactoryKeyName: String = "domain"

  trait Factory[DC <: DomainConfig] {

    def create(
        name: String,
        domainConfig: DC,
        testingConfig: TestingConfigInternal,
        parameters: DomainNodeParameters,
        clock: Clock,
        metrics: DomainMetrics,
        futureSupervisor: FutureSupervisor,
        parentLogger: NamedLoggerFactory = NamedLoggerFactory.root,
    )(implicit
        actorSystem: ActorSystem,
        ec: ExecutionContextIdlenessExecutorService,
        traceContext: TraceContext,
    ): Either[String, DomainNodeBootstrap]
  }

  object CommunityDomainFactory extends Factory[CommunityDomainConfig] {

    override def create(
        name: String,
        config: CommunityDomainConfig,
        testingConfig: TestingConfigInternal,
        parameters: DomainNodeParameters,
        clock: Clock,
        metrics: DomainMetrics,
        futureSupervisor: FutureSupervisor,
        parentLogger: NamedLoggerFactory,
    )(implicit
        actorSystem: ActorSystem,
        executionContext: ExecutionContextIdlenessExecutorService,
        traceContext: TraceContext,
    ): Either[String, DomainNodeBootstrap] = {
      for {
        domainName <- InstanceName.create(name).leftMap(_.toString)
      } yield new DomainNodeBootstrap(
        domainName,
        config,
        testingConfig,
        parameters,
        clock,
        metrics,
        parentLogger,
        DomainTopologyManager.legalIdentityHookNoOp,
        DomainTopologyManager.addMemberNoOp,
        new SequencerRuntimeFactory.Community(config.sequencer),
        CommunityMediatorRuntimeFactory,
        new CommunityStorageFactory(config.storage),
        futureSupervisor,
      )

    }
  }
}

/** A domain in the system.
  *
  * The domain offers to the participant nodes:
  * - sequencing for total-order multicast.
  * - mediator as part of the transaction protocol coordination.
  * - identity providing service.
  *
  * @param config Domain configuration [[com.digitalasset.canton.domain.config.DomainConfig]] parsed from config file.
  */
class Domain(
    val config: DomainConfig,
    override protected val clock: Clock,
    staticDomainParameters: StaticDomainParameters,
    adminApiRegistry: CantonMutableHandlerRegistry,
    val domainTopologyManager: DomainTopologyManager,
    val agreementManager: Option[ServiceAgreementManager],
    topologyManagementArtefacts: TopologyManagementComponents,
    storage: Storage,
    sequencerRuntime: SequencerRuntime,
    @VisibleForTesting
    val mediatorRuntime: MediatorRuntime,
    publicServer: CloseableServer,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContextExecutorService)
    extends CantonNode
    with NamedLogging
    with HasUptime
    with NoTracing {

  registerAdminServices()

  logger.debug("Domain successfully initialized")

  override def status: Future[DomainStatus] =
    for {
      activeMembers <- sequencerRuntime.fetchActiveMembers()
      sequencer <- sequencerRuntime.health
    } yield {
      val ports = Map("admin" -> config.adminApi.port, "public" -> config.publicApi.port)
      val participants = activeMembers.collect { case x: ParticipantId =>
        x
      }
      val topologyQueues = TopologyQueueStatus(
        manager = domainTopologyManager.queueSize,
        dispatcher = topologyManagementArtefacts.dispatcher.queueSize,
        clients = topologyManagementArtefacts.client.numPendingChanges,
      )
      DomainStatus(
        domainTopologyManager.id,
        uptime(),
        ports,
        participants,
        sequencer,
        topologyQueues,
      )
    }

  override def close(): Unit = {
    logger.debug("Stopping domain runner")
    Lifecycle.close(
      topologyManagementArtefacts,
      domainTopologyManager,
      mediatorRuntime,
      sequencerRuntime,
      publicServer,
      storage,
    )(logger)
  }

  def registerAdminServices(): Unit = {
    // The domain admin-API services
    sequencerRuntime.registerAdminGrpcServices { service =>
      adminApiRegistry.addService(service).discard
    }
    mediatorRuntime.registerAdminGrpcServices { service =>
      adminApiRegistry.addService(service).discard
    }
    adminApiRegistry
      .addService(
        DomainServiceGrpc
          .bindService(
            new admingrpc.GrpcDomainService(
              staticDomainParameters,
              agreementManager,
            ),
            executionContext,
          )
      )
      .discard
  }
}

object Domain extends DomainErrorGroup {

  /** If the function maps `member` to `recordConfig`,
    * the sequencer client for `member` will record all sends requested and events received to the directory specified
    * by the recording config.
    * A new recording starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val recordSequencerInteractions: AtomicReference[PartialFunction[Member, RecordingConfig]] =
    new AtomicReference(PartialFunction.empty)

  /** If the function maps `member` to `path`,
    * the sequencer client for `member` will replay events from `path` instead of pulling them from the sequencer.
    * A new replay starts whenever the domain is restarted.
    */
  @VisibleForTesting
  val replaySequencerConfig: AtomicReference[PartialFunction[Member, ReplayConfig]] =
    new AtomicReference(PartialFunction.empty)

  def setMemberRecordingPath(member: Member)(config: RecordingConfig): RecordingConfig = {
    val namePrefix = member.show.stripSuffix("...")
    config.setFilename(namePrefix)
  }

  def defaultReplayPath(member: Member)(config: ReplayConfig): ReplayConfig =
    config.copy(recordingConfig = setMemberRecordingPath(member)(config.recordingConfig))

  abstract class GrpcSequencerAuthenticationErrorGroup extends ErrorGroup

}
