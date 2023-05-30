// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import com.daml.api.util.TimeProvider
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalExplicitDisclosure,
}
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf.functionFullName
import com.daml.ports.Port
import com.daml.tracing.Telemetry
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.api.auth.CachedJwtVerifierLoader
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.participant.state.v2.metrics.{
  TimedReadService,
  TimedWriteService,
}
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ApiRequestLogger
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  RateLimitingInterceptor,
  ThreadpoolCheck,
}
import com.digitalasset.canton.platform.apiserver.{ApiServerConfig, ApiServiceOwner, LedgerFeatures}
import com.digitalasset.canton.platform.configuration.{
  AcsStreamsConfig as LedgerAcsStreamsConfig,
  IndexServiceConfig as LedgerIndexServiceConfig,
  ServerRole,
  TransactionFlatStreamsConfig as LedgerTransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig as LedgerTransactionTreeStreamsConfig,
}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig as DamlIndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.digitalasset.canton.platform.localstore.api.UserManagementStore
import com.digitalasset.canton.platform.localstore.{
  CachedIdentityProviderConfigStore,
  IdentityProviderManagementConfig,
  PersistentIdentityProviderConfigStore,
  PersistentPartyRecordStore,
  PersistentUserManagementStore,
}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SimpleExecutionQueue}
import io.grpc.{BindableService, ServerInterceptor}
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTracing
import io.scalaland.chimney.dsl.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** The StartableStoppableLedgerApi enables a canton participant node to start and stop the ledger API server
  * depending on whether the participant node is a High Availability active or passive replica.
  *
  * @param config ledger api server configuration
  * @param participantDataSourceConfig configuration for the data source (e.g., jdbc url)
  * @param dbConfig the Index DB config
  * @param createExternalServices A factory to create additional gRPC BindableService-s,
  *                               which will be bound to the Ledger API gRPC endpoint.
  *                               All the BindableService-s, which implement java.lang.AutoCloseable,
  *                               will be also closed upon Ledger API service teardown.
  * @param executionContext the execution context
  */
class StartableStoppableLedgerApiServer(
    config: CantonLedgerApiServerWrapper.Config,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    dbConfig: DbSupport.DbConfig,
    createExternalServices: () => List[BindableService] = () => Nil,
    telemetry: Telemetry,
    futureSupervisor: FutureSupervisor,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
    loggingContext: LoggingContext,
) extends FlagCloseableAsync
    with NamedLogging {

  // Use a simple execution queue as locking to ensure only one start and stop run at a time.
  private val execQueue = new SimpleExecutionQueue(
    "start-stop-ledger-api-server-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private val ledgerApiResource = new AtomicReference[Option[Resource[Unit]]](None)

  override protected def loggerFactory: NamedLoggerFactory = config.loggerFactory
  override protected def timeouts: ProcessingTimeout =
    config.cantonParameterConfig.processingTimeouts

  /** Start the ledger API server and remember the resource.
    *
    * Assumes that ledger api is currently stopped erroring otherwise. If asked to start during shutdown ignores start.
    *
    * A possible improvement to consider in the future is to abort start upon subsequent call to stop. As is the stop
    * will wait until an inflight start completes.
    *
    * @param overrideIndexerStartupMode Allows overriding the indexer startup mode.
    *                                   This is useful for forcing a custom startup mode for the first initialization
    *                                   of the participant when it is started as an active replica.
    */
  def start(overrideIndexerStartupMode: Option[IndexerStartupMode] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    execQueue.execute(
      performUnlessClosingF(functionFullName) {
        ledgerApiResource.get match {
          case Some(_ledgerApiAlreadyStarted) =>
            ErrorUtil.invalidStateAsync(
              "Attempt to start ledger API server, but ledger API server already started"
            )
          case None =>
            val ledgerApiServerResource =
              buildLedgerApiServerOwner(overrideIndexerStartupMode).acquire()(
                ResourceContext(executionContext)
              )
            FutureUtil.logOnFailure(
              ledgerApiServerResource.asFuture.map { _unit =>
                ledgerApiResource.set(Some(ledgerApiServerResource))
              },
              "Failed to start ledger API server",
            )
        }
      }.onShutdown {
        logger.info("Not starting ledger API server as we're shutting down")
      },
      "start ledger API server",
    )

  /** Stops the ledger API server, e.g. upon shutdown or when participant becomes passive.
    */
  def stop()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    execQueue.execute(
      ledgerApiResource.get match {
        case Some(ledgerApiServerToStop) =>
          FutureUtil.logOnFailure(
            ledgerApiServerToStop.release().map { _unit =>
              logger.debug("Successfully stopped ledger API server")
              ledgerApiResource.set(None)
            },
            "Failed to stop ledger API server",
          )
        case None =>
          logger.debug("ledger API server already stopped")
          Future.unit
      },
      "stop ledger API server",
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      logger.debug("Shutting down ledger API server")
      Seq(
        AsyncCloseable("ledger API server", stop().unwrap, timeouts.shutdownNetwork.duration),
        SyncCloseable("ledger-api-server-queue", execQueue.close()),
      )
    }

  private def buildLedgerApiServerOwner(
      overrideIndexerStartupMode: Option[IndexerStartupMode]
  )(implicit traceContext: TraceContext) = {
    val acsStreamsConfig =
      config.serverConfig.activeContractsService.transformInto[LedgerAcsStreamsConfig]
    val txFlatStreamsConfig =
      config.serverConfig.activeContractsService.transformInto[LedgerTransactionFlatStreamsConfig]
    val txTreeStreamsConfig =
      config.serverConfig.activeContractsService.transformInto[LedgerTransactionTreeStreamsConfig]

    val indexServiceConfig = LedgerIndexServiceConfig(
      acsStreams = acsStreamsConfig,
      transactionFlatStreams = txFlatStreamsConfig,
      transactionTreeStreams = txTreeStreamsConfig,
      bufferedEventsProcessingParallelism = config.serverConfig.bufferedEventsProcessingParallelism,
      maxContractStateCacheSize = config.serverConfig.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = config.serverConfig.maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize =
        config.serverConfig.maxTransactionsInMemoryFanOutBufferSize,
      apiStreamShutdownTimeout = config.serverConfig.apiStreamShutdownTimeout.unwrap,
      inMemoryStateUpdaterParallelism = config.serverConfig.inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize = config.serverConfig.inMemoryFanOutThreadPoolSize.getOrElse(
        LedgerApiServerConfig.DefaultInMemoryFanOutThreadPoolSize
      ),
      preparePackageMetadataTimeOutWarning =
        config.serverConfig.preparePackageMetadataTimeOutWarning.underlying,
    )

    val indexerConfig = config.indexerConfig.damlConfig(
      indexerLockIds = config.indexerLockIds,
      dataSourceProperties = Some(
        DbSupport.DataSourceProperties(
          connectionPool = DamlIndexerConfig
            .createDataSourceProperties(config.indexerConfig.ingestionParallelism.unwrap)
            .connectionPool
            .copy(connectionTimeout = config.serverConfig.databaseConnectionTimeout.underlying),
          postgres = config.serverConfig.postgresDataSource.damlConfig,
        )
      ),
    )

    val authService = new CantonAdminTokenAuthService(
      config.adminToken,
      parent = config.serverConfig.authServices.map(
        _.create(
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway
        )
      ),
    )

    val jwtVerifierLoader = new CachedJwtVerifierLoader(metrics = config.metrics)

    for {
      (inMemoryState, inMemoryStateUpdaterFlow) <-
        LedgerApiServer.createInMemoryStateAndUpdater(
          indexServiceConfig,
          config.serverConfig.commandService.maxCommandsInFlight,
          config.metrics,
          executionContext,
        )
      timedReadService = new TimedReadService(config.syncService, config.metrics)
      indexerHealth <- new IndexerServiceOwner(
        config.participantId,
        participantDataSourceConfig,
        timedReadService,
        overrideIndexerStartupMode
          .map(overrideStartupMode => indexerConfig.copy(startupMode = overrideStartupMode))
          .getOrElse(indexerConfig),
        config.metrics,
        inMemoryState,
        inMemoryStateUpdaterFlow,
        config.serverConfig.additionalMigrationPaths,
        executionContext,
      )
      dbSupport <- DbSupport
        .owner(
          serverRole = ServerRole.ApiServer,
          metrics = config.metrics,
          dbConfig = dbConfig,
        )
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        initialLedgerId = domain.LedgerId(config.ledgerId),
        config = indexServiceConfig,
        participantId = config.participantId,
        metrics = config.metrics,
        servicesExecutionContext = executionContext,
        engine = config.engine,
        inMemoryState = inMemoryState,
        tracer = config.tracerProvider.tracer,
      )
      userManagementStore = getUserManagementStore(dbSupport)
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = config.metrics,
        timeProvider = TimeProvider.UTC,
        executionContext = executionContext,
      )

      apiServerConfig = getApiServerConfig

      timedWriteService = new TimedWriteService(config.syncService, config.metrics)
      _ <- ApiServiceOwner(
        submissionTracker = inMemoryState.submissionTracker,
        indexService = indexService,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = getIdentityProviderConfigStore(dbSupport, apiServerConfig),
        partyRecordStore = partyRecordStore,
        ledgerId = config.ledgerId,
        participantId = config.participantId,
        config = apiServerConfig,
        optWriteService = Some(timedWriteService),
        healthChecks = new HealthChecks(
          "read" -> timedReadService,
          "write" -> (() => config.syncService.currentWriteHealth()),
          "indexer" -> indexerHealth,
        ),
        metrics = config.metrics,
        timeServiceBackend = config.testingTimeService,
        otherServices = Nil,
        otherInterceptors = getInterceptors(dbSupport.dbDispatcher.executor),
        engine = config.engine,
        authorityResolver = config.syncService.cantonAuthorityResolver,
        servicesExecutionContext = executionContext,
        checkOverloaded = config.syncService.checkOverloaded,
        ledgerFeatures = getLedgerFeatures,
        authService = authService,
        jwtVerifierLoader = jwtVerifierLoader,
        jwtTimestampLeeway =
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway,
        meteringReportKey = config.meteringReportKey,
        createExternalServices = createExternalServices,
        explicitDisclosureUnsafeEnabled = config.serverConfig.explicitDisclosureUnsafe,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
      )
    } yield ()
  }

  private def getIdentityProviderConfigStore(
      dbSupport: DbSupport,
      apiServerConfig: ApiServerConfig,
  ): CachedIdentityProviderConfigStore = PersistentIdentityProviderConfigStore.cached(
    dbSupport = dbSupport,
    metrics = config.metrics,
    cacheExpiryAfterWrite = apiServerConfig.identityProviderManagement.cacheExpiryAfterWrite,
    maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
  )(executionContext, loggingContext)

  private def getUserManagementStore(dbSupport: DbSupport): UserManagementStore =
    PersistentUserManagementStore.cached(
      dbSupport = dbSupport,
      metrics = config.metrics,
      timeProvider = TimeProvider.UTC,
      cacheExpiryAfterWriteInSeconds =
        config.serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
      maxCacheSize = config.serverConfig.userManagementService.maxCacheSize,
      maxRightsPerUser = config.serverConfig.userManagementService.maxRightsPerUser,
    )(executionContext, loggingContext)

  private def getInterceptors(
      indexerExecutor: QueueAwareExecutor with NamedExecutor
  ): List[ServerInterceptor] = List(
    new ApiRequestLogger(
      config.loggerFactory,
      config.cantonParameterConfig.loggingConfig.api,
    ),
    GrpcTracing
      .builder(config.tracerProvider.openTelemetry)
      .build()
      .newServerInterceptor(),
  ) ::: config.serverConfig.rateLimit
    .map(rateLimit =>
      RateLimitingInterceptor(
        metrics = config.metrics,
        config = rateLimit,
        additionalChecks = List(
          ThreadpoolCheck(
            name = "Environment Execution Threadpool",
            limit = rateLimit.maxApiServicesQueueSize,
            queue = executionContext,
          ),
          ThreadpoolCheck(
            name = "Index DB Threadpool",
            limit = rateLimit.maxApiServicesIndexDbQueueSize,
            queue = indexerExecutor,
          ),
        ),
      )
    )
    .toList

  private def getLedgerFeatures: LedgerFeatures = LedgerFeatures(
    staticTime = config.testingTimeService.isDefined,
    CommandDeduplicationFeatures.of(
      Some(
        CommandDeduplicationPeriodSupport.of(
          offsetSupport = CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_NATIVE_SUPPORT,
          durationSupport =
            CommandDeduplicationPeriodSupport.DurationSupport.DURATION_CONVERT_TO_OFFSET,
        )
      ),
      CommandDeduplicationType.ASYNC_AND_CONCURRENT_SYNC,
      maxDeduplicationDurationEnforced = false,
    ),
    explicitDisclosure =
      ExperimentalExplicitDisclosure.of(config.serverConfig.explicitDisclosureUnsafe),
  )

  private def getApiServerConfig: ApiServerConfig = ApiServerConfig(
    address = Some(config.serverConfig.address),
    apiStreamShutdownTimeout = config.serverConfig.apiStreamShutdownTimeout.unwrap,
    command = config.serverConfig.commandService.damlConfig,
    configurationLoadTimeout = config.serverConfig.configurationLoadTimeout.unwrap,
    initialLedgerConfiguration =
      None, // CantonSyncService provides ledger configuration via ReadService bypassing the WriteService
    managementServiceTimeout = config.serverConfig.managementServiceTimeout.underlying,
    maxInboundMessageSize = config.serverConfig.maxInboundMessageSize.unwrap,
    port = Port(config.serverConfig.port.unwrap),
    portFile = None,
    rateLimit = config.serverConfig.rateLimit,
    seeding = config.cantonParameterConfig.ledgerApiServerParameters.contractIdSeeding,
    timeProviderType = config.testingTimeService match {
      case Some(_) => TimeProviderType.Static
      case None => TimeProviderType.WallClock
    },
    tls = config.serverConfig.tls
      .map(LedgerApiServerConfig.ledgerApiServerTlsConfigFromCantonServerConfig),
    userManagement = config.serverConfig.userManagementService.damlConfig,
  )
}
