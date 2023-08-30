// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import com.daml.api.util.TimeProvider
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.http.HttpApiServer
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
  ExperimentalExplicitDisclosure,
}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.nameof.NameOf.functionFullName
import com.daml.tracing.Telemetry
import com.digitalasset.canton.DiscardOps
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
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{ApiRequestLogger, ClientChannelBuilder}
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.IndexerLockIds
import com.digitalasset.canton.platform.LedgerApiServer
import com.digitalasset.canton.platform.apiserver.ratelimiting.{
  RateLimitingInterceptor,
  ThreadpoolCheck,
}
import com.digitalasset.canton.platform.apiserver.{ApiServerConfig, ApiServiceOwner, LedgerFeatures}
import com.digitalasset.canton.platform.config.{
  IndexServiceConfig as LedgerIndexServiceConfig,
  ServerRole,
}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.indexer.IndexerConfig.DefaultIndexerStartupMode
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.digitalasset.canton.platform.localstore.*
import com.digitalasset.canton.platform.localstore.api.UserManagementStore
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import com.digitalasset.canton.platform.store.dao.events.ContractLoader
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import io.grpc.ServerInterceptor
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTracing

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** The StartableStoppableLedgerApi enables a canton participant node to start and stop the ledger API server
  * depending on whether the participant node is a High Availability active or passive replica.
  *
  * @param config ledger api server configuration
  * @param participantDataSourceConfig configuration for the data source (e.g., jdbc url)
  * @param dbConfig the Index DB config
  * @param executionContext the execution context
  */
class StartableStoppableLedgerApiServer(
    config: CantonLedgerApiServerWrapper.Config,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    dbConfig: DbSupport.DbConfig,
    telemetry: Telemetry,
    futureSupervisor: FutureSupervisor,
    multiDomainEnabled: Boolean,
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
    tracer: Tracer,
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
            logger.info(
              "Attempt to start ledger API server, but ledger API server already started. Ignoring."
            )
            Future.unit
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
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)

    val indexServiceConfig = LedgerIndexServiceConfig(
      activeContractsServiceStreams = config.serverConfig.activeContractsService,
      transactionFlatStreams = config.serverConfig.transactionFlatStreams,
      transactionTreeStreams = config.serverConfig.transactionTreeStreams,
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
      completionsPageSize = config.serverConfig.completionsPageSize,
      globalMaxEventIdQueries = config.serverConfig.globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = config.serverConfig.globalMaxEventPayloadQueries,
      bufferedStreamsPageSize = config.serverConfig.bufferedStreamsPageSize,
    )

    val authService = new CantonAdminTokenAuthService(
      config.adminToken,
      parent = config.serverConfig.authServices.map(
        _.create(
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway,
          loggerFactory,
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
          tracer,
          loggerFactory,
          multiDomainEnabled = multiDomainEnabled,
        )
      timedReadService = new TimedReadService(config.syncService, config.metrics)
      indexerHealth <- new IndexerServiceOwner(
        config.participantId,
        participantDataSourceConfig,
        timedReadService,
        config.indexerConfig,
        config.metrics,
        inMemoryState,
        inMemoryStateUpdaterFlow,
        config.serverConfig.additionalMigrationPaths,
        executionContext,
        tracer,
        loggerFactory,
        multiDomainEnabled = multiDomainEnabled,
        startupMode = overrideIndexerStartupMode.getOrElse(DefaultIndexerStartupMode),
        dataSourceProperties = Some(
          DbSupport.DataSourceProperties(
            connectionPool = IndexerConfig
              .createConnectionPoolConfig(
                ingestionParallelism = config.indexerConfig.ingestionParallelism.unwrap,
                connectionTimeout = config.serverConfig.databaseConnectionTimeout.underlying,
              ),
            postgres = config.serverConfig.postgresDataSource,
          )
        ),
        highAvailability = config.indexerLockIds.fold(HaConfig()) {
          case IndexerLockIds(mainLockId, workerLockId) =>
            HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
        },
      )
      dbSupport <- DbSupport
        .owner(
          serverRole = ServerRole.ApiServer,
          metrics = config.metrics,
          dbConfig = dbConfig,
          loggerFactory = loggerFactory,
        )
      contractLoader <- {
        import config.cantonParameterConfig.ledgerApiServerParameters.contractLoader.*
        ContractLoader.create(
          contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
            inMemoryState.ledgerEndCache,
            inMemoryState.stringInterningView,
          ),
          dbDispatcher = dbSupport.dbDispatcher,
          metrics = config.metrics,
          maxQueueSize = maxQueueSize.value,
          maxBatchSize = maxBatchSize.value,
          parallelism = parallelism.value,
          loggerFactory = loggerFactory,
        )
      }
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
        loggerFactory = loggerFactory,
        incompleteOffsets = timedReadService.incompleteReassignmentOffsets(_, _)(_),
        contractLoader = contractLoader,
      )
      userManagementStore = getUserManagementStore(dbSupport, loggerFactory)
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = config.metrics,
        timeProvider = TimeProvider.UTC,
        executionContext = executionContext,
        loggerFactory = loggerFactory,
      )

      apiServerConfig = getApiServerConfig

      timedWriteService = new TimedWriteService(config.syncService, config.metrics)
      _ <- ApiServiceOwner(
        submissionTracker = inMemoryState.submissionTracker,
        indexService = indexService,
        userManagementStore = userManagementStore,
        identityProviderConfigStore =
          getIdentityProviderConfigStore(dbSupport, apiServerConfig, loggerFactory),
        partyRecordStore = partyRecordStore,
        ledgerId = config.ledgerId,
        participantId = config.participantId,
        config = apiServerConfig,
        optWriteService = Some(timedWriteService),
        readService = timedReadService,
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
        explicitDisclosureUnsafeEnabled = config.serverConfig.explicitDisclosureUnsafe,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        multiDomainEnabled = multiDomainEnabled,
        upgradingEnabled = config.cantonParameterConfig.enableContractUpgrading,
      )
      _ <- startHttpApiIfEnabled
      _ <- {
        config.serverConfig.userManagementService.additionalAdminUserId.fold(ResourceOwner.unit) {
          rawUserId =>
            ResourceOwner.forFuture { () =>
              userManagementStore.createExtraAdminUser(rawUserId)
            }
        }
      }
    } yield ()
  }

  private def getIdentityProviderConfigStore(
      dbSupport: DbSupport,
      apiServerConfig: ApiServerConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): CachedIdentityProviderConfigStore =
    PersistentIdentityProviderConfigStore.cached(
      dbSupport = dbSupport,
      metrics = config.metrics,
      cacheExpiryAfterWrite = apiServerConfig.identityProviderManagement.cacheExpiryAfterWrite,
      maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
      loggerFactory = loggerFactory,
    )

  private def getUserManagementStore(dbSupport: DbSupport, loggerFactory: NamedLoggerFactory)(
      implicit traceContext: TraceContext
  ): UserManagementStore =
    PersistentUserManagementStore.cached(
      dbSupport = dbSupport,
      metrics = config.metrics,
      timeProvider = TimeProvider.UTC,
      cacheExpiryAfterWriteInSeconds =
        config.serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
      maxCacheSize = config.serverConfig.userManagementService.maxCacheSize,
      maxRightsPerUser = config.serverConfig.userManagementService.maxRightsPerUser,
      loggerFactory = loggerFactory,
    )(executionContext, traceContext)

  private def getInterceptors(
      indexerExecutor: QueueAwareExecutor & NamedExecutor
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
        loggerFactory = loggerFactory,
        metrics = config.metrics,
        config = rateLimit,
        additionalChecks = List(
          ThreadpoolCheck(
            name = "Environment Execution Threadpool",
            limit = rateLimit.maxApiServicesQueueSize,
            queue = executionContext,
            loggerFactory = loggerFactory,
          ),
          ThreadpoolCheck(
            name = "Index DB Threadpool",
            limit = rateLimit.maxApiServicesIndexDbQueueSize,
            queue = indexerExecutor,
            loggerFactory = loggerFactory,
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
    command = config.serverConfig.commandService,
    configurationLoadTimeout = config.serverConfig.configurationLoadTimeout.unwrap,
    managementServiceTimeout = config.serverConfig.managementServiceTimeout.underlying,
    maxInboundMessageSize = config.serverConfig.maxInboundMessageSize.unwrap,
    port = config.serverConfig.port,
    rateLimit = config.serverConfig.rateLimit,
    seeding = config.cantonParameterConfig.ledgerApiServerParameters.contractIdSeeding,
    timeProviderType = config.testingTimeService match {
      case Some(_) => TimeProviderType.Static
      case None => TimeProviderType.WallClock
    },
    tls = config.serverConfig.tls
      .map(LedgerApiServerConfig.ledgerApiServerTlsConfigFromCantonServerConfig),
    userManagement = config.serverConfig.userManagementService,
  )

  private def startHttpApiIfEnabled: ResourceOwner[Unit] =
    config.jsonApiConfig
      .fold(ResourceOwner.unit) { jsonApiConfig =>
        for {
          channel <- ResourceOwner
            .forReleasable(() =>
              ClientChannelBuilder.createChannelToTrustedServer(config.serverConfig.clientConfig)
            ) { channel => Future(channel.shutdown().discard) }
          _ <- HttpApiServer(jsonApiConfig, channel, loggerFactory)(config.jsonApiMetrics)
        } yield ()
      }
}
