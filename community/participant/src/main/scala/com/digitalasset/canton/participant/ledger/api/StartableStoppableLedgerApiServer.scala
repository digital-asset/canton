// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
}
import com.daml.ledger.participant.state.v2.metrics.{TimedReadService, TimedWriteService}
import com.daml.ledger.resources.{Resource, ResourceContext}
import com.daml.logging.LoggingContext
import com.daml.platform.LedgerApiServer
import com.daml.platform.apiserver.{ApiServerConfig, ApiServiceOwner, LedgerFeatures}
import com.daml.platform.configuration.{IndexServiceConfig => LedgerIndexServiceConfig, ServerRole}
import com.daml.platform.index.IndexServiceOwner
import com.daml.platform.indexer.{
  IndexerConfig => DamlIndexerConfig,
  IndexerServiceOwner,
  IndexerStartupMode,
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.usermanagement.PersistentUserManagementStore
import com.daml.ports.Port
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ApiRequestLogger
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SimpleExecutionQueue}
import io.functionmeta.functionFullName
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTracing

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.jdk.DurationConverters._

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
)(implicit
    executionContext: ExecutionContextIdlenessExecutorService,
    actorSystem: ActorSystem,
    loggingContext: LoggingContext,
) extends FlagCloseableAsync
    with NamedLogging {

  // Use a simple execution queue as locking to ensure only one start and stop run at a time.
  private val execQueue = new SimpleExecutionQueue()

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
  ): Future[Unit] =
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
  def stop()(implicit traceContext: TraceContext): Future[Unit] =
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
        AsyncCloseable("ledger API server", stop(), timeouts.shutdownNetwork.duration)
      )
    }

  private def buildLedgerApiServerOwner(
      overrideIndexerStartupMode: Option[IndexerStartupMode]
  )(implicit traceContext: TraceContext) = {
    val indexServiceConfig = LedgerIndexServiceConfig(
      eventsPageSize = config.serverConfig.eventsPageSize,
      eventsProcessingParallelism = config.serverConfig.eventsProcessingParallelism,
      acsIdPageSize = config.serverConfig.activeContractsService.acsIdPageSize,
      acsIdPageBufferSize = config.serverConfig.activeContractsService.acsIdPageBufferSize,
      acsIdFetchingParallelism =
        config.serverConfig.activeContractsService.acsIdFetchingParallelism,
      acsContractFetchingParallelism =
        config.serverConfig.activeContractsService.acsContractFetchingParallelism,
      acsGlobalParallelism = config.serverConfig.activeContractsService.acsGlobalParallelism,
      maxContractStateCacheSize = config.serverConfig.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = config.serverConfig.maxContractKeyStateCacheSize,
      maxTransactionsInMemoryFanOutBufferSize =
        config.serverConfig.maxTransactionsInMemoryFanOutBufferSize,
      apiStreamShutdownTimeout = config.serverConfig.apiStreamShutdownTimeout.duration.toScala,
      inMemoryStateUpdaterParallelism = config.serverConfig.inMemoryStateUpdaterParallelism,
      inMemoryFanOutThreadPoolSize = config.serverConfig.inMemoryFanOutThreadPoolSize.getOrElse(
        LedgerApiServerConfig.DefaultInMemoryFanOutThreadPoolSize
      ),
      preparePackageMetadataTimeOutWarning =
        config.serverConfig.preparePackageMetadataTimeOutWarning.duration.toScala,
    )

    val indexerConfig = config.indexerConfig.damlConfig(
      indexerLockIds = config.indexerLockIds,
      dataSourceProperties = Some(
        DbSupport.DataSourceProperties(
          connectionPool = DamlIndexerConfig
            .createDataSourceProperties(config.indexerConfig.ingestionParallelism.unwrap)
            .connectionPool
            .copy(connectionTimeout = config.serverConfig.databaseConnectionTimeout.toScala),
          postgres = config.serverConfig.postgresDataSource.damlConfig,
        )
      ),
    )

    for {
      (inMemoryState, inMemoryStateUpdaterFlow) <-
        LedgerApiServer.createInMemoryStateAndUpdater(
          indexServiceConfig,
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
      )
      userManagementStore =
        PersistentUserManagementStore.cached(
          dbSupport = dbSupport,
          metrics = config.metrics,
          timeProvider = TimeProvider.UTC,
          cacheExpiryAfterWriteInSeconds =
            config.serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
          maxCacheSize = config.serverConfig.userManagementService.maxCacheSize,
          maxRightsPerUser = config.serverConfig.userManagementService.maxRightsPerUser,
        )(executionContext, loggingContext)
      ledgerApiServerConfig = ApiServerConfig(
        address = Some(config.serverConfig.address),
        apiStreamShutdownTimeout = config.serverConfig.apiStreamShutdownTimeout.unwrap.toScala,
        command = config.serverConfig.commandService.damlConfig,
        configurationLoadTimeout = config.serverConfig.configurationLoadTimeout.toScala,
        initialLedgerConfiguration =
          None, // CantonSyncService provides ledger configuration via ReadService bypassing the WriteService
        managementServiceTimeout = config.serverConfig.managementServiceTimeout.toScala,
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

      timedWriteService = new TimedWriteService(config.syncService, config.metrics)
      _ <- ApiServiceOwner(
        indexService = indexService,
        userManagementStore = userManagementStore,
        ledgerId = config.ledgerId,
        participantId = config.participantId,
        config = ledgerApiServerConfig,
        optWriteService = Some(timedWriteService),
        healthChecks = new HealthChecks(
          "read" -> timedReadService,
          "write" -> (() => config.syncService.currentWriteHealth()),
          "indexer" -> indexerHealth,
        ),
        metrics = config.metrics,
        timeServiceBackend = config.testingTimeService,
        otherServices = Nil,
        otherInterceptors = List(
          new ApiRequestLogger(
            config.loggerFactory,
            config.cantonParameterConfig.loggingConfig.api,
          ),
          GrpcTracing
            .builder(config.tracerProvider.openTelemetry)
            .build()
            .newServerInterceptor(),
        ),
        engine = config.engine,
        servicesExecutionContext = executionContext,
        checkOverloaded = config.syncService.checkOverloaded,
        ledgerFeatures = LedgerFeatures(
          staticTime = config.testingTimeService.isDefined,
          CommandDeduplicationFeatures.of(
            Some(
              CommandDeduplicationPeriodSupport.of(
                offsetSupport =
                  CommandDeduplicationPeriodSupport.OffsetSupport.OFFSET_NATIVE_SUPPORT,
                durationSupport =
                  CommandDeduplicationPeriodSupport.DurationSupport.DURATION_CONVERT_TO_OFFSET,
              )
            ),
            CommandDeduplicationType.ASYNC_AND_CONCURRENT_SYNC,
            maxDeduplicationDurationEnforced = false,
          ),
        ),
        authService = new CantonAdminTokenAuthService(
          config.adminToken,
          parent = config.serverConfig.authServices.map(
            _.create(
              config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway
            )
          ),
        ),
        jwtTimestampLeeway =
          config.cantonParameterConfig.ledgerApiServerParameters.jwtTimestampLeeway,
      )
    } yield ()
  }
}
