// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.implicits._
import com.codahale.metrics.SharedMetricRegistries
import com.daml.api.util.TimeProvider
import com.daml.caching
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.experimental_features.{
  CommandDeduplicationFeatures,
  CommandDeduplicationPeriodSupport,
  CommandDeduplicationType,
}
import com.daml.ledger.configuration.{LedgerId, LedgerTimeModel}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{
  ApiServerConfig,
  LedgerFeatures,
  StandaloneApiServer,
  StandaloneIndexService,
  TimeServiceBackend,
}
import com.daml.platform.configuration.{PartyConfiguration, ServerRole}
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.indexer.{
  IndexerStartupMode,
  StandaloneIndexerServer,
  IndexerConfig => DamlIndexerConfig,
}
import com.daml.platform.store.{DbSupport, DbType, LfValueTranslationCache}
import com.daml.platform.usermanagement.PersistentUserManagementStore
import com.daml.ports.{Port => DamlPort}
import com.daml.resources.FutureResourceOwner
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.networking.grpc.ApiRequestLogger
import com.digitalasset.canton.participant.config.{
  IndexerConfig,
  LedgerApiServerConfig,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.util.LoggingContextUtil
import com.digitalasset.canton.tracing.{NoTracing, TracerProvider}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LedgerParticipantId, checked}
import io.opentelemetry.instrumentation.grpc.v1_5.GrpcTracing

import java.time.{Duration => JDuration}
import java.util.UUID.randomUUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Wrapper of Ledger Api Server to manage start, stop, and erasing of state.
  */
object CantonLedgerApiServerWrapper extends NoTracing {

  type LedgerApiServerResource = Resource[Unit]
  type IndexerResource = Resource[Unit]

  // TODO(#3262): Once upstream supports multi-domain in Daml 2.0, configure maximum tolerance time model.
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  val maximumToleranceTimeModel: LedgerTimeModel = checked(
    LedgerTimeModel(
      avgTransactionLatency = JDuration.ZERO,
      minSkew = JDuration.ofDays(365),
      maxSkew = JDuration.ofDays(365),
    ).get
  )

  case class IndexerLockIds(mainLockId: Int, workerLockId: Int)

  /** Config for ledger api server and indexer
    *
    * @param serverConfig          ledger api server configuration
    * @param indexerConfig         indexer configuration
    * @param indexerLockIds        Optional lock IDs to be used for indexer HA
    * @param ledgerId              unique ledger id used by the ledger api server
    * @param participantId         unique participant id used e.g. for a unique ledger api server index db name
    * @param engine                daml engine shared with Canton for performance reasons
    * @param syncService           canton sync service implementing both read and write services
    * @param storageConfig         canton storage config so that indexer can share the participant db
    * @param cantonParameterConfig configurations meant to be overridden primarily in tests (applying to all participants)
    * @param testingTimeService    an optional service during testing for advancing time, participant-specific
    * @param adminToken            canton admin token for ledger api auth
    * @param loggerFactory         canton logger factory
    * @param tracerProvider        tracer provider for open telemetry grpc injection
    * @param metrics               upstream metrics module
    */
  case class Config(
      serverConfig: LedgerApiServerConfig,
      indexerConfig: IndexerConfig,
      indexerLockIds: Option[IndexerLockIds],
      ledgerId: LedgerId,
      participantId: LedgerParticipantId,
      engine: Engine,
      syncService: CantonSyncService,
      storageConfig: StorageConfig,
      cantonParameterConfig: ParticipantNodeParameters,
      testingTimeService: Option[TimeServiceBackend],
      adminToken: CantonAdminToken,
      override val loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
      metrics: Metrics,
  ) extends NamedLogging {
    override def logger: TracedLogger = super.logger

  }

  /** Start a ledger api server asynchronously
    * @param config ledger api server configuration
    * @param startIndexer whether to start the indexer or merely wait until the index db schema has been created
    * @return ledger api server state wrapper EitherT-future
    */
  def start(config: Config, startIndexer: Boolean)(implicit
      ec: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): EitherT[Future, LedgerApiServerError, LedgerApiServerState] = {

    implicit val context = ResourceContext(ec)

    def tryCreateSchemaAsResource(
        ledgerApiStorage: LedgerApiStorage,
        logger: TracedLogger,
    ): ResourceOwner[Unit] =
      new FutureResourceOwner(() => tryCreateSchema(ledgerApiStorage, logger))

    def validateConfig(
        config: Config
    ): Either[LedgerApiServerError, (LedgerApiStorage, DamlIndexerConfig)] =
      for {
        ledgerApiStorage <- LedgerApiStorage.fromStorageConfig(
          config.storageConfig,
          config.participantId,
        )
        asyncCommitMode <- asyncCommitMode(config.serverConfig.synchronousCommitMode)
          .leftMap(FailedToConfigureIndexer)
        indexerConfig = DamlIndexerConfig(
          participantId = config.participantId,
          jdbcUrl = ledgerApiStorage.jdbcUrl,
          startupMode = IndexerStartupMode.MigrateAndStart,
          databaseConnectionTimeout = config.serverConfig.databaseConnectionTimeout.toScala,
          restartDelay = config.indexerConfig.restartDelay.toScala,
          eventsPageSize = config.serverConfig.eventsPageSize,
          eventsProcessingParallelism = config.serverConfig.eventsProcessingParallelism,
          // updatePreparationParallelism not configurable as applicable only to pipelined indexer that will be removed
          allowExistingSchema = false,
          asyncCommitMode = asyncCommitMode,
          maxInputBufferSize = config.indexerConfig.maxInputBufferSize.unwrap,
          inputMappingParallelism = config.indexerConfig.inputMappingParallelism.unwrap,
          batchingParallelism = config.indexerConfig.batchingParallelism.unwrap,
          ingestionParallelism = config.indexerConfig.ingestionParallelism.unwrap,
          submissionBatchSize = config.indexerConfig.submissionBatchSize,
          tailingRateLimitPerSecond = config.indexerConfig.tailingRateLimitPerSecond.unwrap,
          batchWithinMillis = config.indexerConfig.batchWithinMillis,
          enableCompression = config.indexerConfig.enableCompression,
          schemaMigrationAttempts = config.indexerConfig.schemaMigrationAttempts,
          schemaMigrationAttemptBackoff =
            config.indexerConfig.schemaMigrationAttemptBackoff.toScala,
          haConfig = config.indexerLockIds.fold(HaConfig()) {
            case IndexerLockIds(mainLockId, workerLockId) =>
              HaConfig(indexerLockId = mainLockId, indexerWorkerLockId = workerLockId)
          },
          postgresTcpKeepalivesIdle = config.indexerConfig.postgresTcpKeepalivesIdle,
          postgresTcpKeepalivesInterval = config.indexerConfig.postgresTcpKeepalivesInterval,
          postgresTcpKeepalivesCount = config.indexerConfig.postgresTcpKeepalivesCount,
        )
      } yield (ledgerApiStorage, indexerConfig)

    EitherT
      .fromEither[Future](validateConfig(config))
      .flatMap { case (ledgerApiStorage, indexerConfig) =>
        val uniquifier = randomUUID.toString

        val ledgerApiServerConfig = ApiServerConfig(
          participantId = config.participantId,
          archiveFiles = List.empty,
          port = DamlPort(config.serverConfig.port.unwrap),
          address = Some(config.serverConfig.address),
          jdbcUrl = ledgerApiStorage.jdbcUrl,
          databaseConnectionPoolSize = config.storageConfig.maxConnectionsLedgerApiServer,
          databaseConnectionTimeout = config.serverConfig.databaseConnectionTimeout.toScala,
          tlsConfig = config.serverConfig.tls
            .map(LedgerApiServerConfig.ledgerApiServerTlsConfigFromCantonServerConfig),
          maxInboundMessageSize = config.serverConfig.maxInboundMessageSize.unwrap,
          initialLedgerConfiguration =
            None, // CantonSyncService provides ledger configuration via ReadService bypassing the WriteService
          configurationLoadTimeout =
            JDuration.ofNanos(config.serverConfig.configurationLoadTimeout.unwrap.toNanos),
          eventsPageSize = config.serverConfig.eventsPageSize,
          eventsProcessingParallelism = config.serverConfig.eventsProcessingParallelism,
          acsIdPageSize = config.serverConfig.activeContractsService.acsIdPageSize,
          acsIdFetchingParallelism =
            config.serverConfig.activeContractsService.acsIdFetchingParallelism,
          acsContractFetchingParallelism =
            config.serverConfig.activeContractsService.acsContractFetchingParallelism,
          acsGlobalParallelism = config.serverConfig.activeContractsService.acsGlobalParallelism,
          portFile = None,
          seeding = config.cantonParameterConfig.contractIdSeeding,
          managementServiceTimeout =
            JDuration.ofNanos(config.serverConfig.managementServiceTimeout.unwrap.toNanos),
          maxContractStateCacheSize = config.serverConfig.maxContractStateCacheSize,
          maxContractKeyStateCacheSize = config.serverConfig.maxContractKeyStateCacheSize,
          maxTransactionsInMemoryFanOutBufferSize =
            config.serverConfig.maxTransactionsInMemoryFanOutBufferSize,
          enableInMemoryFanOutForLedgerApi = config.serverConfig.enableInMemoryFanOutForLedgerApi,
          userManagementConfig = config.serverConfig.userManagementService.damlConfig,
        )

        // Propagate NamedLoggerFactory's properties as map to upstream LoggingContext.
        val (indexer, ledgerApiServerResource) =
          LoggingContextUtil.createLoggingContext(config.loggerFactory) { implicit loggingContext =>
            val metrics = new Metrics(
              SharedMetricRegistries.getOrCreate(s"${config.participantId}-$uniquifier")
            )
            val lfValueTranslationCache =
              LfValueTranslationCache.Cache.newInstrumentedInstance(
                eventConfiguration =
                  caching.SizedCache.Configuration(config.serverConfig.maxEventCacheWeight),
                contractConfiguration =
                  caching.SizedCache.Configuration(config.serverConfig.maxContractCacheWeight),
                metrics = metrics,
              )

            val indexerResource = for {
              _ <- tryCreateSchemaAsResource(ledgerApiStorage, config.logger).acquire()
              reportsHealth <- new StandaloneIndexerServer(
                config.syncService,
                indexerConfig.copy(
                  startupMode =
                    if (startIndexer) IndexerStartupMode.MigrateOnEmptySchemaAndStart
                    else IndexerStartupMode.ValidateAndWaitOnly
                ),
                metrics,
                lfValueTranslationCache,
                config.serverConfig.additionalMigrationPaths,
              ).acquire()
            } yield reportsHealth

            val startableStoppableIndexer =
              new StartableStoppableIndexer(
                indexerConfig,
                metrics,
                lfValueTranslationCache,
                config.syncService,
                // if we have started the indexer, pass resource to restartable indexer to allow stopping and starting
                // indexer independently from ledger api server
                if (startIndexer) Some(indexerResource) else None,
                config.serverConfig.additionalMigrationPaths,
                config.cantonParameterConfig.processingTimeouts,
                config.loggerFactory,
              )

            val ledgerApiServerResource = for {
              reportsIndexerHealth <-
                if (!startIndexer)
                  indexerResource // if indexer not starting, chain resource for release
                else
                  Resource.fromFuture(
                    indexerResource.asFuture
                  ) // otherwise begin ledger api server resource from scratch

              // Since there is no "getOrElse" on the daml Resource monad, use the `ReportsHealth` instance unpacked by
              // the previous flatMap for comprehension statement.
              _ = startableStoppableIndexer.setHealthReporter(Some(reportsIndexerHealth))

              dbSupport <- DbSupport
                .owner(
                  jdbcUrl = ledgerApiServerConfig.jdbcUrl,
                  serverRole = ServerRole.ApiServer,
                  connectionPoolSize = ledgerApiServerConfig.databaseConnectionPoolSize,
                  connectionTimeout = ledgerApiServerConfig.databaseConnectionTimeout,
                  metrics = metrics,
                )
                .acquire()

              userManagementStore =
                PersistentUserManagementStore.cached(
                  dbSupport = dbSupport,
                  metrics = metrics,
                  timeProvider = TimeProvider.UTC,
                  cacheExpiryAfterWriteInSeconds =
                    config.serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
                  maxCacheSize = config.serverConfig.userManagementService.maxCacheSize,
                  maxRightsPerUser = config.serverConfig.userManagementService.maxRightsPerUser,
                )(ec, loggingContext)

              indexService <- StandaloneIndexService(
                dbSupport = dbSupport,
                ledgerId = config.ledgerId,
                config = ledgerApiServerConfig,
                metrics = metrics,
                engine = config.engine,
                servicesExecutionContext = ec,
                lfValueTranslationCache = lfValueTranslationCache,
              ).acquire()

              _ <- StandaloneApiServer(
                indexService = indexService,
                userManagementStore = userManagementStore,
                ledgerId = config.ledgerId,
                config = ledgerApiServerConfig,
                commandConfig = config.serverConfig.commandService.damlConfig,
                partyConfig = PartyConfiguration.default,
                optWriteService = Some(config.syncService),
                authService = {
                  new CantonAdminTokenAuthService(
                    config.adminToken,
                    config.serverConfig.authServices.map(_.create()),
                  )
                },
                healthChecks = new HealthChecks(
                  "read" -> config.syncService,
                  "write" -> (() => config.syncService.currentWriteHealth()),
                  "startable-stoppable-indexer" -> startableStoppableIndexer.reportsHealthWrapper(),
                ),
                metrics = config.metrics,
                timeServiceBackend = config.testingTimeService,
                engine = config.engine,
                otherInterceptors = List(
                  new ApiRequestLogger(
                    config.loggerFactory,
                    config.cantonParameterConfig.loggingConfig.api,
                  ),
                  GrpcTracing
                    .newBuilder(config.tracerProvider.openTelemetry)
                    .build()
                    .newServerInterceptor(),
                ),
                servicesExecutionContext = ec,
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
                userManagementConfig = config.serverConfig.userManagementService.damlConfig,
              ).acquire()
            } yield ()

            (startableStoppableIndexer, ledgerApiServerResource)
          }

        EitherT(ledgerApiServerResource.asFuture.transformWith {
          case Success(_) =>
            Future.successful(
              Either.right(
                LedgerApiServerState(
                  ledgerApiStorage,
                  ledgerApiServerResource,
                  indexer,
                  config.logger,
                  config.cantonParameterConfig.processingTimeouts,
                )(ec)
              )
            )
          case Failure(e) => Future.successful(Left(FailedToStartLedgerApiServer(e)))
        })
      }
  }

  private def tryCreateSchema(ledgerApiStorage: LedgerApiStorage, logger: TracedLogger)(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    Future {
      logger.debug(s"Trying to create schema for ledger API server...")
      ledgerApiStorage.createSchema().valueOr { err =>
        logger.error(s"Failed to create schema for ledger API server", err)
      }
    }

  private def asyncCommitMode(mode: String): Either[String, DbType.AsyncCommitMode] =
    mode.toUpperCase() match {
      case DbType.SynchronousCommit.setting => Right(DbType.SynchronousCommit)
      case DbType.LocalSynchronousCommit.setting => Right(DbType.LocalSynchronousCommit)
      case DbType.AsynchronousCommit.setting => Right(DbType.AsynchronousCommit)
      case unknownMode => Left(s"Unsupported synchronous_commit mode: $unknownMode")
    }

  /** Config for indexer migrate schema entry point
    *
    * @param dbConfig          canton DB storage config so that indexer can share the participant db
    * @param additionalMigrationPaths optional paths for extra migration files
    */
  case class MigrateSchemaConfig(
      dbConfig: DbConfig,
      additionalMigrationPaths: Seq[String],
  )

  /** Migrates ledger api server database schema to latest flyway version
    */
  def migrateSchema(config: MigrateSchemaConfig, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    implicit val context = ResourceContext(ec)

    val logger = loggerFactory.getTracedLogger(getClass)

    LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      for {
        ledgerApiStorage <- LedgerApiStorage
          .fromDbConfig(config.dbConfig)
          .fold(t => Future.failed(t.asRuntimeException()), Future.successful)
        _ <- tryCreateSchema(ledgerApiStorage, logger)
        _ <- StandaloneIndexerServer.migrateOnly(
          ledgerApiStorage.jdbcUrl,
          additionalMigrationPaths = config.additionalMigrationPaths,
        )
      } yield ()
    }
  }

  def stop(
      components: LedgerApiServerState
  )(implicit executionContext: ExecutionContext): EitherT[Future, LedgerApiServerError, Unit] = {
    val LedgerApiServerState(_ledgerApiStorage, ledgerApiServer, _indexer, logger, timeouts) =
      components
    logger.debug("About to stop ledger api server")
    EitherT(
      ledgerApiServer
        .release()
        .transformWith {
          case Failure(t) =>
            logger.warn("Failed to stop ledger api server", t)
            Future
              .successful(Left(FailedToStopLedgerApiServer("Failed to stop ledger API server", t)))
          case Success(_) =>
            logger.debug("Successfully stopped ledger api server")
            Future.successful(Right(()))
        }(executionContext)
    )
  }

  case class LedgerApiServerState(
      ledgerApiStorage: LedgerApiStorage,
      ledgerApiServer: LedgerApiServerResource,
      indexer: StartableStoppableIndexer,
      override protected val logger: TracedLogger,
      protected override val timeouts: ProcessingTimeout,
  )(implicit ec: ExecutionContext)
      extends FlagCloseableAsync {

    override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
      implicit val loggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      List(
        AsyncCloseable(
          "ledgerApiServerWrapper",
          (for {
            _ <- CantonLedgerApiServerWrapper.stop(this)
            _ <- EitherTUtil
              .fromFuture(indexer.stop(), t => FailedToStopIndexer("Failed to stop indexer", t))
            _ = indexer.close()
            _ <- EitherT.fromEither[Future](ledgerApiStorage.close()): EitherT[
              Future,
              LedgerApiServerError,
              Unit,
            ]
          } yield ()).valueOr(e => throw e.asRuntimeException()),
          1.minute,
        )
      )
    }

    override def toString: String = getClass.getSimpleName
  }

  sealed trait LedgerApiServerError extends Product with Serializable with PrettyPrinting {
    protected def errorMessage: String = ""
    def cause: Throwable
    def asRuntimeException(additionalMessage: String = ""): RuntimeException =
      new RuntimeException(
        if (additionalMessage.isEmpty) errorMessage else s"$additionalMessage $errorMessage",
        cause,
      )
  }
  sealed trait LedgerApiServerErrorWithoutCause extends LedgerApiServerError {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    override def cause: Throwable = null
  }

  case class FailedToStartLedgerApiServer(cause: Throwable) extends LedgerApiServerError {
    override def pretty: Pretty[FailedToStartLedgerApiServer] = prettyOfClass(unnamedParam(_.cause))
  }

  case class FailedToStopLedgerApiServer(
      override protected val errorMessage: String,
      cause: Throwable,
  ) extends LedgerApiServerError {
    override def pretty: Pretty[FailedToStopLedgerApiServer] =
      prettyOfClass(param("error", _.errorMessage.unquoted), param("cause", _.cause))
  }

  case class FailedToStopIndexer(
      override protected val errorMessage: String,
      override val cause: Throwable,
  ) extends LedgerApiServerError {
    override def pretty: Pretty[FailedToStopIndexer] =
      prettyOfClass(param("error", _.errorMessage.unquoted), param("cause", _.cause))
  }

  case class FailedToConfigureLedgerApiStorage(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override def pretty: Pretty[FailedToConfigureLedgerApiStorage] =
      prettyOfClass(unnamedParam(_.errorMessage.unquoted))
  }

  case class FailedToConfigureIndexer(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override def pretty: Pretty[FailedToConfigureIndexer] = prettyOfClass(
      unnamedParam(_.errorMessage.unquoted)
    )
  }
}
