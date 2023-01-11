// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.ledger.configuration.{LedgerId, LedgerTimeModel}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.*
import com.daml.platform.apiserver.meteringreport.MeteringReportKey
import com.daml.platform.indexer.{IndexerServiceOwner, IndexerStartupMode}
import com.daml.platform.store.DbSupport
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
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.config.{IndexerConfig, LedgerApiServerConfig}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.util.LoggingContextUtil
import com.digitalasset.canton.tracing.{NoTracing, TracerProvider}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{LedgerParticipantId, checked}
import io.grpc.BindableService

import java.time.Duration as JDuration
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Wrapper of ledger API server to manage start, stop, and erasing of state.
  */
object CantonLedgerApiServerWrapper extends NoTracing {
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

  /** Config for ledger API server and indexer
    *
    * @param serverConfig          ledger API server configuration
    * @param indexerConfig         indexer configuration
    * @param indexerLockIds        Optional lock IDs to be used for indexer HA
    * @param ledgerId              unique ledger id used by the ledger API server
    * @param participantId         unique participant id used e.g. for a unique ledger API server index db name
    * @param engine                daml engine shared with Canton for performance reasons
    * @param syncService           canton sync service implementing both read and write services
    * @param storageConfig         canton storage config so that indexer can share the participant db
    * @param cantonParameterConfig configurations meant to be overridden primarily in tests (applying to all participants)
    * @param testingTimeService    an optional service during testing for advancing time, participant-specific
    * @param adminToken            canton admin token for ledger api auth
    * @param loggerFactory         canton logger factory
    * @param tracerProvider        tracer provider for open telemetry grpc injection
    * @param metrics               upstream metrics module
    * @param envQueueSize          method to read the environment execution context queue size
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
      meteringReportKey: MeteringReportKey,
      envQueueName: String,
      envQueueSize: () => Long,
  ) extends NamedLogging {
    override def logger: TracedLogger = super.logger

  }

  /** Initialize a ledger API server asynchronously
    *
    * @param config ledger API server configuration
    * @param startLedgerApiServer whether to start the ledger API server or not
    *              (i.e. when participant node is initialized in passive mode)
    * @param createExternalServices A factory to create additional gRPC BindableService-s,
    *                               which will be bound to the Ledger API gRPC endpoint.
    *                               All the BindableService-s, which implement java.lang.AutoCloseable,
    *                               will be also closed upon Ledger API service teardown.
    * @return ledger API server state wrapper EitherT-future
    */
  def initialize(
      config: Config,
      startLedgerApiServer: Boolean,
      createExternalServices: () => List[BindableService] = () => Nil,
  )(implicit
      ec: ExecutionContextIdlenessExecutorService,
      actorSystem: ActorSystem,
  ): EitherT[Future, LedgerApiServerError, LedgerApiServerState] = {

    val ledgerApiStorageE = LedgerApiStorage.fromStorageConfig(
      config.storageConfig,
      config.participantId,
    )

    EitherT
      .fromEither[Future](ledgerApiStorageE)
      .flatMap { ledgerApiStorage =>
        val connectionPoolConfig = DbSupport.ConnectionPoolConfig(
          connectionPoolSize = config.storageConfig.maxConnectionsLedgerApiServer,
          connectionTimeout = config.serverConfig.databaseConnectionTimeout.toScala,
        )

        val dbConfig = DbSupport.DbConfig(
          jdbcUrl = ledgerApiStorage.jdbcUrl,
          connectionPool = connectionPoolConfig,
          postgres = config.serverConfig.postgresDataSource.damlConfig,
        )

        val participantDataSourceConfig =
          DbSupport.ParticipantDataSourceConfig(ledgerApiStorage.jdbcUrl)

        val startableStoppableLedgerApiServer =
          // Propagate NamedLoggerFactory's properties as map to upstream LoggingContext.
          LoggingContextUtil.createLoggingContext(config.loggerFactory) { implicit loggingContext =>
            new StartableStoppableLedgerApiServer(
              config = config,
              participantDataSourceConfig = participantDataSourceConfig,
              dbConfig = dbConfig,
              createExternalServices = createExternalServices,
            )
          }

        val startF = for {
          _ <- tryCreateSchema(ledgerApiStorage, config.logger)
          _ <-
            if (startLedgerApiServer)
              startableStoppableLedgerApiServer.start(overrideIndexerStartupMode =
                Some(IndexerStartupMode.MigrateOnEmptySchemaAndStart)
              )
            else Future.unit
        } yield ()

        EitherT(startF.transformWith {
          case Success(_) =>
            Future.successful(
              Either.right(
                LedgerApiServerState(
                  ledgerApiStorage,
                  startableStoppableLedgerApiServer,
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

  /** Config for indexer migrate schema entry point
    *
    * @param dbConfig          canton DB storage config so that indexer can share the participant db
    * @param additionalMigrationPaths optional paths for extra migration files
    */
  case class MigrateSchemaConfig(
      dbConfig: DbConfig,
      additionalMigrationPaths: Seq[String],
  )

  /** Migrates ledger API server database schema to latest flyway version
    */
  def migrateSchema(config: MigrateSchemaConfig, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext
  ): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val logger = loggerFactory.getTracedLogger(getClass)

    LoggingContextUtil.createLoggingContext(loggerFactory) { implicit loggingContext =>
      for {
        ledgerApiStorage <- LedgerApiStorage
          .fromDbConfig(config.dbConfig)
          .fold(t => Future.failed(t.asRuntimeException()), Future.successful)
        _ <- tryCreateSchema(ledgerApiStorage, logger)
        _ <- IndexerServiceOwner.migrateOnly(
          ledgerApiStorage.jdbcUrl,
          additionalMigrationPaths = config.additionalMigrationPaths,
        )
      } yield ()
    }
  }

  case class LedgerApiServerState(
      ledgerApiStorage: LedgerApiStorage,
      startableStoppableLedgerApi: StartableStoppableLedgerApiServer,
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
            _ <- EitherTUtil
              .fromFuture(
                startableStoppableLedgerApi.stop(),
                t => FailedToStopLedgerApiServer("Failed to stop ledger API server", t),
              )
            _ = startableStoppableLedgerApi.close()
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

  case class FailedToConfigureLedgerApiStorage(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override def pretty: Pretty[FailedToConfigureLedgerApiStorage] =
      prettyOfClass(unnamedParam(_.errorMessage.unquoted))
  }
}
