// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either._
import cats.syntax.foldable._
import cats.{Applicative, Id}
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{
  DbConfig,
  LocalNodeConfig,
  LocalNodeParameters,
  StorageConfig,
}
import com.digitalasset.canton.domain.config.{DomainConfig, DomainNodeParameters}
import com.digitalasset.canton.domain.{Domain, DomainNodeBootstrap}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{
  LocalParticipantConfig,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper.MigrateSchemaConfig
import com.digitalasset.canton.participant.{ParticipantNode, ParticipantNodeBootstrap}
import com.digitalasset.canton.resource.{DbMigrations, DbMigrationsFactory}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil

import scala.Function.tupled
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Try

/** Group of CantonNodes of the same type (domains, participants, sequencers). */
trait Nodes[+Node <: CantonNode, +NodeBootstrap <: CantonNodeBootstrap[Node]]
    extends AutoCloseable {

  /** Start all configured nodes but stop on first error */
  def attemptStartAll(implicit traceContext: TraceContext): Either[StartupError, Unit]

  /** Start all nodes regardless if some fail */
  def startAll(implicit traceContext: TraceContext): Either[Seq[StartupError], Unit]

  /** Start an individual node by name */
  def start(name: String)(implicit traceContext: TraceContext): Either[StartupError, NodeBootstrap]

  /** Is the named node running? */
  def isRunning(name: String): Boolean

  /** Get the single running node */
  def getRunning(name: String): Option[NodeBootstrap]

  /** Stop the named node */
  def stop(name: String): Either[ShutdownError, Unit]

  /** Get nodes that are currently running */
  def running: Seq[NodeBootstrap]

  /** Independently run any pending database migrations for the named node */
  def migrateDatabase(name: String): Either[StartupError, Unit]

  /** Independently repair the Flyway schema history table for the named node to reset Flyway migration checksums etc */
  def repairDatabaseMigration(name: String): Either[StartupError, Unit]
}

/** Nodes group that can start nodes with the provided configuration and factory */
class ManagedNodes[
    Node <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    NodeParameters <: LocalNodeParameters,
    NodeBootstrap <: CantonNodeBootstrap[Node],
](
    create: (String, NodeConfig) => NodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    configs: Map[String, NodeConfig],
    parametersFor: String => LocalNodeParameters,
    protected val loggerFactory: NamedLoggerFactory,
) extends Nodes[Node, NodeBootstrap]
    with NamedLogging {

  // this is a mutable collections so modifications must be synchronized
  // (this may not be necessary if all calls are happening from the same startup thread or console)
  private val nodes = mutable.Map[String, NodeBootstrap]()

  override def running: Seq[NodeBootstrap] = synchronized {
    nodes.values.toSeq
  }

  override def attemptStartAll(implicit traceContext: TraceContext): Either[StartupError, Unit] =
    configs.toList.traverse_(tupled(start(_, _)))

  override def startAll(implicit traceContext: TraceContext): Either[Seq[StartupError], Unit] = {
    configs
      .map(tupled(start(_, _)))
      .toList
      .foldLeft[Either[Seq[StartupError], Unit]](Right(())) {
        case (results, Right(_)) => results
        case (Left(errors), Left(error)) => Left(errors :+ error)
        case (Right(_), Left(error)) => Left(Seq(error))
      }
  }

  override def start(
      name: String
  )(implicit traceContext: TraceContext): Either[StartupError, NodeBootstrap] = {
    configs
      .get(name)
      .toRight(ConfigurationNotFound(name): StartupError)
      .flatMap(start(name, _))
  }

  private def start(name: String, config: NodeConfig)(implicit
      traceContext: TraceContext
  ): Either[StartupError, NodeBootstrap] = synchronized {
    for {
      instance <- nodes.get(name) match {
        case Some(instance) => Right(instance)
        case None =>
          val failFast = config.init.startupFailFast
          val params = parametersFor(name)
          for {
            _ <- checkMigration(name, config.storage, failFast, params)
            instance = create(name, config)
            // we call start which will perform the asynchronous startup
            _ <- Try(
              params.processingTimeouts.unbounded.await(s"Starting node $name")(
                instance.start().value
              )
            )
              // intentionally rethrowing unhandled exception including timeouts as the node may be in a corrupted partially started state
              .fold(
                ex => throw ex,
                _.leftMap { error =>
                  instance
                    .close() // clean up resources allocated during instance creation (e.g., db)
                  StartFailed(name, error)
                },
              )
            // register the running instance
            _ = nodes.put(name, instance)
          } yield instance
      }
    } yield instance
  }

  override def migrateDatabase(name: String): Either[StartupError, Unit] = synchronized {
    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name): StartupError)
      _ <- checkNotRunning(name)
      _ <- runMigration(name, config.storage)
    } yield ()
  }

  override def repairDatabaseMigration(name: String): Either[StartupError, Unit] = synchronized {
    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name): StartupError)
      _ <- checkNotRunning(name)
      _ <- runRepairMigration(name, config.storage)
    } yield ()
  }

  override def isRunning(name: String): Boolean = synchronized {
    nodes.contains(name)
  }

  override def getRunning(name: String): Option[NodeBootstrap] = nodes.get(name)

  override def stop(name: String): Either[ShutdownError, Unit] =
    for {
      _ <- configs.get(name).toRight[ShutdownError](ConfigurationNotFound(name))
    } yield synchronized {
      nodes.remove(name).foreach { instance =>
        Lifecycle.close(instance)(logger)
      }
    }

  override def close(): Unit = synchronized {
    val runningInstances = nodes.values.toList
    nodes.clear()
    Lifecycle.close(runningInstances: _*)(logger)
  }

  protected def runIfUsingDatabase[F[_]](storageConfig: StorageConfig)(
      fn: DbConfig => F[Either[StartupError, Unit]]
  )(implicit F: Applicative[F]): F[Either[StartupError, Unit]] = storageConfig match {
    case dbConfig: DbConfig => fn(dbConfig)
    case _ => F.pure(Right(()))
  }

  // if database is fresh, we will migrate it. Otherwise, we will check if there is any pending migrations,
  // which need to be triggered manually.
  private def checkMigration(
      name: String,
      storageConfig: StorageConfig,
      failFastIfDbOut: Boolean,
      params: LocalNodeParameters,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig: DbConfig =>
      val migrations = migrationsFactory.create(dbConfig, name)
      import TraceContext.Implicits.Empty._
      logger.info(s"Setting up database schemas for $name")
      val started = System.nanoTime()
      val standardConfig = !params.nonStandardConfig

      def errorMapping(err: DbMigrations.Error): StartupError = {
        err match {
          case DbMigrations.PendingMigrationError(msg) => PendingDatabaseMigration(name, msg)
          case err: DbMigrations.FlywayError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseError => FailedDatabaseMigration(name, err)
          case err: DbMigrations.DatabaseVersionError => FailedDatabaseVersionChecks(name, err)
          case err: DbMigrations.DatabaseConfigError => FailedDatabaseConfigChecks(name, err)
        }
      }

      for {
        _ <- migrations
          .connectionCheck(failFastIfDbOut, params.processingTimeouts)
          .leftMap(
            FailedDatabaseMigration(name, _)
          )
        _ <- migrations
          .checkDbVersion(params.processingTimeouts, standardConfig)
          .leftMap(errorMapping)
        _ <- migrations.migrateIfFreshAndCheckPending().leftMap(errorMapping)
      } yield {
        val elapsed = System.nanoTime() - started
        logger.debug(
          s"Finished setting up database schemas after ${LoggerUtil
            .roundDurationForHumans(Duration.fromNanos(elapsed))}"
        )
      }
    }

  private def checkNotRunning(name: String): Either[StartupError, Unit] =
    if (isRunning(name)) Left(AlreadyRunning(name))
    else Right(())

  private def runMigration(name: String, storageConfig: StorageConfig): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig: DbConfig =>
      migrationsFactory
        .create(dbConfig, name)
        .migrateDatabase()
        .leftMap(FailedDatabaseMigration(name, _))
    }

  private def runRepairMigration(
      name: String,
      storageConfig: StorageConfig,
  ): Either[StartupError, Unit] =
    runIfUsingDatabase[Id](storageConfig) { dbConfig: DbConfig =>
      migrationsFactory
        .create(dbConfig, name)
        .repairFlywayMigration()
        .leftMap(FailedDatabaseRepairMigration(name, _))
    }
}

class ParticipantNodes[PC <: LocalParticipantConfig](
    create: (String, PC) => ParticipantNodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    configs: Map[String, PC],
    parametersFor: String => ParticipantNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContextIdlenessExecutorService
) extends ManagedNodes[ParticipantNode, PC, ParticipantNodeParameters, ParticipantNodeBootstrap](
      create,
      migrationsFactory,
      configs,
      parametersFor,
      loggerFactory,
    ) {
  private def migrateIndexerDatabase(name: String): Either[StartupError, Unit] = {
    import TraceContext.Implicits.Empty._

    for {
      config <- configs.get(name).toRight(ConfigurationNotFound(name))
      parameters = parametersFor(name)
      _ = parameters.processingTimeouts.unbounded.await() {
        runIfUsingDatabase[Future](config.storage) { dbConfig: DbConfig =>
          CantonLedgerApiServerWrapper
            .migrateSchema(
              MigrateSchemaConfig(
                dbConfig,
                config.ledgerApi.additionalMigrationPaths,
              ),
              loggerFactory,
            )
            .map(_.asRight)
        }
      }
    } yield ()
  }

  override def migrateDatabase(name: String): Either[StartupError, Unit] =
    for {
      _ <- super.migrateDatabase(name)
      _ <- migrateIndexerDatabase(name)
    } yield ()
}

class DomainNodes[DC <: DomainConfig](
    create: (String, DC) => DomainNodeBootstrap,
    migrationsFactory: DbMigrationsFactory,
    configs: Map[String, DC],
    parameters: String => DomainNodeParameters,
    loggerFactory: NamedLoggerFactory,
)(implicit
    protected val executionContext: ExecutionContextIdlenessExecutorService
) extends ManagedNodes[Domain, DC, DomainNodeParameters, DomainNodeBootstrap](
      create,
      migrationsFactory,
      configs,
      parameters,
      loggerFactory,
    )
