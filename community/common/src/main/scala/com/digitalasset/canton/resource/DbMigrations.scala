// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either._
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.util.retry.RetryEither
import io.functionmeta.functionFullName
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import slick.jdbc.{DataSourceJdbcDataSource, JdbcBackend, JdbcDataSource}

import javax.sql.DataSource
import scala.concurrent.blocking
import scala.util.Try

trait DbMigrationsFactory {

  def create(dbConfig: DbConfig)(implicit closeContext: CloseContext): DbMigrations

  def create(dbConfig: DbConfig, name: String)(implicit closeContext: CloseContext): DbMigrations

}

trait DbMigrations { this: NamedLogging =>

  implicit protected def closeContext: CloseContext

  protected def createDataSource(jdbcDataSource: JdbcDataSource): DataSource =
    jdbcDataSource match {
      case dataS: DataSourceJdbcDataSource => dataS.ds
      case dataS: HikariCPJdbcDataSource => dataS.ds
      case unsupported =>
        // This should never happen
        sys.error(s"Data source not supported for migrations: ${unsupported.getClass}")
    }

  /** Database is migrated using Flyway, which looks at the migration files at
    * src/main/resources/db/migration/canton as explained at https://flywaydb.org/documentation/getstarted/firststeps/api
    */
  protected def createFlyway(dataSource: DataSource): Flyway = {
    Flyway.configure
      .locations(dbConfig.migrationsPaths: _*)
      .dataSource(dataSource)
      .cleanOnValidationError(dbConfig.cleanOnValidationError)
      .baselineOnMigrate(dbConfig.baselineOnMigrate)
      .lockRetryCount(60)
      .load()
  }

  protected def withCreatedDb[A](
      fn: Database => Either[DbMigrations.Error, A]
  ): Either[DbMigrations.Error, A] = {
    DbStorage
      .createDatabase(
        dbConfig,
        forParticipant =
          false, // no need to adjust the connection pool for participants, as we are not yet running the ledger api server
        withWriteConnectionPool = false,
        withMainConnection = false,
        forMigration = true,
      )(loggerFactory)
      .leftMap(DbMigrations.DatabaseError)
      .flatMap(db => ResourceUtil.withResource(db)(fn))
  }

  /** Obtain access to the database to run the migration operation. */
  protected def withDb[A](fn: Database => Either[DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): Either[DbMigrations.Error, A]

  protected def migrateDatabaseInternal(
      db: Database
  )(implicit traceContext: TraceContext): Either[DbMigrations.Error, Unit] = {
    val flyway = createFlyway(createDataSource(db.source))
    // Retry the migration in case of failures, which may happen due to a race condition in concurrent migrations
    RetryEither[DbMigrations.Error, Unit](10, 100, functionFullName, logger) {
      Either
        .catchOnly[FlywayException](flyway.migrate())
        .map(r => logger.info(s"Applied ${r.migrationsExecuted} migrations successfully"))
        .leftMap(DbMigrations.FlywayError)
    }
  }

  protected def repairFlywayMigrationInternal(
      db: Database
  )(implicit traceContext: TraceContext): Either[DbMigrations.Error, Unit] = {
    val flyway = createFlyway(createDataSource(db.source))
    Either
      .catchOnly[FlywayException](flyway.repair())
      .map(r =>
        logger.info(
          s"The repair of the Flyway database migration succeeded. This is the Flyway repair report: $r"
        )
      )
      .leftMap(DbMigrations.FlywayError)
  }

  protected def dbConfig: DbConfig

  /** Migrate the database with all pending migrations. */
  def migrateDatabase(): Either[DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withDb(migrateDatabaseInternal)
    }

  /** Repair the database in case the migrations files changed (e.g. due to comment changes)
    * To quote the Flyway documentation:
    * ```
    * Repair is your tool to fix issues with the schema history table. It has a few main uses:
    *
    * - Remove failed migration entries (only for databases that do NOT support DDL transactions)
    * - Realign the checksums, descriptions, and types of the applied migrations with the ones of the available migrations
    * - Mark all missing migrations as deleted
    * ```
    */
  def repairFlywayMigration(): Either[DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withDb(repairFlywayMigrationInternal)
    }

  protected def withFlyway[A](fn: (Database, Flyway) => Either[DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): Either[DbMigrations.Error, A] =
    withDb { createdDb =>
      ResourceUtil.withResource(createdDb) { db =>
        val flyway = createFlyway(createDataSource(db.source))
        fn(db, flyway)
      }
    }

  def connectionCheck(
      failFast: Boolean,
      processingTimeout: ProcessingTimeout,
  )(implicit tc: TraceContext): Either[DbMigrations.DatabaseError, Unit] = {
    def attempt: Either[String, Unit] = Try {
      withDb(
        { createdDb =>
          ResourceUtil.withResource(createdDb) { db: JdbcBackend.Database =>
            //TODO(phoebe): The DataSource could be created from the DbConfig, without first having to create the whole
            // Database. Swap to this more light-weight approach.
            val dataSource = db.source
            val conn = dataSource.createConnection()
            val valid = blocking {
              conn.isValid(processingTimeout.network.duration.toSeconds.toInt)
            }
            if (valid) Right(())
            else Left(DbMigrations.DatabaseError(s"A trial database connection was not valid"))
          }
        }
      ).leftMap(err => err.toString)
    }.toEither.fold(throwable => Left(throwable.toString), identity)

    if (failFast) { attempt.leftMap(DbMigrations.DatabaseError) }
    else {
      // Repeatedly attempt to create a valid connection, so that the system waits for the database to come up
      // We must retry the whole `attempt` operation including the `withDb`, as `withDb` may itself fail if the
      // database is not up.
      val retryConfig = RetryConfig.forever
      val res = RetryEither[String, Unit](
        retryConfig.maxRetries,
        retryConfig.retryWaitingTime.toMillis,
        functionFullName,
        logger,
      )(attempt)
      res.leftMap(DbMigrations.DatabaseError)
    }
  }

  /** Migrate a database if it is empty, otherwise skip the migration. */
  def migrateIfFresh(): Either[DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withFlyway { case (db, flyway) =>
        migrateIfFreshInternal(db, flyway)
      }
    }

  private def migrateIfFreshInternal(db: Database, flyway: Flyway)(implicit
      traceContext: TraceContext
  ): Either[DbMigrations.Error, Unit] = {
    if (flyway.info().applied().isEmpty)
      migrateDatabaseInternal(db)
    else {
      logger.debug("Skip flyway migration on non-empty database")
      Right(())
    }
  }

  /** Combined method of migrateIfFresh and checkPendingMigration, avoids creating multiple pools */
  def migrateIfFreshAndCheckPending(): Either[DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withFlyway { case (db, flyway) =>
        for {
          _ <- migrateIfFreshInternal(db, flyway)
          _ <- checkPendingMigrationInternal(flyway)
        } yield ()
      }
    }

  def checkDbVersion(
      timeouts: ProcessingTimeout,
      standardConfig: Boolean,
  )(implicit tc: TraceContext): Either[DbMigrations.Error, Unit] =
    withDb(DbVersionCheck.dbVersionCheck(timeouts, standardConfig, dbConfig))

  private def checkPendingMigrationInternal(
      flyway: Flyway
  ): Either[DbMigrations.Error, Unit] = {
    for {
      info <- Either
        .catchOnly[FlywayException](flyway.info())
        .leftMap(DbMigrations.FlywayError)
      pendingMigrations = info.pending()
      _ <-
        if (pendingMigrations.isEmpty) Right(())
        else {
          val currentVersion = Option(info.current()).map(_.getVersion.getVersion)
          val lastVersion = pendingMigrations.last.getVersion.getVersion
          val pendingMsg =
            s"There are ${pendingMigrations.length} pending migrations to get to database schema version $lastVersion"
          val msg =
            currentVersion.fold(s"No migrations have been applied yet. $pendingMsg.")(version =>
              s"$pendingMsg. Currently on version $version."
            )
          Left(DbMigrations.PendingMigrationError(msg))
        }
    } yield ()
  }

}

class CommunityDbMigrationsFactory(loggerFactory: NamedLoggerFactory) extends DbMigrationsFactory {
  override def create(dbConfig: DbConfig, name: String)(implicit
      closeContext: CloseContext
  ): DbMigrations =
    new CommunityDbMigrations(dbConfig, loggerFactory.appendUnnamedKey("node", name))

  override def create(dbConfig: DbConfig)(implicit closeContext: CloseContext): DbMigrations =
    new CommunityDbMigrations(dbConfig, loggerFactory)
}

class CommunityDbMigrations(
    protected val dbConfig: DbConfig,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val closeContext: CloseContext)
    extends DbMigrations
    with NamedLogging {

  override protected def withDb[A](fn: Database => Either[DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): Either[DbMigrations.Error, A] = withCreatedDb(fn)
}

object DbMigrations {
  sealed trait Error extends PrettyPrinting
  case class FlywayError(err: FlywayException) extends Error {
    override def pretty: Pretty[FlywayError] = prettyOfClass(unnamedParam(_.err))
  }
  case class PendingMigrationError(msg: String) extends Error {
    override def pretty: Pretty[PendingMigrationError] = prettyOfClass(unnamedParam(_.msg.unquoted))
  }
  case class DatabaseError(error: String) extends Error {
    override def pretty: Pretty[DatabaseError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  case class DatabaseVersionError(error: String) extends Error {
    override def pretty: Pretty[DatabaseVersionError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  case class DatabaseConfigError(error: String) extends Error {
    override def pretty: Pretty[DatabaseConfigError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}
