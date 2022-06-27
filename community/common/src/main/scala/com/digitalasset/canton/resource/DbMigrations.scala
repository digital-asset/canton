// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.retry.RetryEither
import io.functionmeta.functionFullName
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import slick.jdbc.{DataSourceJdbcDataSource, JdbcBackend, JdbcDataSource}

import java.sql.SQLException
import javax.sql.DataSource
import scala.concurrent.blocking

trait DbMigrationsFactory {

  def create(dbConfig: DbConfig, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations

  def create(dbConfig: DbConfig, name: String, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations

}

trait DbMigrations { this: NamedLogging =>

  implicit protected def closeContext: CloseContext

  /** Whether we want to add the schema files found in the dev folder to the migration
    *
    * A user that does that, won't be able to upgrade to new Canton versions, as we reserve our right to just
    * modify the dev version files in any way we like.
    */
  protected def devVersionSupport: Boolean

  /** Database is migrated using Flyway, which looks at the migration files at
    * src/main/resources/db/migration/canton as explained at https://flywaydb.org/documentation/getstarted/firststeps/api
    */
  protected def createFlyway(dataSource: DataSource): Flyway = {
    Flyway.configure
      .locations(dbConfig.buildMigrationsPaths(devVersionSupport): _*)
      .dataSource(dataSource)
      .cleanOnValidationError(dbConfig.cleanOnValidationError)
      .baselineOnMigrate(dbConfig.baselineOnMigrate)
      .lockRetryCount(60)
      .load()
  }

  protected def withCreatedDb[A](
      fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A]
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] = {
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
  protected def withDb[A](fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A]

  protected def migrateDatabaseInternal(
      db: Database
  )(implicit traceContext: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    val flyway = createFlyway(DbMigrations.createDataSource(db.source))
    // Retry the migration in case of failures, which may happen due to a race condition in concurrent migrations
    RetryEither.retry[DbMigrations.Error, Unit](10, 100, functionFullName, logger) {
      Either
        .catchOnly[FlywayException](flyway.migrate())
        .map(r => logger.info(s"Applied ${r.migrationsExecuted} migrations successfully"))
        .leftMap(DbMigrations.FlywayError)
    }
  }

  protected def repairFlywayMigrationInternal(
      db: Database
  )(implicit traceContext: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    val flyway = createFlyway(DbMigrations.createDataSource(db.source))
    Either
      .catchOnly[FlywayException](flyway.repair())
      .map(r =>
        logger.info(
          s"The repair of the Flyway database migration succeeded. This is the Flyway repair report: $r"
        )
      )
      .leftMap[DbMigrations.Error](DbMigrations.FlywayError)
      .toEitherT[UnlessShutdown]
  }

  protected def dbConfig: DbConfig

  /** Migrate the database with all pending migrations. */
  def migrateDatabase(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
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
  def repairFlywayMigration(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withDb(repairFlywayMigrationInternal)
    }

  protected def withFlyway[A](
      fn: (Database, Flyway) => EitherT[UnlessShutdown, DbMigrations.Error, A]
  )(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] =
    withDb { createdDb =>
      ResourceUtil.withResource(createdDb) { db =>
        val flyway = createFlyway(DbMigrations.createDataSource(db.source))
        fn(db, flyway)
      }
    }

  def connectionCheck(
      failFast: Boolean,
      processingTimeout: ProcessingTimeout,
  )(implicit tc: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    def attempt: EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
      withDb { createdDb =>
        ResourceUtil.withResource(createdDb) { db: JdbcBackend.Database =>
          //TODO(i9497): The DataSource could be created from the DbConfig, without first having to create the whole
          // Database. Swap to this more light-weight approach.
          val dataSource = db.source
          val conn = dataSource.createConnection()
          val valid = blocking {
            Either.catchOnly[SQLException](
              conn.isValid(processingTimeout.network.duration.toSeconds.toInt)
            )
          }

          valid
            .leftMap(err => show"failed to check connection $err")
            .flatMap { valid =>
              Either.cond(
                valid,
                (),
                "A trial database connection was not valid",
              )
            }
            .leftMap[DbMigrations.Error](err => DbMigrations.DatabaseError(err))
            .toEitherT[UnlessShutdown]
        }
      }
    }

    if (failFast) {
      attempt
    } else {
      // Repeatedly attempt to create a valid connection, so that the system waits for the database to come up
      // We must retry the whole `attempt` operation including the `withDb`, as `withDb` may itself fail if the
      // database is not up.
      val retryConfig = RetryConfig.forever
      RetryEither.retryUnlessShutdown[DbMigrations.Error, Unit](
        retryConfig.maxRetries,
        retryConfig.retryWaitingTime.toMillis,
        functionFullName,
        logger,
      )(attempt)
    }
  }

  /** Migrate a database if it is empty, otherwise skip the migration. */
  def migrateIfFresh(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withFlyway { case (db, flyway) =>
        migrateIfFreshInternal(db, flyway)
      }
    }

  private def migrateIfFreshInternal(db: Database, flyway: Flyway)(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    if (flyway.info().applied().isEmpty)
      migrateDatabaseInternal(db)
    else {
      logger.debug("Skip flyway migration on non-empty database")
      EitherT.rightT(())
    }
  }

  /** Combined method of migrateIfFresh and checkPendingMigration, avoids creating multiple pools */
  def migrateIfFreshAndCheckPending(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withFlyway { case (db, flyway) =>
        for {
          _ <- migrateIfFreshInternal(db, flyway)
          _ <- checkPendingMigrationInternal(flyway).toEitherT[UnlessShutdown]
        } yield ()
      }
    }

  def checkDbVersion(
      timeouts: ProcessingTimeout,
      standardConfig: Boolean,
  )(implicit tc: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    withDb { db =>
      val check = DbVersionCheck
        .dbVersionCheck(timeouts, standardConfig, dbConfig)
      check(db).toEitherT[UnlessShutdown]
    }

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
  override def create(dbConfig: DbConfig, name: String, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations =
    new CommunityDbMigrations(
      dbConfig,
      devVersionSupport,
      loggerFactory.appendUnnamedKey("node", name),
    )

  override def create(dbConfig: DbConfig, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations =
    new CommunityDbMigrations(dbConfig, devVersionSupport, loggerFactory)
}

class CommunityDbMigrations(
    protected val dbConfig: DbConfig,
    protected val devVersionSupport: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val closeContext: CloseContext)
    extends DbMigrations
    with NamedLogging {

  override protected def withDb[A](fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A])(
      implicit traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] = withCreatedDb(fn)
}

object DbMigrations {

  def createDataSource(jdbcDataSource: JdbcDataSource): DataSource =
    jdbcDataSource match {
      case dataS: DataSourceJdbcDataSource => dataS.ds
      case dataS: HikariCPJdbcDataSource => dataS.ds
      case unsupported =>
        // This should never happen
        sys.error(s"Data source not supported for migrations: ${unsupported.getClass}")
    }

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
