// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.syntax.either._
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.{Profile, RetryConfig}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.RetryEither
import com.digitalasset.canton.util.{LoggerUtil, ResourceUtil}
import io.functionmeta.functionFullName
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.actionBasedSQLInterpolationCanton
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
  )(implicit tc: TraceContext): Either[DbMigrations.Error, Unit] = {

    withDb { db =>
      logger.debug(s"Performing version checks")
      val profile = DbStorage.profile(dbConfig)
      val either: Either[DbMigrations.Error, Unit] = profile match {

        case Profile.Postgres(jdbc) =>
          val expectedPostgresVersions = Seq(10, 11)
          val expectedPostgresVersionsStr = Seq(10, 11).mkString(" or ")
          @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
          val maxPostgresVersion = expectedPostgresVersions.max

          // See https://www.postgresql.org/docs/9.1/sql-show.html
          val query = sql"show server_version".as[String]
          // Block on the query result, because `withDb` does not support running functions that return a
          // future (at the time of writing).
          val vector = timeouts.network.await(functionFullName)(db.run(query))
          val stringO = vector.headOption
          val either = for {
            versionString <- stringO.toRight(left = s"Could not read Postgres version")
            // An example `versionString` is 12.9 (Debian 12.9-1.pgdg110+1)
            majorVersion <- versionString
              .split('.')
              .headOption
              .toRight(left =
                s"Could not parse Postgres version string $versionString. Are you using the recommended Postgres version 11 ?"
              )
              .flatMap(str =>
                Try(str.toInt).toEither.leftMap(exn =>
                  s"Exception in parsing Postgres version string $versionString: $exn"
                )
              )
            _unit <- {
              if (expectedPostgresVersions.contains(majorVersion)) Right(())
              else if (majorVersion > maxPostgresVersion) {
                val level = if (standardConfig) Level.WARN else Level.INFO
                LoggerUtil.logAtLevel(
                  level,
                  s"Expected Postgres version $expectedPostgresVersionsStr but got higher version $versionString",
                )
                Right(())
              } else
                Left(
                  s"Expected Postgres version $expectedPostgresVersionsStr but got lower version $versionString"
                )
            }
          } yield ()
          either.leftMap(DbMigrations.DatabaseVersionError)

        case Profile.Oracle(jdbc) =>
          def checkOracleVersion(): Either[String, Unit] = {

            val expectedOracleVersion = 19
            val expectedOracleVersionPrefix =
              " 19." // Leading whitespace is intentional, see the example bannerString

            // See https://docs.oracle.com/en/database/oracle/oracle-database/18/refrn/V-VERSION.html
            val oracleVersionQuery = sql"select banner from v$$version".as[String].headOption
            val stringO = timeouts.network.await(functionFullName)(db.run(oracleVersionQuery))
            stringO match {
              case Some(bannerString) =>
                // An example `bannerString` is "Oracle Database 18c Express Edition Release 18.0.0.0.0 - Production"
                if (bannerString.contains(expectedOracleVersionPrefix)) {
                  logger.debug(
                    s"Check for oracle version $expectedOracleVersion passed: using $bannerString"
                  )
                  Right(())
                } else {
                  Left(s"Expected Oracle version $expectedOracleVersion but got $bannerString")
                }
              case None =>
                Left(s"Database version check failed: could not read Oracle version")
            }
          }

          // Checks that the NLS parameter `param` is set to one of the `expected` strings
          // - The DB setting must be set
          // - The session setting may be empty
          def checkNlsParameter(
              param: String,
              expected: Seq[String],
          ): Either[String, Unit] = {
            def prettyExpected: String =
              if (expected.size == 1) expected(0)
              else s"one of ${expected.mkString(", ")}"

            logger.debug(s"Checking NLS parameter $param")

            @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
            val queryDbSetting =
              sql"SELECT value from nls_database_parameters where parameter=$param"
                .as[String]
                .headOption
            val dbSettingO =
              timeouts.network.await(functionFullName + s"-database-$param")(db.run(queryDbSetting))

            @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
            val querySessionSetting =
              sql"SELECT value from nls_session_parameters where parameter=$param"
                .as[String]
                .headOption
            val sessionSettingO = timeouts.network.await(functionFullName + s"-session-$param")(
              db.run(querySessionSetting)
            )

            for {
              // Require to find the setting for the database, but leave it optional for the session
              dbSetting <- dbSettingO.toRight(
                s"Oracle NLS database parameter $param is not set, but should be $prettyExpected"
              )
              _ <- Either.cond(
                expected.contains(dbSetting.toUpperCase),
                logger.debug(s"NLS database parameter $param is set to $dbSetting"),
                s"Oracle NLS database parameter $param is $dbSetting, but should be $prettyExpected",
              )

              _ <- sessionSettingO.fold(
                Either.right[String, Unit](logger.debug(s"NLS session parameter $param is unset"))
              ) { sessionSetting =>
                Either.cond(
                  expected.contains(sessionSetting.toUpperCase),
                  logger.debug(s"NLS session parameter $param is set to $sessionSetting"),
                  s"Oracle NLS session parameter $param is $sessionSetting, but should be $prettyExpected",
                )
              }
            } yield ()
          }

          // Check the NLS settings of the database so that Oracle uses the expected encodings and collations for
          // string fields in tables.
          def checkOracleNlsSetting(): Either[String, Unit] =
            for {
              _ <- checkNlsParameter("NLS_CHARACTERSET", Seq("AL32UTF8"))
              _ <- checkNlsParameter("NLS_NCHAR_CHARACTERSET", Seq("AL32UTF8", "AL16UTF16"))
              _ <- checkNlsParameter("NLS_SORT", Seq("BINARY"))
              _ <- checkNlsParameter("NLS_COMP", Seq("BINARY"))
            } yield ()

          for {
            _ <- checkOracleVersion().leftMap(DbMigrations.DatabaseVersionError)
            _ <- checkOracleNlsSetting().leftMap(DbMigrations.DatabaseConfigError)
          } yield ()
        case Profile.H2(_) =>
          // We don't perform version checks for H2
          Right(())
      }
      if (standardConfig) either
      else
        either.leftFlatMap { error =>
          logger.info(error.toString)
          Right(())
        }
    }
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
