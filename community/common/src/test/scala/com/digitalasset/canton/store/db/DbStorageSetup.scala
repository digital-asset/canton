// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{CommunityDbMigrationsFactory, DbStorage, DbStorageSingle}
import com.digitalasset.canton.store.db.DbStorageSetup.Config.{DbBasicConfig, PostgresBasicConfig}
import com.digitalasset.canton.util.ShowUtil._
import com.typesafe.config.{Config, ConfigFactory}
import org.testcontainers.containers.PostgreSQLContainer

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

/** Provide a storage backend for tests.
  * Description is used by DbStorageTest to name tests.
  */
trait DbStorageSetup[C <: DbConfig] extends AutoCloseable {
  val storage: DbStorage

  def config: C

  override def close(): Unit = storage.close()
}

trait DbStorageBasicConfig[BC <: DbBasicConfig[BC]] {
  def basicConfig: BC
}

abstract class PostgresDbStorageSetup(
    timeout: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    skipDbMigration: Boolean,
)(implicit ec: ExecutionContext)
    extends DbStorageSetup[PostgresDbConfig]
    with DbStorageBasicConfig[PostgresBasicConfig] {

  override lazy val storage: DbStorage = {
    val metrics =
      CommonMockMetrics.dbStorage // This storage will only be used to setup the DB. Therefore, it is ok to use mock metrics, even in performance tests.
    val s = DbStorageSingle.tryCreate(
      config,
      // no need to adjust the connection pool for participants, as we are not yet running the ledger api server
      connectionPoolForParticipant = false,
      None,
      metrics,
      timeout,
      loggerFactory,
    )
    if (!skipDbMigration) {
      val migrationResult =
        new CommunityDbMigrationsFactory(loggerFactory).create(config).migrateDatabase()
      // throw so the first part of the test that attempts to use storage will fail with an exception
      migrationResult.left.foreach(err =>
        throw new RuntimeException(show"Failed to migrate database: $err")
      )
    }
    s
  }
}

abstract class PostgresDbStorageFunctionalTestSetup(
    loggerFactory: NamedLoggerFactory,
    skipDbMigration: Boolean,
)(implicit ec: ExecutionContext)
    extends PostgresDbStorageSetup(
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      skipDbMigration,
    ) {

  override def config: PostgresDbConfig = {
    val defaultConfig = DbStorageSetup.Config.pgConfig(basicConfig)
    defaultConfig.copy(
      config = ConfigFactory
        .parseMap(Map("connectionPool" -> "disabled").asJava)
        .withFallback(defaultConfig.config),
      cleanOnValidationError = true,
    )
  }
}

/** Assumes Postgres is available on a already running and that connections details are
  * provided through environment variables.
  * In CI this is done by running a Postgres docker container alongside the build.
  */
class PostgresCISetup(loggerFactory: NamedLoggerFactory, skipDbMigration: Boolean)(implicit
    ec: ExecutionContext
) extends PostgresDbStorageFunctionalTestSetup(loggerFactory, skipDbMigration) {
  override val basicConfig: PostgresBasicConfig = PostgresBasicConfig(
    env("POSTGRES_USER"),
    env("POSTGRES_PASSWORD"),
    env("POSTGRES_DB"),
  )

  /** Lookup environment variable and return. Throw [[java.lang.RuntimeException]] if missing. */
  private def env(name: String): String =
    sys.env.getOrElse(name, sys.error(s"Environment variable not set [$name]"))
}

/** Use [TestContainers]() to create a Postgres docker container instance to run against.
  * Used for running tests locally.
  */
class PostgresTestContainerSetup(val loggerFactory: NamedLoggerFactory, skipDbMigration: Boolean)(
    implicit ec: ExecutionContext
) extends PostgresDbStorageFunctionalTestSetup(loggerFactory, skipDbMigration)
    with NamedLogging {
  private val postgresContainer = new PostgreSQLContainer(s"${PostgreSQLContainer.IMAGE}:11")
  // up the connection limit to deal with everyone using connection pools in tests that can run concurrently.
  // we also have a matching max connections limit set in the CircleCI postgres executor (`.circle/config.yml`)
  private val command = postgresContainer.getCommandParts.toSeq :+ "-c" :+ "max_connections=500"
  postgresContainer.setCommandParts(command.toArray)
  noTracingLogger.debug(s"Starting postgres container with $command")
  postgresContainer.start()

  override val basicConfig: PostgresBasicConfig = PostgresBasicConfig(
    postgresContainer.getUsername,
    postgresContainer.getPassword,
    postgresContainer.getDatabaseName,
    postgresContainer.getContainerIpAddress,
    postgresContainer.getFirstMappedPort,
  )

  def getContainerID: String = postgresContainer.getContainerId

  override def close(): Unit = {
    try super.close()
    finally postgresContainer.close()
  }
}

class PostgresPerformanceTestingSetup(
    loggerFactory: NamedLoggerFactory,
    createDatabaseMode: Boolean,
    override val basicConfig: PostgresBasicConfig,
)(implicit executionContext: ExecutionContext)
    extends PostgresDbStorageSetup(
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      createDatabaseMode,
    ) {

  override def config: PostgresDbConfig = {
    val defaultConfig = DbStorageSetup.Config.pgConfig(basicConfig)
    val pgConfigChanges = Map[String, Any]("connectionPool" -> "HikariCP", "registerMbeans" -> true)

    defaultConfig.copy(
      config = ConfigFactory.parseMap(pgConfigChanges.asJava).withFallback(defaultConfig.config),
      cleanOnValidationError = true,
    )
  }
}

class H2DbStorageSetup(timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext
) extends DbStorageSetup[H2DbConfig] {
  val config: H2DbConfig = DbStorageSetup.Config.h2Config(
    "",
    "",
    loggerFactory.name, // this will typically be the test name so we end up with isolated databases between tests
  )

  val storage: DbStorage = DbStorageSingle.tryCreate(
    config,
    // no need to adjust the connection pool for participants, as we are not yet running the ledger api server
    connectionPoolForParticipant = false,
    None,
    CommonMockMetrics.dbStorage,
    timeouts,
    loggerFactory,
  )

  private val migrations = new CommunityDbMigrationsFactory(loggerFactory).create(config)

  migrations
    .migrateIfFresh()
    .left
    .foreach(err => throw new RuntimeException(show"Failed to migrate database: $err"))
}

object DbStorageSetup {

  /** If we are running in CI on a docker executor opt to use a separate docker setup for testing postgres
    * If we are running in CI on a `machine` use a local [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer]
    * If we are running locally use a local [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer]
    *
    * The returned setup is not suitable for performance testing. Use [[PostgresPerformanceTestingSetup]] for that.
    */
  def postgresFunctionalTestSetup(
      loggerFactory: NamedLoggerFactory,
      skipDbMigration: Boolean = false,
  )(implicit ec: ExecutionContext): PostgresDbStorageFunctionalTestSetup =
    if (sys.env.contains("CI") && !sys.env.contains("MACHINE"))
      new PostgresCISetup(loggerFactory, skipDbMigration)
    else new PostgresTestContainerSetup(loggerFactory, skipDbMigration)

  def h2(loggerFactory: NamedLoggerFactory)(implicit ec: ExecutionContext): H2DbStorageSetup =
    new H2DbStorageSetup(DefaultProcessingTimeouts.testing, loggerFactory)

  object Config {
    trait DbBasicConfig[A <: DbBasicConfig[A]] {
      this: {
        def copy(
            username: String,
            password: String,
            dbName: String,
            host: String,
            port: Int,
            options: String,
        ): A
      } =>

      val username: String
      val password: String
      val dbName: String
      val host: String
      val port: Int

      /** Comma separated list of driver specific options. E.g. ApplicationName=myApplication for Postgres. */
      val options: String

      def toConfig: Config

      def modify(
          username: String = this.username,
          password: String = this.password,
          dbName: String = this.dbName,
          host: String = this.host,
          port: Int = this.port,
          options: String = this.options,
      ): A = copy(username, password, dbName, host, port, options)

    }

    case class PostgresBasicConfig(
        override val username: String,
        override val password: String,
        override val dbName: String,
        override val host: String = "localhost",
        override val port: Int = 5432,
        override val options: String = "",
    ) extends DbBasicConfig[PostgresBasicConfig] {
      override def toConfig: Config = {
        val optionsSuffix = if (options.isEmpty) "" else "?" + options
        ConfigFactory.parseMap(
          Map(
            "url" -> s"${DbConfig.postgresUrl(host, port, dbName)}$optionsSuffix",
            "user" -> username,
            "password" -> password,
            "driver" -> "org.postgresql.Driver",
          ).asJava
        )
      }
    }

    def h2Config[H2C <: H2DbConfig](
        username: String,
        password: String,
        dbName: String,
        mkH2Config: Config => H2C,
    ): H2C =
      mkH2Config(
        ConfigFactory.parseMap(
          Map(
            "url" -> DbConfig.h2Url(dbName),
            "user" -> username,
            "password" -> password,
            "driver" -> "org.h2.Driver",
          ).asJava
        )
      )

    def h2Config(username: String, password: String, dbName: String): CommunityDbConfig.H2 =
      h2Config(username, password, dbName, CommunityDbConfig.H2(_))

    def pgConfig[PC <: PostgresDbConfig](c: PostgresBasicConfig, mkPostgres: Config => PC): PC =
      mkPostgres(c.toConfig)

    def pgConfig(c: PostgresBasicConfig): CommunityDbConfig.Postgres =
      pgConfig(c, CommunityDbConfig.Postgres(_))
  }

}
