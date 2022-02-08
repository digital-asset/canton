// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.jdk.CollectionConverters._

trait StorageConfig {

  /** Database specific configuration parameters used by Slick.
    * Also available for in-memory storage to support easy switching between in-memory and database storage.
    */
  def config: Config

  /** Allows for setting the maximum number of db connections used by Canton and the ledger API server.
    * If None or non-positive, the value will be auto-detected from the number of processors.
    * Has no effect, if the number of connections is already set via slick options
    * (i.e., `config.numThreads`).
    */
  def maxConnections: Option[Int]

  private def maxConnectionsOrDefault(
      logger: TracedLogger
  )(implicit traceContext: TraceContext): Int = {
    // The following is an educated guess of a sane default for the number of DB connections.
    // https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
    maxConnections match {
      case Some(value) if value > 0 => value
      case _ => Threading.detectNumberOfThreads(logger)
    }
  }

  /** Returns the size of the Canton connection pool for the given usage.
    *
    * @param forParticipant True if the connection pool is used by a participant, then we reserve connections for the ledger API server.
    * @param withWriteConnectionPool True for a replicated node's write connection pool, then we split the available connections between the read and write pools.
    * @param withMainConnection True for accounting an additional connection (write connection, or main connection with lock)
    */
  def maxConnectionsCanton(
      forParticipant: Boolean,
      withWriteConnectionPool: Boolean,
      withMainConnection: Boolean,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext
  ): PositiveInt = {
    val c = maxConnectionsOrDefault(logger)

    // A participant evenly shares the max connections between the ledger API server (not indexer) and canton
    val totalConnectionPoolSize = if (forParticipant) c / 2 else c

    // For replicated nodes we have an additional connection pool for writes. Split evenly between reads and writes.
    val replicatedConnectionPoolSize =
      if (withWriteConnectionPool) totalConnectionPoolSize / 2 else totalConnectionPoolSize

    val resultMaxConnections = if (withMainConnection) {
      // The write connection pool for replicated nodes require an additional connection outside of the pool
      (replicatedConnectionPoolSize - 1)
    } else
      replicatedConnectionPoolSize

    // Return at least one connection
    PositiveInt.tryCreate(resultMaxConnections max 1)
  }

  /** Max connections for the Ledger API server. The Ledger API indexer's max connections are configured separately. */
  def maxConnectionsLedgerApiServer(logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Int = {
    // The Ledger Api Server always gets half of the max connections allocated to canton
    maxConnectionsOrDefault(logger) / 2 max 1
  }
}

/** Determines how a node stores persistent data.
  */
sealed trait CommunityStorageConfig extends StorageConfig

trait MemoryStorageConfig extends StorageConfig {
  override val maxConnections: Option[Int] = None
}

object CommunityStorageConfig {

  /** Dictates that persistent data is stored in memory.
    * So in fact, the data is not persistent. It is deleted whenever the node is stopped.
    *
    * @param config IGNORED configuration option, used to allow users to use configuration mixins with postgres and h2
    */
  case class Memory(override val config: Config = ConfigFactory.empty())
      extends CommunityStorageConfig
      with MemoryStorageConfig
}

/** Dictates that persistent data is stored in a database.
  */
trait DbConfig extends StorageConfig with PrettyPrinting {

  /** Where should database migrations be read from.
    * Enables specialized DDL for different database servers (e.g. Postgres, Oracle).
    */
  val migrationsPaths: Seq[String]

  /** Canton attempts to generate appropriate configuration for the daml ledger-api to persist the data it requires.
    * In most circumstances this should be sufficient and there is no need to override this.
    * However if this generation fails or an advanced configuration is required, the ledger-api jdbc url can be
    * explicitly configured using this property.
    * The jdbc url **must** specify the schema of `ledger_api` (using h2 parameter `schema` or postgres parameter `currentSchema`).
    * This property is not used by a domain node as it does not run a ledger-api instance,
    * and will be ignored if the node is configured with in-memory persistence.
    */
  val ledgerApiJdbcUrl: Option[String]

  /** How long to wait for acquiring a database connection */
  val connectionTimeout: NonNegativeFiniteDuration

  /** TO BE USED ONLY FOR TESTING!
    * Clean the database if validation during DB migration fails.
    */
  def cleanOnValidationError: Boolean = false

  /** TO BE USED ONLY FOR TESTING!
    * <p>
    * Whether to automatically call baseline when migrate is executed against a non-empty schema with no schema history table.
    * This schema will then be baselined with the {@code baselineVersion} before executing the migrations.
    * Only migrations above {@code baselineVersion} will then be applied.
    * </p>
    * <p>
    * This is useful for databases projects where the initial vendor schema is not empty
    * </p>
    *
    * if baseline should be called on migrate for non-empty schemas, { @code false} if not. (default: { @code false})
    */
  def baselineOnMigrate: Boolean = false

  override def pretty: Pretty[DbConfig] =
    prettyOfClass(
      param(
        "config",
        _.config.toString.replaceAll("\"password\":\".*?\"", "\"password\":\"???\"").unquoted,
      ),
      param("migrationsPaths", _.migrationsPaths.map(_.doubleQuoted)),
      paramIfDefined("ledgerApiJdbcUrl", _.ledgerApiJdbcUrl.map(_.doubleQuoted)),
    )
}

trait H2DbConfig extends DbConfig {
  def databaseName: Option[String] = {
    if (config.hasPath("url")) {
      val url = config.getString("url")
      "(:mem:|:file:)([^:;]+)([:;])".r.findFirstMatchIn(url).map(_.group(2))
    } else None
  }
}

trait PostgresDbConfig extends DbConfig

sealed trait CommunityDbConfig extends CommunityStorageConfig with DbConfig

object CommunityDbConfig {
  case class H2(
      override val config: Config,
      override val maxConnections: Option[Int] = None,
      override val migrationsPaths: Seq[String] =
        Seq(DbConfig.postgresH2SharedMigrationsPath, DbConfig.h2MigrationsPath),
      override val ledgerApiJdbcUrl: Option[String] = None,
      override val connectionTimeout: NonNegativeFiniteDuration = DbConfig.defaultConnectionTimeout,
  ) extends CommunityDbConfig
      with H2DbConfig

  case class Postgres(
      override val config: Config,
      override val maxConnections: Option[Int] = None,
      override val migrationsPaths: Seq[String] =
        Seq(DbConfig.postgresH2SharedMigrationsPath, DbConfig.postgresMigrationsPath),
      override val ledgerApiJdbcUrl: Option[String] = None,
      override val connectionTimeout: NonNegativeFiniteDuration = DbConfig.defaultConnectionTimeout,
      override val cleanOnValidationError: Boolean = false,
  ) extends CommunityDbConfig
      with PostgresDbConfig
}

object DbConfig extends NoTracing {

  val defaultConnectionTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)

  val postgresH2SharedMigrationsPath: String = "classpath:db/migration/canton/postgres_h2_shared"
  val postgresMigrationsPath: String = "classpath:db/migration/canton/postgres"
  val h2MigrationsPath: String = "classpath:db/migration/canton/h2"
  val oracleMigrationPath: String = "classpath:db/migration/canton/oracle"

  def postgresUrl(host: String, port: Int, name: String): String =
    s"jdbc:postgresql://$host:$port/$name"

  def h2Url(dbName: String): String =
    s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;LOCK_TIMEOUT=10000;DB_CLOSE_DELAY=-1"

  def oracleUrl(host: String, port: Int, name: String): String =
    s"jdbc:oracle:thin:@$host:$port/$name"

  /** Apply default values to the given db config
    */
  def configWithFallback(
      dbConfig: DbConfig
  )(
      forParticipant: Boolean,
      withWriteConnectionPool: Boolean,
      withMainConnection: Boolean,
      poolName: String,
      logger: TracedLogger,
  ): Config = {
    def toConfig(map: Map[String, Any]): Config = ConfigFactory.parseMap(map.asJava)

    val commonDefaults = toConfig(
      Map(
        "poolName" -> poolName,
        "numThreads" -> dbConfig
          .maxConnectionsCanton(
            forParticipant,
            withWriteConnectionPool,
            withMainConnection,
            logger,
          )
          .unwrap,
        "connectionTimeout" -> dbConfig.connectionTimeout.unwrap.toMillis,
        "initializationFailTimeout" -> 1, // Must be greater than 0 to force a connection validation on startup
      )
    )
    val h2Defaults = toConfig(Map("driver" -> "org.h2.Driver"))
    (dbConfig match {
      case h2: H2DbConfig =>
        def containsOption(c: Config, optionName: String, optionValue: String) = {
          val propertiesPath = s"properties.$optionName"
          val valueIsInProperties =
            c.hasPath(propertiesPath) && c.getString(propertiesPath).contains(optionValue)
          val valueIsInUrl = Seq("url", "jdbcUrl")
            .map(path => c.hasPath(path) && c.getString(path).contains(s"$optionName=$optionValue"))
            .exists(identity)
          valueIsInProperties || valueIsInUrl
        }
        def enforcePgMode(c: Config): Config =
          if (!containsOption(c, "MODE", "PostgreSQL")) {
            logger.warn(
              "Given H2 config did not contain PostgreSQL compatibility mode. Automatically added it."
            )
            c.withValue("properties.MODE", ConfigValueFactory.fromAnyRef("PostgreSQL"))
          } else c
        def enforceDelayClose(c: Config): Config = {
          val isInMemory =
            Seq("url", "jdbcUrl")
              .map(path => c.hasPath(path) && c.getString(path).contains(":mem:"))
              .exists(identity)
          if (isInMemory && !containsOption(c, "DB_CLOSE_DELAY", "-1")) {
            logger.warn(
              s"Given H2 config is in-memory and does not contain DB_CLOSE_DELAY=-1. Automatically added this to avoid accidentally losing all data. $c"
            )
            c.withValue("properties.DB_CLOSE_DELAY", ConfigValueFactory.fromAnyRef("-1"))
          } else c
        }
        def enforceSingleConnection(c: Config): Config = {
          if (!c.hasPath("numThreads") || c.getInt("numThreads") != 1) {
            logger.info("Overriding numThreads to 1 to avoid concurrency issues.")
          }
          c.withValue("numThreads", ConfigValueFactory.fromAnyRef(1))
        }
        enforceDelayClose(enforcePgMode(enforceSingleConnection(h2.config)))
          .withFallback(h2Defaults)
      case postgres: PostgresDbConfig => postgres.config
      // TODO(soren): this other is a workaround for supporting oracle without referencing the oracle config
      case other => other.config
    }).withFallback(commonDefaults)
  }

  /** strip the password and the url out of the config object */
  def hideConfidential(config: Config): Config = {
    val hidden = ConfigValueFactory.fromAnyRef("****")
    val replace = Seq("password", "properties.password", "url", "properties.url")
    replace.foldLeft(config) { case (acc, path) =>
      if (acc.hasPath(path))
        acc.withValue(path, hidden)
      else acc
    }
  }

}
