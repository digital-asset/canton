// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.syntax.parallel.*
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.parallelInstanceFutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{
  DbStorageSetup,
  PostgresDbStorageSetup,
  PostgresTestDatabase,
}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Pause, Success as RetrySuccess}
import com.typesafe.config.{Config, ConfigFactory}
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random
import scala.util.control.NonFatal

/** Plugin to provide a postgres backend to a
  * [[com.digitalasset.canton.integration.BaseIntegrationTest]] instance.
  *
  * By default, tests will run against a dockerized db. So you need to do nothing except for
  * installing docker.
  *
  * To get higher performance (in particular on Mac OSX), you may want to run tests against a
  * non-containerized postgres db. To do so, export the following environment variables:
  * {{{
  * CI=1
  * POSTGRES_USER=test # changing the username is not recommended, see data continuity tests documentation in contributing.md
  * POSTGRES_PASSWORD=supersafe
  * POSTGRES_DB=postgres
  * }}}
  *
  * Please note that you need to create in this case two databases: $$POSTGRES_DB AND
  * "$${POSTGRES_DB}_dev"
  *
  * On the database, you need to create the user as follows:
  *
  * {{{
  * $ psql postgres
  * postgres=# create user test with password 'supersafe';
  * CREATE ROLE
  * postgres=# ALTER USER test CREATEDB CREATEROLE;
  * ALTER ROLE
  * postgres=# ALTER USER test CREATEROLE;
  * ALTER ROLE
  * postgres=# exit
  * }}}
  *
  * @param customDbNames
  *   optionally defines a mapping from identifier to db name (String => String) and a suffix to add
  *   to the db name
  * @param dropDatabaseAfterTest
  *   default true, otherwise preserve the database after the test
  * @param nodeDbMapping
  *   Allow nodes to share the DB: if `node1` maps to `node2`, then `node1` will use `node2` DB.
  */
class UsePostgres(
    protected[plugins] val loggerFactory: NamedLoggerFactory,
    customDbNames: Option[(String => String, String)] = None,
    nodeDbMapping: String => String = identity,
    customMaxConnectionsByNode: Option[String => Option[PositiveInt]] = None,
    forceTestContainer: Boolean = false,
    dropDatabaseAfterTest: Boolean = true,
) extends EnvironmentSetupPlugin
    with FlagCloseable
    with HasCloseContext
    with NoTracing {

  private lazy val dbSetupExecutorService: ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(
      loggerFactory.threadName + "-db-execution-context",
      noTracingLogger,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
    )

  private[plugins] def dbSetupExecutionContext: ExecutionContext = dbSetupExecutorService

  // Make sure that each environment and its database names are unique by generating a random prefix
  // Short prefix due to postgres database name limit of 63 bytes
  private lazy val dbPrefix = "d" + Random.alphanumeric.map(_.toLower).take(7).mkString

  private lazy val applicationName: String = loggerFactory.name

  def generateDbName(nodeName: String): String =
    customDbNames match {
      case Some((dbNames, suffix)) => dbNames(nodeName) + suffix
      case None => s"${dbPrefix}_${nodeDbMapping(nodeName)}"
    }

  // we have to keep the storage instance alive for the duration of the test as this could be
  // managing an external docker container and other resources
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private[integration] var dbSetup: PostgresDbStorageSetup = _

  override def beforeTests(): Unit =
    dbSetup = DbStorageSetup.postgres(loggerFactory, forceTestContainer = forceTestContainer)(
      dbSetupExecutionContext
    )

  // can throw NullPointerException if called before `beforeTests`
  def getDbUsernameOrThrow: String = dbSetup.basicConfig.username

  // Comment out to keep the container running after tests
  override def onClosed(): Unit =
    LifeCycle.close(
      dbSetup,
      ExecutorServiceExtensions(dbSetupExecutorService)(logger, DefaultProcessingTimeouts.testing),
    )(logger)

  override def afterTests(): Unit = {
    dbSetup.analyzeDatabaseStatistics()
    close()
  }

  def generateDbConfig(
      name: String,
      baseParameters: DbParametersConfig,
      baseDbConfig: Config,
  ): DbConfig = {

    val dbName = generateDbName(name)

    val basicConfigWithDbName =
      dbSetup.basicConfig.copy(
        username = PostgresTestDatabase.userName(dbName),
        password = PostgresTestDatabase.password,
        dbName = dbName,
        connectionPoolEnabled = true,
      )

    val configWithApplicationName =
      ConfigFactory
        .parseString(s"properties.applicationName = $applicationName")
        .withFallback(basicConfigWithDbName.toPostgresConfig)
        .withFallback(baseDbConfig)

    val dbConfigCanton = DbConfig.Postgres(
      configWithApplicationName,
      baseParameters,
    )

    customMaxConnectionsByNode match {
      case Some(maxConnectionsByNode) =>
        dbConfigCanton.focus(_.parameters.maxConnections).replace(maxConnectionsByNode(name))
      case None => dbConfigCanton
    }
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    val transformedConfig = ConfigTransforms.modifyAllStorageConfigs((_, name, storage) =>
      generateDbConfig(name, storage.parameters, storage.config)
    )(config)

    Await.result(
      recreateDatabases(transformedConfig),
      config.parameters.timeouts.processing.io.duration,
    )
    transformedConfig
  }

  // Comment out to keep the databases after tests
  override def afterEnvironmentDestroyed(config: CantonConfig): Unit = {
    val nodes = nodeNamesOfConfig(config)
    if (dropDatabaseAfterTest) {
      val drops = dropDatabases(nodes)
      Await.result(drops, config.parameters.timeouts.processing.io.duration)
    } else {
      val dbNames = nodes.map(generateDbName).distinct
      logger.info(s"Preserving the following database names:\n  ${dbNames.mkString("\n  ")}")
    }
  }

  private def logOpenQueries(storage: DbStorage): FutureUnlessShutdown[Unit] = {
    implicit val ec: ExecutionContext = dbSetupExecutionContext
    import storage.api.*

    storage
      .query(
        sql"select datname, query, state from pg_stat_activity".as[(String, String, String)],
        functionFullName,
      )
      .map { queries =>
        logger.info(s"Running queries: ${queries.mkString("\n")}")
      }
  }

  def dropDatabases(config: CantonConfig): FutureUnlessShutdown[Unit] =
    dropDatabases(nodeNamesOfConfig(config))

  def dropDatabases(nodes: Seq[String]): FutureUnlessShutdown[Unit] = {
    implicit val ec: ExecutionContext = dbSetupExecutionContext
    val dbNames = nodes.map(generateDbName).distinct
    val storage = dbSetup.storage
    import storage.api.*

    implicit val success: RetrySuccess[Unit] = RetrySuccess.always

    val retryPolicy = Pause(
      logger = logger,
      hasSynchronizeWithClosing = this,
      maxRetries = 5,
      delay = 1.second,
      operationName = "drop-postgres-databases",
    )

    def startDropDbFUS(): FutureUnlessShutdown[Unit] = storage.update_(
      DBIO.seq(dbNames.flatMap(PostgresTestDatabase.dropStatements)*),
      functionFullName,
    )

    retryPolicy
      .unlessShutdown(startDropDbFUS(), AllExceptionRetryPolicy)
      .recover { case NonFatal(e) =>
        logger.warn(s"Dropping database failed after all retries", e)
        logOpenQueries(storage).discard
        UnlessShutdown.Outcome(())
      }
  }

  def recreateDatabases(config: CantonConfig): FutureUnlessShutdown[Unit] = {
    implicit val ec: ExecutionContext = dbSetupExecutionContext
    nodeNamesOfConfig(config)
      .map(generateDbName)
      .distinct
      .parTraverse_(dbName => recreateDatabase(dbName))
  }

  def recreateDatabase(node: InstanceReference): FutureUnlessShutdown[Unit] =
    recreateDatabaseForNodeWithName(node.name)

  def recreateDatabaseForNodeWithName(node: String): FutureUnlessShutdown[Unit] =
    recreateDatabase(generateDbName(node))

  def recreateDatabase(dbName: String): FutureUnlessShutdown[Unit] = {
    val storage = dbSetup.storage
    import storage.api.*

    storage.update_(
      DBIO.seq(
        (PostgresTestDatabase.dropStatements(dbName) ++
          PostgresTestDatabase.createStatements(dbName))*
      ),
      operationName = "Recreate databases",
    )
  }

  private def nodeNamesOfConfig(transformedConfig: CantonConfig): List[String] =
    (transformedConfig.participants.keys ++ transformedConfig.mediators.keys ++ transformedConfig.sequencers.keys).toList
      .map(_.unwrap)

  override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
}
