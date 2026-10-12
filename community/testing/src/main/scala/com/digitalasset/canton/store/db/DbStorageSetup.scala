// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.LogReporter
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.DbConfig.{H2, Postgres}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.resource.{DbMigrations, DbStorage, DbStorageSingle}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.github.dockerjava.api.model.Bind
import com.typesafe.config.{Config, ConfigFactory}
import org.postgresql.util.PSQLException
import org.scalatest.Args
import org.scalatest.Assertions.fail
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import slick.sql.SqlAction

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Provide a storage backend for tests.
  */
trait DbStorageSetup extends FlagCloseable with HasCloseContext with NamedLogging {
  type C <: DbConfig

  protected implicit def executionContext: ExecutionContext
  private implicit val traceContext: TraceContext = TraceContext.empty

  def basicConfig: DbBasicConfig

  def mkDbConfig: DbBasicConfig => C

  /** Used when creating the storage */
  def retryConfig: DbStorage.RetryConfig

  // Generous timeout to give the db some time to startup.
  protected lazy val dbStartupTimeout: NonNegativeDuration = NonNegativeDuration.ofMinutes(10)

  protected def prepareDatabase(): Unit

  def migrationMode: MigrationMode

  protected def destroyDatabase(): Unit

  def analyzeDatabaseStatistics(): Unit = ()

  override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

  // Needs to run first, because test containers need a running db to determine basicConfig.
  prepareDatabase()

  final lazy val config: C = mkDbConfig(basicConfig)

  final lazy val storage: DbStorage = mkStorage(config)

  /** Will run all setup code of this instance and throw the setup code fails. If this method is not
    * called up front, the setup code will run (and possibly fail) when a field is accessed.
    */
  // We could make `storage` a `val` so that no separate method is needed.
  // In that case, the initialization code of this trait would invoke code from subtypes through abstract methods.
  // As a result, it could read uninitialized fields from subtypes, which can lead to annoying bugs.
  def initialized(): this.type = {
    storage.discard[DbStorage]
    this
  }

  protected final def mkStorage(cfg: DbConfig): DbStorage =
    DbStorageSingle.tryCreate(
      cfg,
      new SimClock(CantonTimestamp.Epoch, loggerFactory),
      None,
      connectionPoolForParticipant =
        false, // can always be false, because this storage is only used for initialization and unit tests
      None,
      CommonMockMetrics.dbStorage,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      retryConfig,
    )

  final def migrateDb(): Unit = {
    val migrationResult =
      DbMigrations
        .create(config, migrationMode == MigrationMode.DevVersion, timeouts, loggerFactory)
        .migrateDatabase()
    // throw so the first part of the test that attempts to use storage will fail with an exception
    migrationResult
      .valueOr(err => fail(s"Failed to migrate database: $err"))
      .onShutdown(fail("DB migration interrupted due to shutdown"))
  }

  override final def onClosed(): Unit = {
    storage.close()
    destroyDatabase()
  }
}

sealed trait MigrationMode extends Product with Serializable
object MigrationMode {
  case object Standard extends MigrationMode
  case object DevVersion extends MigrationMode
}

/** Postgres database storage setup
  */
abstract class PostgresDbStorageSetup(
    override protected val loggerFactory: NamedLoggerFactory
)(implicit override val executionContext: ExecutionContext)
    extends DbStorageSetup
    with NamedLogging
    with NoTracing {

  override type C = Postgres

  override lazy val retryConfig: RetryConfig = RetryConfig.failFast

}

/** Assumes Postgres is available on a already running and that connections details are provided
  * through environment variables. In CI this is done by running a Postgres docker container
  * alongside the build.
  * @param migrationMode
  *   migration mode to use
  * @param mkDbConfig
  *   function to create a db config from a basic config
  * @param useDbNameO
  *   optional name of the database to use for the tests (will be created if it does not exist)
  */
class PostgresCISetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => Postgres,
    useDbNameO: Option[String] = None,
    loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends PostgresDbStorageSetup(loggerFactory) {

  /** name of existing database we can use (either for testing or for setting up new databases) */
  private lazy val envDb = DbStorageSetup.envDbName

  /** name of db to use for the tests (avoiding flyway migration conflicts) */
  private lazy val useDb =
    useDbNameO.getOrElse(envDb) + (if (migrationMode == MigrationMode.DevVersion) "_dev" else "")

  override lazy val basicConfig: DbBasicConfig = DbStorageSetup.envBasicConfig(useDb)

  override protected def prepareDatabase(): Unit =
    if (useDb != envDb) { // for provided db name or dev migrations
      val envDbConfig =
        DbConfig.Postgres(basicConfig.copy(dbName = envDb).toPostgresConfig)
      val envDbStorage = mkStorage(envDbConfig)

      def transformQueryResult[T](v: T): PartialFunction[Throwable, UnlessShutdown[T]] = {
        // Due to a race condition, it may happen that between checking if the database exists and creating it,
        // it has already been created and a duplicate database error is thrown.
        // We can safely ignore this error.
        case ex: PSQLException
            // 23505 means "unique_violation"
            // 42P04 means "duplicate_database"
            // either of these 2 errors could happen when trying to create a database that already exists
            // (source: https://www.postgresql.org/docs/current/errcodes-appendix.html)
            if ex.getSQLState == "23505" || ex.getSQLState == "42P04" =>
          UnlessShutdown.Outcome(v)

        // Two concurrent calls try to do some operation
        case ex: PSQLException if ex.getMessage.contains("ERROR: tuple concurrently updated") =>
          UnlessShutdown.Outcome(v)
      }

      try {
        import envDbStorage.api.*
        val genF = envDbStorage
          .query(sql"SELECT 1 FROM pg_database WHERE datname = $useDb".as[Int], functionFullName)
          .recover(transformQueryResult(Vector(1)))
          .flatMap { res =>
            if (res.isEmpty) {
              logger.debug(s"Creating database $useDb using connection to $envDb")
              envDbStorage
                .update_(PostgresTestDatabase.createDatabase(useDb), functionFullName)
                .recover(transformQueryResult(()))
            } else FutureUnlessShutdown.unit
          }
        DefaultProcessingTimeouts.default.awaitUS_(s"creating database $useDb")(genF)
      } finally envDbStorage.close()
    }

  override protected def destroyDatabase(): Unit = ()
}

/** Use [TestContainers]() to create a Postgres docker container instance to run against. Used for
  * running tests locally.
  */
class PostgresTestContainerSetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => Postgres,
    loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends PostgresDbStorageSetup(loggerFactory)
    with NamedLogging {

  private lazy val postgresContainer = DbStorageSetup.newPostgresContainer()

  override protected def prepareDatabase(): Unit = {
    noTracingLogger.debug(
      s"Starting postgres container with ${postgresContainer.getCommandParts.toSeq}"
    )

    val startF = Future(postgresContainer.start())
    dbStartupTimeout.await_("startup of postgres container")(startF)
  }

  override lazy val basicConfig: DbBasicConfig =
    DbStorageSetup.containerBasicConfig(postgresContainer)

  def getContainerID: String = postgresContainer.getContainerId

  override protected def destroyDatabase(): Unit = postgresContainer.close()

  override def analyzeDatabaseStatistics(): Unit =
    if (DbStorageSetup.collectQueryStats) {
      logger.info("Analyzing postgres stats")
      if (storage.isClosing) {
        logger.warn("Storage is already closed, skipping analysis")
      } else {
        val pgAnalysis = new PostgresStatsAnalysis(storage, timeouts)
        // Note: this prints directly to the stdout, and this is exactly what we want,
        // since testcontainers are used for local test runs
        pgAnalysis.run(
          testName = None,
          args = Args(reporter = new LogReporter),
        )
      }
    } else {
      logger.info(
        "Skipping Postgres stats analysis, to enable it set CANTON_COLLECT_QUERY_STATS env var " +
          "and have locally built `digitalasset/query-stats-postgres:local` container image available, " +
          "by running locally: 'docker build -f .ci/Dockerfile.query-stats-postgres .ci " +
          "-t digitalasset/query-stats-postgres:local' from the canton repository root"
      )
    }
}

class H2DbStorageSetup(
    override val migrationMode: MigrationMode,
    override val mkDbConfig: DbBasicConfig => H2,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    override val executionContext: ExecutionContext
) extends DbStorageSetup
    with NamedLogging {

  override type C = H2

  override lazy val basicConfig: DbBasicConfig = DbBasicConfig("", "", loggerFactory.name, "", 0)

  override lazy val retryConfig: RetryConfig = RetryConfig.failFast

  override protected def prepareDatabase(): Unit = ()

  override protected def destroyDatabase(): Unit = ()
}

object PostgresTestDatabase {

  import slick.jdbc.PostgresProfile.api.*

  type SetupStatement = SqlAction[Int, NoStream, Effect]

  val password: String = "user"

  def userName(dbName: String): String = s"$dbName-user"

  def createDatabase(dbName: String): SetupStatement =
    sqlu"""CREATE DATABASE "#$dbName""""

  private def createUser(dbName: String): SetupStatement =
    sqlu"""CREATE USER "#${userName(dbName)}" WITH PASSWORD '#$password'"""

  private def grantUserToCurrentUser(dbName: String): SetupStatement =
    sqlu"""GRANT "#${userName(dbName)}" TO CURRENT_USER"""

  private def createDatabaseOwnedByUser(dbName: String): SetupStatement =
    sqlu"""CREATE DATABASE "#$dbName" OWNER "#${userName(dbName)}""""

  def createStatements(dbName: String): Seq[SetupStatement] = Seq(
    createUser(dbName),
    grantUserToCurrentUser(dbName),
    createDatabaseOwnedByUser(dbName),
  )

  def dropStatements(dbName: String): Seq[SetupStatement] = Seq(
    sqlu"""DROP DATABASE IF EXISTS "#$dbName"""",
    sqlu"""DROP USER IF EXISTS "#${userName(dbName)}"""",
  )
}

object DbStorageSetup {

  val PostgresVersion: String = "17"

  val PostgresImage: String = s"${PostgreSQLContainer.IMAGE}:$PostgresVersion"

  def env(name: String): String =
    sys.env.getOrElse(name, sys.error(s"Environment variable not set [$name]"))

  def envDbName: String = env("POSTGRES_DB")

  def envBasicConfig(dbName: String): DbBasicConfig = DbBasicConfig(
    username = env("POSTGRES_USER"),
    password = env("POSTGRES_PASSWORD"),
    dbName = dbName,
    host = sys.env.getOrElse("POSTGRES_HOST", "localhost"),
    port = 5432,
  )

  def containerBasicConfig(container: PostgreSQLContainer): DbBasicConfig = DbBasicConfig(
    username = container.getUsername,
    password = container.getPassword,
    dbName = container.getDatabaseName,
    host = container.getHost,
    port = container.getFirstMappedPort,
  )

  /** Whether to use a local test container rather than the Postgres provided through the
    * environment (`POSTGRES_*` variables), see [[postgres]] for the rules. Using a test container
    * can be forced by setting the `DB_FORCE_TEST_CONTAINER` environment variable or through
    * `forceTestContainer`.
    */
  def useTestContainer(forceTestContainer: Boolean = false): Boolean = {
    val isCI = sys.env.contains("CI")
    val isMachine = sys.env.contains("MACHINE")
    val useTestContainerByForce = sys.env.contains("DB_FORCE_TEST_CONTAINER") || forceTestContainer

    useTestContainerByForce || !isCI || isMachine
  }

  def newPostgresContainer(withQueryStats: Boolean = collectQueryStats): PostgreSQLContainer = {
    // TODO(#29774): switch to always running with the published `query-stats-postgres` image when available
    val container =
      if (withQueryStats) {
        new PostgreSQLContainer(
          DockerImageName
            .parse("digitalasset/query-stats-postgres:local")
            .asCompatibleSubstituteFor(PostgresImage)
        )
      } else {
        new PostgreSQLContainer(PostgresImage)
      }

    // up the connection limit to deal with everyone using connection pools in tests that can run concurrently.
    // we also have a matching max connections limit set in the CircleCI postgres executor (`.circle/config.yml`)
    val command =
      // TODO(#29774): switch to always running with the published `query-stats-postgres` image when available
      if (withQueryStats) {
        container.getCommandParts.toSeq ++ Seq(
          "-c",
          "max_connections=500",
          "-c",
          "shared_preload_libraries=pg_store_plans,pg_stat_statements",
          "-c",
          "pg_store_plans.plan_format=json",
          "-c",
          "pg_store_plans.max=100000",
          "-c",
          "pg_store_plans.max_plan_length=10240",
          "-c",
          "pg_stat_statements.max=100000",
        )
      } else {
        container.getCommandParts.toSeq :+ "-c" :+ "max_connections=500"
      }
    container.setCommandParts(command.toArray)
    container.setBinds(
      java.util.List.of(
        Bind.parse("/tmp/canton/data-continuity-dumps:/tmp/canton/data-continuity-dumps")
      )
    )
    container
  }

  /** If we are running in CI on a docker executor opt to use a separate docker setup for testing
    * postgres. If we are running in CI on a `machine` use a local
    * [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer]. If we are running
    * locally use a local [org.testcontainers.containers.PostgreSQLContainer.PostgreSQLContainer].
    * See [[useTestContainer]].
    */
  def postgres(
      loggerFactory: NamedLoggerFactory,
      migrationMode: MigrationMode = MigrationMode.Standard,
      mkDbConfig: DbBasicConfig => Postgres = _.toPostgresDbConfig,
      forceTestContainer: Boolean = false,
      useDbNameO: Option[String] = None,
  )(implicit ec: ExecutionContext): PostgresDbStorageSetup =
    if (useTestContainer(forceTestContainer))
      new PostgresTestContainerSetup(migrationMode, mkDbConfig, loggerFactory).initialized()
    else new PostgresCISetup(migrationMode, mkDbConfig, useDbNameO, loggerFactory).initialized()

  def h2(
      loggerFactory: NamedLoggerFactory,
      migrationMode: MigrationMode = MigrationMode.Standard,
      mkDbConfig: DbBasicConfig => H2 = _.toH2DbConfig,
  )(implicit ec: ExecutionContext): H2DbStorageSetup =
    new H2DbStorageSetup(migrationMode, mkDbConfig, loggerFactory).initialized()

  final case class DbBasicConfig(
      username: String,
      password: String,
      dbName: String,
      host: String,
      port: Int,
      connectionPoolEnabled: Boolean =
        false, // disable by default, as the config is mainly used for setup / migration
  ) {

    private def configOfMap(map: Map[String, Object]): Config = {
      val connectionPoolConfig = ConfigFactory.parseString(
        if (connectionPoolEnabled) "connectionPool = HikariCP" else "connectionPool = disabled"
      )
      val config = ConfigFactory.parseMap(map.asJava)
      connectionPoolConfig.withFallback(config)
    }

    def toPostgresConfig: Config = configOfMap(
      Map[String, Object](
        "dataSourceClass" -> "org.postgresql.ds.PGSimpleDataSource",
        "properties.serverName" -> host,
        "properties.databaseName" -> dbName,
        "properties.portNumber" -> (port: Integer),
        "properties.user" -> username,
        "properties.password" -> password,
      )
    )

    def url: String =
      s"${DbConfig.postgresUrl(host, port, dbName)}?user=$username&password=$password"

    def toPostgresDbConfig: Postgres = Postgres(
      toPostgresConfig,
      parameters = DbParametersConfig(unsafeCleanOnValidationError = true),
    )

    def toH2Config: Config = configOfMap(
      Map(
        "driver" -> "org.h2.Driver",
        "url" -> DbConfig.h2Url(dbName),
        "user" -> username,
        "password" -> password,
      )
    )

    def toH2DbConfig: H2 = H2(toH2Config)
  }

  // TODO(#29774): Remove this flag when we can always use the `query-stats-postgres` image.
  val collectQueryStats: Boolean = sys.env.contains("CANTON_COLLECT_QUERY_STATS")
}
