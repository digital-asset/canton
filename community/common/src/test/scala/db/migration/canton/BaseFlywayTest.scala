// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.config.{DbConfig, PartitionConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.store.db.DbStorageSetup
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Base for tests that drive Flyway migrations directly against a real database.
  */
trait BaseFlywayTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterAll {

  protected def migrationLocations: Seq[String]
  protected def driverClassName: String
  protected def jdbcUrl: String
  protected def dbUser: String
  protected def dbPassword: String

  protected def openConnection(): Connection = {
    Class.forName(driverClassName).discard
    DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
  }

  protected def withConnection[A](f: Connection => A): A =
    ResourceUtil.withResource(openConnection())(f)

  /** Builds a Flyway instance migrating up to `target` (or to the latest version if `None`). */
  protected def flyway(targetVersion: Option[String]): Flyway = {
    val configuration = Flyway.configure
      .locations(migrationLocations*)
      .dataSource(jdbcUrl, dbUser, dbPassword)
      .placeholders(
        Map(
          "initialBftOrdererTablesPartitionSize" ->
            PartitionConfig().initialBftOrdererTablesPartitionSize.toString
        ).asJava
      )
    targetVersion
      .fold(configuration)(t => configuration.target(MigrationVersion.fromVersion(t)))
      .load()
  }
}

/** Runs a [[BaseFlywayTest]] against an in-memory H2 database. */
trait BaseFlywayTestH2 extends BaseFlywayTest {

  private val dbName = s"flyway_test_${UUID.randomUUID().toString.replace("-", "")}"
  override protected val driverClassName = "org.h2.Driver"
  override protected val jdbcUrl =
    s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;LOCK_TIMEOUT=10000;DB_CLOSE_DELAY=-1"
  override protected val dbUser = ""
  override protected val dbPassword = ""
  override protected val migrationLocations =
    Seq(DbConfig.h2MigrationsPathStable, DbConfig.h2DefaultTableSettingsPath)

  // Keep one connection open so the in-memory database survives between Flyway runs.
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private var keepAlive: Connection = _

  override def beforeAll(): Unit = {
    keepAlive = openConnection()
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally keepAlive.close()
}

/** Runs a [[BaseFlywayTest]] against a Postgres database.
  */
trait BaseFlywayTestPostgres extends BaseFlywayTest {

  /** Name of the database derived from the suite's class name. */
  private def dbName: String =
    getClass.getSimpleName.replaceAll("[^a-zA-Z0-9]", "").toLowerCase.take(63)

  private lazy val setup: DbStorageSetup =
    DbStorageSetup.postgres(
      loggerFactory,
      useDbNameO = Some(dbName),
    )(parallelExecutionContext)

  override protected val driverClassName = "org.postgresql.Driver"
  override protected lazy val jdbcUrl =
    s"jdbc:postgresql://${setup.basicConfig.host}:${setup.basicConfig.port}/${setup.basicConfig.dbName}"
  override protected lazy val dbUser = setup.basicConfig.username
  override protected lazy val dbPassword = setup.basicConfig.password
  override protected val migrationLocations =
    Seq(DbConfig.postgresMigrationsPathStable, DbConfig.postgresDefaultTableSettingsPath)

  override def afterAll(): Unit =
    try super.afterAll()
    finally setup.close()
}
