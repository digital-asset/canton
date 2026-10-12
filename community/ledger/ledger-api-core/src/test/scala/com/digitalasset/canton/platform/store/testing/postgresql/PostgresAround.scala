// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.{DbStorageSetup, PostgresTestDatabase}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.typesafe.config.ConfigValueFactory
import org.testcontainers.postgresql.PostgreSQLContainer
import slick.jdbc.PostgresProfile.api.*

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

trait PostgresAround {
  this: NamedLogging =>
  import PostgresAround.*

  private val serverConfig: AtomicReference[DbBasicConfig] = new AtomicReference
  private val ownedServerContainer: AtomicReference[Option[PostgreSQLContainer]] =
    new AtomicReference(None)

  protected def connectToPostgresqlServer(): Unit = {
    val isTestContainer = DbStorageSetup.useTestContainer()
    val config =
      if (isTestContainer) {
        // using own temporal resource
        // these tests do not analyze query statistics, so no need for the query stats image
        val container = DbStorageSetup.newPostgresContainer(withQueryStats = false)
        ownedServerContainer.set(Some(container))
        noTracingLogger.info("Starting PostgreSQL Container...")
        container.start()
        noTracingLogger.info("PostgreSQL Container started.")
        DbStorageSetup.containerBasicConfig(container)
      } else {
        // using specified resource
        DbStorageSetup.envBasicConfig(DbStorageSetup.envDbName)
      }
    serverConfig.set(config)
    val serverKind = if (isTestContainer) "PostgreSQL Container" else "PostgreSQL"
    noTracingLogger.info(s"Using $serverKind on ${config.host}:${config.port}.")
  }

  protected def disconnectFromPostgresqlServer(): Unit =
    ownedServerContainer.get().foreach { container =>
      noTracingLogger.info("Stopping PostgreSQL Container...")
      container.close()
      noTracingLogger.info("PostgreSQL Container stopped.")
    }

  protected def createNewRandomDatabaseConfig(): DbBasicConfig = {
    val databaseName = UUID.randomUUID().toString
    executeAdminStatement(PostgresTestDatabase.createStatements(databaseName)*)
    serverConfig
      .get()
      .copy(
        dbName = databaseName,
        username = PostgresTestDatabase.userName(databaseName),
        password = PostgresTestDatabase.password,
      )
  }

  protected def dropDatabase(database: DbBasicConfig): Unit =
    executeAdminStatement(PostgresTestDatabase.dropStatements(database.dbName)*)

  private def executeAdminStatement(statements: PostgresTestDatabase.SetupStatement*): Unit =
    TraceContext.withNewTraceContext("postgres_admin_statement") { implicit traceContext =>
      ResourceUtil.withResource(openConnection(serverConfig.get()))(connection =>
        DefaultProcessingTimeouts.default.await("execute admin statement")(
          connection.run(DBIO.seq(statements*))
        )
      )
    }
}

object PostgresAround {

  private def openConnection(config: DbBasicConfig): Database =
    Database.forConfig(
      path = "",
      config = config.toPostgresConfig.withValue("numThreads", ConfigValueFactory.fromAnyRef(1)),
    )
}
