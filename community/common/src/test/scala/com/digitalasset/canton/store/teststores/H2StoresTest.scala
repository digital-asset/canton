// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.teststores

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.{DbStorageSetup, H2DbStorageSetup, MigrationMode}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal

trait H2StoresTest
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with HasCloseContext
    with FlagCloseable {
  self: Suite & NamedLogging =>

  // Use this to mimic InMemory stores behavior as close as possible
  // Some tests rely on precise sequencing of actions
  protected val h2InMemoryEc: ExecutionContext = DirectExecutionContext(noTracingLogger)

  /** Flag to define the migration mode for the schemas */
  private def migrationMode: MigrationMode =
    if (BaseTest.testedProtocolVersion.isDev) MigrationMode.DevVersion
    else MigrationMode.Standard

  private def mkDbConfig(basicConfig: DbBasicConfig): DbConfig.H2 = basicConfig.toH2DbConfig

  private def createSetup(): H2DbStorageSetup =
    DbStorageSetup.h2(loggerFactory, migrationMode, mkDbConfig)(h2InMemoryEc)

  protected def tablesToClean: Seq[String] = Seq()

  private def cleanDb(
      storage: DbStorage
  )(implicit tc: TraceContext): FutureUnlessShutdown[?] = {
    import storage.api.*
    val tables = tablesToClean.distinct

    storage.update_(
      DBIO.seq(tables.map(table => sqlu"delete from #$table")*),
      functionFullName,
    )(tc, closeContext, implicitly)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var setup: DbStorageSetup = _

  /** Stores the db storage implementation. Will throw if accessed before the test has started */
  protected lazy val inMemoryH2Storage: DbStorage =
    Option(setup).map(_.storage).getOrElse(sys.error("Test has not started"))

  override def beforeAll(): Unit = TraceContext.withNewTraceContext("test") { implicit tc =>
    // Non-standard order. Setup needs to be created first, because super can be MyDbTest and therefore super.beforeAll
    // may already access setup.
    try {
      setup = createSetup().initialized()
      setup.migrateDb()
      super.beforeAll()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("beforeAll failed", e)
        throw e
    }
  }

  override def afterAll(): Unit = TraceContext.withNewTraceContext("test") { implicit tc =>
    try {
      cleanup()
      close()
      super.afterAll()
      inMemoryH2Storage.close()
      setup.close()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("afterAll failed", e)
        throw e
    }
  }

  override def beforeEach(): Unit = TraceContext.withNewTraceContext("test") { implicit tc =>
    try {
      cleanup()
      super.beforeEach()
    } catch {
      case NonFatal(e) =>
        // Logging the error, as an exception in this method will abort the test suite with no log output.
        logger.error("beforeEach failed", e)
        throw e
    }
  }

  private def cleanup()(implicit tc: TraceContext): Unit =
    Await.result(cleanDb(inMemoryH2Storage), 10.seconds)

}
