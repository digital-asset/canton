// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.config.{DbConfig, H2DbConfig, PostgresDbConfig}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** Base test for writing a database backed storage test.
  * To ensure idempotency and safety under retries of the store each write operation is executed twice.
  * Each database should provide a DbTest implementation that can then be mixed into a storage test to provide the actual backend.
  * See DbCryptoVaultStoreTest for example usage.
  */
trait DbTest
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with FlagCloseable
    with HasCloseContext {
  this: Suite with HasExecutionContext with NamedLogging =>

  type Config <: DbConfig

  /** Flag to define the migration mode for the schemas */
  def migrationMode: MigrationMode = MigrationMode.Standard

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var setup: DbStorageSetup[Config] = _

  /** Stores the db storage implementation. Will throw if accessed before the test has started */
  protected lazy val storage: DbStorageIdempotency = {
    val s = Option(setup).map(_.storage).getOrElse(sys.error("Test has not started"))
    new DbStorageIdempotency(s, timeouts, loggerFactory)
  }

  override def beforeAll(): Unit = {
    setup = createSetup()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      cleanup()
      close()
      super.afterAll()
    } finally setup.close()
  }

  override def beforeEach(): Unit = {
    cleanup()
    super.beforeEach()
  }

  private def cleanup(): Unit = {
    // Use the underlying storage for clean-up operations, so we don't run clean-ups twice
    Await.result(cleanDb(storage.underlying), 10.seconds)
  }

  def createSetup(): DbStorageSetup[Config]

  /** Hook for cleaning database before running next test. */
  def cleanDb(storage: DbStorage): Future[_]
}

/** Run db test against h2 */
trait H2Test extends DbTest with BaseTest with HasExecutionContext {
  this: Suite =>

  override type Config = H2DbConfig

  override def createSetup(): DbStorageSetup[H2DbConfig] =
    DbStorageSetup.h2(migrationMode, loggerFactory)
}

/** Run db test for running against postgres */
trait PostgresTest extends DbTest with BaseTest with HasExecutionContext {
  this: Suite =>

  override type Config = PostgresDbConfig

  override def createSetup(): DbStorageSetup[PostgresDbConfig] =
    DbStorageSetup.postgresFunctionalTestSetup(loggerFactory, migrationMode)
}

trait DevDbTest {
  this: DbTest =>

  override def migrationMode: MigrationMode = MigrationMode.DevVersion

}
