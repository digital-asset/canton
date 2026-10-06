// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import org.scalatest.Suite

trait PostgresAroundSuite extends PostgresAround {
  self: Suite & NamedLogging =>

  @volatile
  private var database: Option[DbBasicConfig] = None

  protected def jdbcUrl: String = postgresDatabase.url

  protected def dbConfig: DbConfig =
    postgresDatabase.copy(connectionPoolEnabled = true).toPostgresDbConfig

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  protected def postgresDatabase: DbBasicConfig = database.get

  protected def lockIdSeed: Int =
    1000 // For postgres each test-suite uses different DB, so no unique lock-ids needed

  protected def createNewDatabase(): Unit =
    database = Some(createNewRandomDatabaseConfig())

  protected def dropDatabase(): Unit = {
    dropDatabase(postgresDatabase)
    database = None
  }
}
