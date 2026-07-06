// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.testing.postgresql

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig

final case class PostgresDatabase private[postgresql] (
    private val server: PostgresServer,
    databaseName: String,
    userName: String,
    password: String,
) {
  def hostName: String = server.hostName

  def port: Int = server.port

  def urlWithoutCredentials: String =
    s"jdbc:postgresql://$hostName:$port/$databaseName"

  def url: String =
    s"$urlWithoutCredentials?user=$userName&password=$password"

  def toDbConfig(connectionPoolEnabled: Boolean = true): DbConfig =
    DbBasicConfig(
      username = userName,
      password = password,
      dbName = databaseName,
      host = hostName,
      port = port,
      connectionPoolEnabled = connectionPoolEnabled,
    ).toPostgresDbConfig

  override def toString: String = url
}
