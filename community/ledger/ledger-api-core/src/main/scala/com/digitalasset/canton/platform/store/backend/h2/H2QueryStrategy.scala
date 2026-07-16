// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.{ComposableQuery, QueryStrategy}

import java.sql.Connection

object H2QueryStrategy extends QueryStrategy {

  override def analyzeTable(tableName: String): ComposableQuery.CompositeSql =
    cSQL"ANALYZE TABLE #$tableName"

  override def cleanTable(tableName: String)(implicit connection: Connection): Unit =
    SQL"""TRUNCATE TABLE #$tableName""".execute().discard

  override def vacuumTable(tableName: String)(implicit connection: Connection): Unit = ()

  override def reindexTableIfPossible(tableName: String)(implicit connection: Connection): Boolean =
    true

  override def vacuumAndReindexTable(
      tableName: String
  )(implicit connection: Connection, errorLoggingContext: ErrorLoggingContext): Unit = ()
}
