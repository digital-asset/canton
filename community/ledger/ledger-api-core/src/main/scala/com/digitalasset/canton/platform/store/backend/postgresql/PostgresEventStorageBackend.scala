// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.long
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.CannotAcquireAllRowLocksException
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import org.postgresql.util.PSQLException

import java.sql.Connection

class PostgresEventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = PostgresQueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
    ) {

  override def lockExclusivelyPruningProcessingTable(connection: Connection): Unit = {
    assert(!connection.getAutoCommit)
    SQL"""LOCK TABLE lapi_pruning_candidate_deactivated IN ACCESS EXCLUSIVE MODE"""
      .execute()(connection)
      .discard
  }

  override def lockExclusivelyContractPruningProcessingTable(connection: Connection): Unit = {
    assert(!connection.getAutoCommit)
    SQL"""LOCK TABLE lapi_pruning_contract_candidate IN ACCESS EXCLUSIVE MODE"""
      .execute()(connection)
      .discard
  }

  override def readLockInternalContractIds(
      internalContractIds: Set[Long]
  )(connection: Connection): Set[Long] =
    internalContractIds -- SQL"""
      SELECT internal_contract_id
      FROM par_contracts
      WHERE internal_contract_id ${PostgresQueryStrategy.anyOf(internalContractIds)}
      ORDER BY internal_contract_id
      FOR KEY SHARE
      """
      .withFetchSize(Some(internalContractIds.size))
      .asVectorOf(long("internal_contract_id"))(connection)
      .toSet

  override def writeLockInternalContractIds(whereInternalContractIdExprs: CompositeSql)(
      connection: Connection
  ): Unit =
    try {
      SQL"""
      SELECT internal_contract_id
      FROM par_contracts
      WHERE internal_contract_id $whereInternalContractIdExprs
      ORDER BY internal_contract_id
      FOR UPDATE NOWAIT
      """.execute()(connection).discard
    } catch {
      case pSQLException: PSQLException
          // 55P03 	lock_not_available (reference from https://www.postgresql.org/docs/current/errcodes-appendix.html)
          if pSQLException.getServerErrorMessage.getSQLState == "55P03" =>
        throw new CannotAcquireAllRowLocksException
    }
}
