// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.SqlParser.long
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.CannotAcquireAllRowLocksException
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.EventStorageBackendTemplate
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.DbLockMeta
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

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

  override def lockExclusivelyPruningProcessingTable(
      dbLockMeta: DbLockMeta
  )(implicit connection: Connection, errorLoggingContext: ErrorLoggingContext): Unit =
    PostgresQueryStrategy.acquireLock(dbLockMeta) {
      errorLoggingContext.info("Lock lapi_pruning_candidate_deactivated table")
      SQL"LOCK TABLE lapi_pruning_candidate_deactivated IN SHARE ROW EXCLUSIVE MODE"
        .execute()(connection)
        .discard
      errorLoggingContext.info("Locked lapi_pruning_candidate_deactivated table")
    }

  override def lockExclusivelyContractPruningProcessingTable(
      dbLockMeta: DbLockMeta
  )(implicit connection: Connection, errorLoggingContext: ErrorLoggingContext): Unit =
    PostgresQueryStrategy.acquireLock(dbLockMeta) {
      errorLoggingContext.info("Lock lapi_pruning_contract_candidate table")
      SQL"LOCK TABLE lapi_pruning_contract_candidate IN SHARE ROW EXCLUSIVE MODE"
        .execute()(connection)
        .discard
      errorLoggingContext.info("Locked lapi_pruning_contract_candidate table")
    }

  override def readLockInternalContractIds(
      internalContractIds: Set[Long],
      dbLockMeta: DbLockMeta,
  )(implicit connection: Connection, errorLoggingContext: ErrorLoggingContext): Set[Long] =
    PostgresQueryStrategy.acquireLock(dbLockMeta) {
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
    }

  override def writeLockInternalContractIds(whereInternalContractIdExprs: CompositeSql)(
      connection: Connection
  ): Unit =
    PostgresQueryStrategy.handleNowaitException(
      SQL"""
      SELECT internal_contract_id
      FROM par_contracts
      WHERE internal_contract_id $whereInternalContractIdExprs
      ORDER BY internal_contract_id
      FOR UPDATE NOWAIT
      """.execute()(connection).discard
    )(uponLockNotAvailable = throw new CannotAcquireAllRowLocksException)
}
