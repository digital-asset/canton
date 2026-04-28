// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.common.{
  ComposableQuery,
  EventStorageBackendTemplate,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection

class H2EventStorageBackend(
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    loggerFactory: NamedLoggerFactory,
) extends EventStorageBackendTemplate(
      queryStrategy = H2QueryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
    ) {

  // no need for locking in H2 as we access H2 DB on a single connection
  override def lockExclusivelyPruningProcessingTable(connection: Connection): Unit = ()

  // no need for locking in H2 as we access H2 DB on a single connection
  override def lockExclusivelyContractPruningProcessingTable(connection: Connection): Unit = ()

  // no need for locking in H2 as we access H2 DB on a single connection
  override def readLockInternalContractIds(internalContractIds: Set[Long])(
      connection: Connection
  ): Set[Long] = Set.empty

  // no need for locking in H2 as we access H2 DB on a single connection
  override def writeLockInternalContractIds(
      whereInternalContractIdExprs: ComposableQuery.CompositeSql
  )(connection: Connection): Unit = ()
}
