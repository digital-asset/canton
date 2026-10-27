// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.*
import com.digitalasset.canton.platform.store.backend.common.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref

final case class PostgresStorageBackendFactory(
    disableMergeAndHashJoins: Boolean,
    loggerFactory: NamedLoggerFactory,
) extends StorageBackendFactory
    with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[?] =
    new IngestionStorageBackendTemplate(PGSchema.schema)

  private val queryStrategy = PostgresQueryStrategy(disableMergeAndHashJoins)

  override def createParameterStorageBackend(
      stringInterning: StringInterning
  ): ParameterStorageBackend =
    new ParameterStorageBackendImpl(queryStrategy, stringInterning)

  override def createPartyStorageBackend(
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): PartyStorageBackend =
    new PartyStorageBackendTemplate(participantId, ledgerEndCache, stringInterning)

  override def createCompletionStorageBackend(
      stringInterning: StringInterning,
      ledgerEndCache: LedgerEndCache,
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(stringInterning, ledgerEndCache, loggerFactory)

  override def createContractStorageBackend(
      stringInterning: StringInterning,
      ledgerEndCache: LedgerEndCache,
  ): ContractStorageBackend =
    new PostgresContractStorageBackend(queryStrategy, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new PostgresEventStorageBackend(
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
      queryStrategy = queryStrategy,
    )

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    PostgresDataSourceStorageBackend(loggerFactory)

  override val createDBLockStorageBackend: DBLockStorageBackend =
    PostgresDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    PostgresResetStorageBackend

}
