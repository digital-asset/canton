// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.*
import com.digitalasset.canton.platform.store.backend.common.*
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

object PostgresStorageBackendFactory
    extends StorageBackendFactory
    with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(PostgresQueryStrategy, PGSchema.schema)

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    new PackageStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache)

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    new ConfigurationStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache)

  override def createCompletionStorageBackend(
      stringInterning: StringInterning
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(PostgresQueryStrategy, stringInterning)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new PostgresEventStorageBackend(
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
    )

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    PostgresDataSourceStorageBackend()

  override val createDBLockStorageBackend: DBLockStorageBackend =
    PostgresDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    PostgresResetStorageBackend

}