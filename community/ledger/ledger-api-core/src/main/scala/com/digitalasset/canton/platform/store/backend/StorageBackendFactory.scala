// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.backend.h2.H2StorageBackendFactory
import com.digitalasset.canton.platform.store.backend.localstore.{
  IdentityProviderStorageBackend,
  PartyRecordStorageBackend,
  UserManagementStorageBackend,
}
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresStorageBackendFactory
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref

trait StorageBackendFactory {
  def createIngestionStorageBackend: IngestionStorageBackend[?]
  def createParameterStorageBackend(stringInterning: StringInterning): ParameterStorageBackend
  def createPartyStorageBackend(
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): PartyStorageBackend
  def createPartyRecordStorageBackend: PartyRecordStorageBackend
  def createCompletionStorageBackend(
      stringInterning: StringInterning,
      ledgerEndCache: LedgerEndCache,
      loggerFactory: NamedLoggerFactory,
  ): CompletionStorageBackend
  def createContractStorageBackend(
      stringInterning: StringInterning,
      ledgerEndCache: LedgerEndCache,
  ): ContractStorageBackend
  def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): EventStorageBackend
  def createDataSourceStorageBackend: DataSourceStorageBackend
  def createDBLockStorageBackend: DBLockStorageBackend
  def createIntegrityStorageBackend: IntegrityStorageBackend
  def createResetStorageBackend: ResetStorageBackend
  def createStringInterningStorageBackend: StringInterningStorageBackend
  def createUserManagementStorageBackend: UserManagementStorageBackend
  def createIdentityProviderConfigStorageBackend: IdentityProviderStorageBackend

  final def readStorageBackend(
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): ReadStorageBackend =
    ReadStorageBackend(
      partyStorageBackend =
        createPartyStorageBackend(participantId, ledgerEndCache, stringInterning),
      completionStorageBackend =
        createCompletionStorageBackend(stringInterning, ledgerEndCache, loggerFactory),
      contractStorageBackend = createContractStorageBackend(stringInterning, ledgerEndCache),
      eventStorageBackend =
        createEventStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
    )
}

object StorageBackendFactory {
  def of(dbType: DbType, loggerFactory: NamedLoggerFactory): StorageBackendFactory =
    dbType match {
      case DbType.H2Database => H2StorageBackendFactory
      case DbType.Postgres => PostgresStorageBackendFactory(loggerFactory)
    }
}

final case class ReadStorageBackend(
    partyStorageBackend: PartyStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    contractStorageBackend: ContractStorageBackend,
    eventStorageBackend: EventStorageBackend,
)
