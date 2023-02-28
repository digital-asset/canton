// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.db.DbSyncDomainPersistentState
import com.digitalasset.canton.participant.store.memory.InMemorySyncDomainPersistentState
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** The state of a synchronization domain that is independent of the connectivity to the domain. */
trait SyncDomainPersistentState extends NamedLogging with AutoCloseable {
  protected[participant] def loggerFactory: NamedLoggerFactory

  /** The crypto operations used on the domain */
  def pureCryptoApi: CryptoPureApi
  def domainId: IndexedDomain
  def enableAdditionalConsistencyChecks: Boolean
  def eventLog: SingleDimensionEventLog[DomainEventLogId]
  def contractStore: ContractStore
  def transferStore: TransferStore
  def activeContractStore: ActiveContractStore
  def contractKeyJournal: ContractKeyJournal
  def sequencedEventStore: SequencedEventStore
  def sequencerCounterTrackerStore: SequencerCounterTrackerStore
  def sendTrackerStore: SendTrackerStore
  def requestJournalStore: RequestJournalStore
  def acsCommitmentStore: AcsCommitmentStore
  def parameterStore: DomainParameterStore
  def causalDependencyStore: SingleDomainCausalDependencyStore
}

object SyncDomainPersistentState {
  def apply(
      storage: Storage,
      domainAlias: DomainAlias,
      domainId: IndexedDomain,
      protocolVersion: ProtocolVersion,
      pureCryptoApi: CryptoPureApi,
      parameters: ParticipantStoreConfig,
      caching: CachingConfigs,
      maxDbConnections: Int,
      processingTimeouts: ProcessingTimeout,
      enableAdditionalConsistencyChecks: Boolean,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SyncDomainPersistentState = {
    val domainLoggerFactory = loggerFactory.append("domain-alias", domainAlias.unwrap)
    storage match {
      case _: MemoryStorage =>
        new InMemorySyncDomainPersistentState(
          domainId,
          pureCryptoApi,
          enableAdditionalConsistencyChecks,
          domainLoggerFactory,
          processingTimeouts,
        )
      case db: DbStorage =>
        new DbSyncDomainPersistentState(
          domainId,
          protocolVersion,
          db,
          pureCryptoApi,
          parameters,
          caching,
          maxDbConnections,
          processingTimeouts,
          enableAdditionalConsistencyChecks,
          indexedStringStore,
          domainLoggerFactory,
        )
    }
  }

}
