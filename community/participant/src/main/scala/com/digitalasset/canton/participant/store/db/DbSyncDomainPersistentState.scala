// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateOld,
  SyncDomainPersistentStateX,
}
import com.digitalasset.canton.protocol.TargetDomainId
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{
  DbSequencedEventStore,
  DbSequencerCounterTrackerStore,
  SequencerClientDiscriminator,
}
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.db.{DbTopologyStore, DbTopologyStoreX}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}

import scala.concurrent.ExecutionContext

abstract class DbSyncDomainPersistentStateCommon(
    override val domainId: IndexedDomain,
    val protocolVersion: ProtocolVersion,
    storage: DbStorage,
    override val pureCryptoApi: CryptoPureApi,
    parameters: ParticipantStoreConfig,
    val caching: CachingConfigs,
    maxDbConnections: Int,
    val timeouts: ProcessingTimeout,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState
    with AutoCloseable
    with NoTracing {

  val eventLog: DbSingleDimensionEventLog[DomainEventLogId] = new DbSingleDimensionEventLog(
    DomainEventLogId(domainId),
    storage,
    indexedStringStore,
    ReleaseProtocolVersion.latest,
    timeouts,
    loggerFactory,
  )

  val contractStore: DbContractStore =
    new DbContractStore(
      storage,
      domainId,
      protocolVersion,
      parameters.maxItemsInSqlClause,
      maxDbConnections,
      caching.contractStore,
      dbQueryBatcherConfig = parameters.dbBatchAggregationConfig,
      insertBatchAggregatorConfig = parameters.dbBatchAggregationConfig,
      timeouts,
      loggerFactory,
    )
  val transferStore: DbTransferStore = new DbTransferStore(
    storage,
    TargetDomainId(domainId.item),
    protocolVersion,
    pureCryptoApi,
    timeouts,
    loggerFactory,
  )
  val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      domainId,
      enableAdditionalConsistencyChecks,
      parameters.maxItemsInSqlClause,
      indexedStringStore,
      timeouts,
      loggerFactory,
    )
  val contractKeyJournal: DbContractKeyJournal = new DbContractKeyJournal(
    storage,
    domainId,
    parameters.maxItemsInSqlClause,
    timeouts,
    loggerFactory,
  )
  private val client = SequencerClientDiscriminator.fromIndexedDomainId(domainId)
  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    client,
    protocolVersion,
    timeouts,
    loggerFactory,
  )
  val requestJournalStore: DbRequestJournalStore = new DbRequestJournalStore(
    domainId,
    storage,
    parameters.maxItemsInSqlClause,
    insertBatchAggregatorConfig = parameters.dbBatchAggregationConfig,
    replaceBatchAggregatorConfig = parameters.dbBatchAggregationConfig,
    enableAdditionalConsistencyChecks,
    timeouts,
    loggerFactory,
  )
  val acsCommitmentStore = new DbAcsCommitmentStore(
    storage,
    domainId,
    protocolVersion,
    pureCryptoApi,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )

  val parameterStore: DbDomainParameterStore =
    new DbDomainParameterStore(domainId.item, storage, timeouts, loggerFactory)
  val sequencerCounterTrackerStore =
    new DbSequencerCounterTrackerStore(client, storage, timeouts, loggerFactory)
  // TODO(i5660): Use the db-based send tracker store
  val sendTrackerStore = new InMemorySendTrackerStore()

  override def isMemory(): Boolean = false

  override def close() = Lifecycle.close(
    eventLog,
    contractStore,
    transferStore,
    activeContractStore,
    contractKeyJournal,
    sequencedEventStore,
    requestJournalStore,
    acsCommitmentStore,
    parameterStore,
    sequencerCounterTrackerStore,
    sendTrackerStore,
  )(logger)
}

class DbSyncDomainPersistentStateOld(
    domainId: IndexedDomain,
    protocolVersion: ProtocolVersion,
    storage: DbStorage,
    pureCryptoApi: CryptoPureApi,
    parameters: ParticipantStoreConfig,
    caching: CachingConfigs,
    maxDbConnections: Int,
    timeouts: ProcessingTimeout,
    enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends DbSyncDomainPersistentStateCommon(
      domainId,
      protocolVersion,
      storage,
      pureCryptoApi,
      parameters,
      caching,
      maxDbConnections,
      timeouts,
      enableAdditionalConsistencyChecks,
      indexedStringStore,
      loggerFactory,
      futureSupervisor,
    )
    with SyncDomainPersistentStateOld {

  val topologyStore =
    new DbTopologyStore(
      storage,
      DomainStore(domainId.item),
      timeouts,
      loggerFactory,
      futureSupervisor,
    )

  override def close() = {
    Lifecycle.close(
      topologyStore
    )(logger)
    super.close()
  }

}

class DbSyncDomainPersistentStateX(
    domainId: IndexedDomain,
    protocolVersion: ProtocolVersion,
    storage: DbStorage,
    pureCryptoApi: CryptoPureApi,
    parameters: ParticipantStoreConfig,
    cachingConfigs: CachingConfigs,
    maxDbConnections: Int,
    processingTimeouts: ProcessingTimeout,
    enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends DbSyncDomainPersistentStateCommon(
      domainId,
      protocolVersion,
      storage,
      pureCryptoApi,
      parameters,
      cachingConfigs,
      maxDbConnections,
      processingTimeouts,
      enableAdditionalConsistencyChecks,
      indexedStringStore,
      loggerFactory,
      futureSupervisor,
    )
    with SyncDomainPersistentStateX {

  val topologyStore =
    new DbTopologyStoreX(
      storage,
      DomainStore(domainId.item),
      processingTimeouts,
      loggerFactory,
    )

  override def close() = {
    Lifecycle.close(
      topologyStore
    )(logger)
    super.close()
  }

}
