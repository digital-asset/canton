// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.{CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ParticipantStoreConfig
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{
  DbSequencedEventStore,
  DbSequencerCounterTrackerStore,
  SequencerClientDiscriminator,
}
import com.digitalasset.canton.store.memory.InMemorySendTrackerStore
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.tracing.NoTracing

import scala.concurrent.ExecutionContext

class DbSyncDomainPersistentState(
    override val domainId: IndexedDomain,
    storage: DbStorage,
    override val pureCryptoApi: CryptoPureApi,
    parameters: ParticipantStoreConfig,
    caching: CachingConfigs,
    processingTimeouts: ProcessingTimeout,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState
    with AutoCloseable
    with NoTracing {

  val eventLog = new DbSingleDimensionEventLog(
    DomainEventLogId(domainId),
    storage,
    indexedStringStore,
    loggerFactory,
  )

  val contractStore =
    new DbContractStore(
      storage,
      domainId,
      parameters.maxItemsInSqlClause,
      caching.contractStore,
      parameters.dbBatchAggregationConfig,
      processingTimeouts,
      loggerFactory,
    )
  val transferStore = new DbTransferStore(storage, domainId.item, pureCryptoApi, loggerFactory)
  val activeContractStore =
    new DbActiveContractStore(
      storage,
      domainId,
      enableAdditionalConsistencyChecks,
      parameters.maxItemsInSqlClause,
      indexedStringStore,
      loggerFactory,
    )
  val contractKeyJournal = new DbContractKeyJournal(
    storage,
    domainId,
    parameters.maxItemsInSqlClause,
    loggerFactory,
  )
  private val client = SequencerClientDiscriminator.fromIndexedDomainId(domainId)
  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    client,
    processingTimeouts,
    loggerFactory,
  )
  val requestJournalStore = new DbRequestJournalStore(
    domainId,
    storage,
    parameters.maxItemsInSqlClause,
    insertBatchAggregatorConfig = parameters.dbBatchAggregationConfig,
    replaceBatchAggregatorConfig = parameters.dbBatchAggregationConfig,
    enableAdditionalConsistencyChecks,
    loggerFactory,
  )
  val acsCommitmentStore = new DbAcsCommitmentStore(storage, domainId, pureCryptoApi, loggerFactory)

  val parameterStore = new DbDomainParameterStore(domainId.item, storage, loggerFactory)
  val sequencerCounterTrackerStore =
    new DbSequencerCounterTrackerStore(client, storage, loggerFactory)
  //TODO(i5660): Use the db-based send tracker store
  val sendTrackerStore = new InMemorySendTrackerStore()
  val causalDependencyStore =
    new DbSingleDomainCausalDependencyStore(domainId.item, storage, loggerFactory)

  override def close() = Lifecycle.close(contractStore)(logger)
}
