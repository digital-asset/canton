// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.memory.{
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
  InMemorySequencerCounterTrackerStore,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore

import scala.concurrent.ExecutionContext

class InMemorySyncDomainPersistentState(
    override val domainId: IndexedDomain,
    override val pureCryptoApi: CryptoPureApi,
    override val enableAdditionalConsistencyChecks: Boolean,
    val loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncDomainPersistentState {

  val eventLog = new InMemorySingleDimensionEventLog(DomainEventLogId(domainId), loggerFactory)
  val contractStore = new InMemoryContractStore(loggerFactory)
  val activeContractStore = new InMemoryActiveContractStore(loggerFactory)
  val contractKeyJournal = new InMemoryContractKeyJournal(loggerFactory)
  val transferStore = new InMemoryTransferStore(domainId.item, loggerFactory)
  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
  val parameterStore = new InMemoryDomainParameterStore()
  val sequencerCounterTrackerStore =
    new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
  val sendTrackerStore = new InMemorySendTrackerStore()
  val causalDependencyStore =
    new InMemorySingleDomainCausalDependencyStore(domainId.item, loggerFactory)
  val topologyStore =
    new InMemoryTopologyStore(DomainStore(domainId.item), loggerFactory, timeouts, futureSupervisor)

  override def close(): Unit = ()
}
