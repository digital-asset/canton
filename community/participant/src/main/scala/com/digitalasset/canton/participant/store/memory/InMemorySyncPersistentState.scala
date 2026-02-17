// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

class InMemoryLogicalSyncPersistentState(
    override val synchronizerIdx: IndexedSynchronizer,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LogicalSyncPersistentState {

  override val activeContractStore =
    new InMemoryActiveContractStore(
      indexedStringStore,
      loggerFactory,
    )

  val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(
      synchronizerIdx.synchronizerId,
      acsCounterParticipantConfigStore,
      loggerFactory,
    )

  override val acsInspection: AcsInspection =
    new AcsInspection(
      synchronizerIdx.synchronizerId,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )

  override val reassignmentStore =
    new InMemoryReassignmentStore(Target(synchronizerIdx.item), loggerFactory)

  override def close(): Unit = ()
}

class InMemoryPhysicalSyncPersistentState(
    crypto: SynchronizerCrypto,
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends PhysicalSyncPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val parameterStore = new InMemorySynchronizerParameterStore()
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override val topologyStore =
    new InMemoryTopologyStore(
      SynchronizerStore(psid),
      staticSynchronizerParameters.protocolVersion,
      loggerFactory,
      timeouts,
    )

  override def isMemory: Boolean = true

  override def close(): Unit = ()

}
