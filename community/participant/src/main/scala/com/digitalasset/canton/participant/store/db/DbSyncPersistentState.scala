// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbSequencedEventStore
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
  IndexedTopologyStoreId,
  SendTrackerStore,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ReassignmentTag

import scala.concurrent.ExecutionContext

class DbLogicalSyncPersistentState(
    override val synchronizerIdx: IndexedSynchronizer,
    storage: DbStorage,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    contractStore: ContractStore,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends LogicalSyncPersistentState {

  private val timeouts = parameters.processingTimeouts

  override val enableAdditionalConsistencyChecks: Boolean =
    parameters.enableAdditionalConsistencyChecks

  override val activeContractStore: DbActiveContractStore =
    new DbActiveContractStore(
      storage,
      synchronizerIdx,
      Option.when(enableAdditionalConsistencyChecks)(
        parameters.activationFrequencyForWarnAboutConsistencyChecks
      ),
      parameters.stores.journalPruning.toInternal,
      parameters.batchingConfig,
      indexedStringStore,
      timeouts,
      loggerFactory,
    )

  override val acsCommitmentStore = new DbAcsCommitmentStore(
    storage,
    synchronizerIdx,
    acsCounterParticipantConfigStore,
    timeouts,
    loggerFactory,
    ledgerApiStore.map(_.stringInterningView),
  )

  override val acsInspection: AcsInspection =
    new AcsInspection(
      lsid,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )

  override val reassignmentStore: DbReassignmentStore = new DbReassignmentStore(
    storage,
    ReassignmentTag.Target(synchronizerIdx),
    indexedStringStore,
    futureSupervisor,
    exitOnFatalFailures = parameters.exitOnFatalFailures,
    parameters.batchingConfig,
    timeouts,
    loggerFactory,
  )

  override def close(): Unit =
    LifeCycle.close(activeContractStore, acsCommitmentStore)(logger)
}

class DbPhysicalSyncPersistentState(
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    indexedTopologyStoreId: IndexedTopologyStoreId,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    storage: DbStorage,
    crypto: SynchronizerCrypto,
    parameters: ParticipantNodeParameters,
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends PhysicalSyncPersistentState
    with AutoCloseable
    with NoTracing {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  private val timeouts = parameters.processingTimeouts
  private val batching = parameters.batchingConfig

  val sequencedEventStore = new DbSequencedEventStore(
    storage,
    physicalSynchronizerIdx,
    timeouts,
    loggerFactory,
  )
  val requestJournalStore: DbRequestJournalStore = new DbRequestJournalStore(
    physicalSynchronizerIdx,
    storage,
    insertBatchAggregatorConfig = batching.aggregator,
    replaceBatchAggregatorConfig = batching.aggregator,
    timeouts,
    loggerFactory,
  )

  val parameterStore: DbSynchronizerParameterStore =
    new DbSynchronizerParameterStore(
      psid,
      storage,
      timeouts,
      loggerFactory,
    )

  val sendTrackerStore: SendTrackerStore = SendTrackerStore()

  val submissionTrackerStore =
    new DbSubmissionTrackerStore(
      storage,
      physicalSynchronizerIdx,
      parameters.stores.journalPruning.toInternal,
      timeouts,
      loggerFactory,
    )

  override val topologyStore =
    new DbTopologyStore(
      storage,
      SynchronizerStore(psid),
      indexedTopologyStoreId,
      staticSynchronizerParameters.protocolVersion,
      timeouts,
      parameters.batchingConfig,
      loggerFactory,
    )

  override def close(): Unit =
    LifeCycle.close(
      topologyStore,
      sequencedEventStore,
      requestJournalStore,
      parameterStore,
      sendTrackerStore,
      submissionTrackerStore,
    )(logger)

  override def isMemory: Boolean = false
}
