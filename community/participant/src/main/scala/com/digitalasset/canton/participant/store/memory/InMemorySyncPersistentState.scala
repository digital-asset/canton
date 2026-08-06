// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation.PendingOnboardingClearanceStore
import com.digitalasset.canton.participant.store.{
  AcsCommitmentPeriodStore,
  AcsCommitmentSenderWatermarkStore,
  AcsCounterParticipantConfigStore,
  AcsDigestStore,
  AcsInspection,
  BatchingAcsDigestStore,
  ContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.memory.{
  InMemoryPendingOperationStore,
  InMemorySendTrackerStore,
  InMemorySequencedEventStore,
}
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

class InMemoryLogicalSyncPersistentState(
    override val synchronizerIdx: IndexedSynchronizer,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LogicalSyncPersistentState {
  override val enableAdditionalConsistencyChecks: Boolean =
    parameters.enableAdditionalConsistencyChecks

  override val activeContractStore =
    new InMemoryActiveContractStore(
      indexedStringStore,
      loggerFactory,
    )

  override val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(
      synchronizerIdx.synchronizerId,
      acsCounterParticipantConfigStore,
      loggerFactory,
    )

  override val acsDigestStore: AcsDigestStore = {
    val underlying = InMemoryAcsDigestStore.create(
      ledgerApiStore.map(_.stringInterningView),
      loggerFactory,
    )
    new BatchingAcsDigestStore(
      underlying,
      parameters.acsCommitments.loadBatching,
      parameters.processingTimeouts,
      loggerFactory,
    )
  }

  override val acsCommitmentPeriodStore: AcsCommitmentPeriodStore =
    new InMemoryAcsCommitmentPeriodStore(
      ledgerApiStore.map(_.stringInterningView),
      loggerFactory,
      enableAdditionalConsistencyChecks,
    )

  override val acsCommitmentSenderWatermarkStore: AcsCommitmentSenderWatermarkStore =
    new InMemoryAcsCommitmentSenderWatermarkStore(loggerFactory)

  override val acsInspection: AcsInspection =
    new AcsInspection(
      synchronizerIdx.synchronizerId,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )

  override val reassignmentStore =
    new InMemoryReassignmentStore(Target(synchronizerIdx.item), loggerFactory)

  override val pendingOnboardingClearanceStore: PendingOnboardingClearanceStore =
    new InMemoryPendingOperationStore(OnboardingClearanceOperation, loggerFactory)

  override val partyReplicationIndexingStoreIfOnPREnabled
      : Option[InMemoryPartyReplicationIndexingStore] =
    parameters.alphaOnlinePartyReplicationSupport.map(cfg =>
      new InMemoryPartyReplicationIndexingStore(
        cfg.pauseSynchronizerIndexingDuringPartyReplication,
        loggerFactory,
      )
    )

  override def close(): Unit =
    LifeCycle.close(acsDigestStore)(logger)
}

class InMemoryPhysicalSyncPersistentState(
    crypto: SynchronizerCrypto,
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    val predecessor: Option[SynchronizerPredecessor],
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends PhysicalSyncPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  override val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  override val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  override val connectivityStatusStore = new InMemorySynchronizerConnectivityStatusStore()
  override val sendTrackerStore = new InMemorySendTrackerStore()
  override val submissionTrackerStore =
    new InMemorySubmissionTrackerStore(psid, loggerFactory, timeouts)

  override val topologyStore =
    new InMemoryTopologyStore(
      SynchronizerStore(psid),
      predecessor = predecessor,
      staticSynchronizerParameters.protocolVersion,
      loggerFactory,
      timeouts,
    )

  override def isMemory: Boolean = true

  override def close(): Unit = ()

  override protected def doInitialize()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit
}
