// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{CryptoPureApi, SynchronizerCrypto}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.{
  AcsCounterParticipantConfigStore,
  AcsInspection,
  ContractStore,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.topology.ParticipantTopologyValidation
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target

import scala.concurrent.ExecutionContext

class InMemorySyncPersistentState(
    participantId: ParticipantId,
    clock: Clock,
    crypto: SynchronizerCrypto,
    override val physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
    override val synchronizerIdx: IndexedSynchronizer,
    val staticSynchronizerParameters: StaticSynchronizerParameters,
    override val enableAdditionalConsistencyChecks: Boolean,
    indexedStringStore: IndexedStringStore,
    contractStore: ContractStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    exitOnFatalFailures: Boolean,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    val futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends SyncPersistentState {

  override val pureCryptoApi: CryptoPureApi = crypto.pureCrypto

  val activeContractStore =
    new InMemoryActiveContractStore(
      indexedStringStore,
      loggerFactory,
    )
  val reassignmentStore =
    new InMemoryReassignmentStore(Target(synchronizerIdx.item), loggerFactory)

  val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory, timeouts)
  val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
  val acsCommitmentStore =
    new InMemoryAcsCommitmentStore(
      synchronizerIdx.synchronizerId,
      acsCounterParticipantConfigStore,
      loggerFactory,
    )
  val parameterStore = new InMemorySynchronizerParameterStore()
  val sendTrackerStore = new InMemorySendTrackerStore()
  val submissionTrackerStore = new InMemorySubmissionTrackerStore(loggerFactory)

  override val topologyStore =
    new InMemoryTopologyStore(
      SynchronizerStore(physicalSynchronizerIdx.synchronizerId),
      staticSynchronizerParameters.protocolVersion,
      loggerFactory,
      timeouts,
    )

  override val synchronizerOutboxQueue = new SynchronizerOutboxQueue(loggerFactory)
  override val topologyManager: SynchronizerTopologyManager = new SynchronizerTopologyManager(
    participantId.uid,
    clock,
    crypto,
    staticSynchronizerParameters,
    topologyStore,
    synchronizerOutboxQueue,
    exitOnFatalFailures = exitOnFatalFailures,
    timeouts,
    futureSupervisor,
    loggerFactory,
  ) with ParticipantTopologyValidation {

    override def validatePackageVetting(
        currentlyVettedPackages: Set[LfPackageId],
        nextPackageIds: Set[LfPackageId],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      validatePackageVetting(
        currentlyVettedPackages,
        nextPackageIds,
        packageDependencyResolver,
        acsInspections = () => Map(synchronizerIdx.synchronizerId -> acsInspection),
        forceFlags,
      )
    override def checkCannotDisablePartyWithActiveContracts(
        partyId: PartyId,
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkCannotDisablePartyWithActiveContracts(
        partyId,
        forceFlags,
        acsInspections = () => Map(synchronizerIdx.synchronizerId -> acsInspection),
      )

    override def checkInsufficientSignatoryAssigningParticipantsForParty(
        partyId: PartyId,
        currentThreshold: PositiveInt,
        nextThreshold: Option[PositiveInt],
        nextConfirmingParticipants: Seq[HostingParticipant],
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkInsufficientSignatoryAssigningParticipantsForParty(
        partyId,
        currentThreshold,
        nextThreshold,
        nextConfirmingParticipants,
        forceFlags,
        () => Map(synchronizerIdx.synchronizerId -> reassignmentStore),
        () => ledgerApiStore.value.ledgerEnd,
      )

    override def checkInsufficientParticipantPermissionForSignatoryParty(
        partyId: PartyId,
        forceFlags: ForceFlags,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
      checkInsufficientParticipantPermissionForSignatoryParty(
        partyId,
        forceFlags,
        acsInspections = () => Map(synchronizerIdx.synchronizerId -> acsInspection),
      )

  }

  override def isMemory: Boolean = true

  override def close(): Unit = ()

  override def acsInspection: AcsInspection =
    new AcsInspection(
      synchronizerIdx.synchronizerId,
      activeContractStore,
      contractStore,
      ledgerApiStore,
    )
}
