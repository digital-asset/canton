// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  ProcessingTimeout,
  TopologyConfig,
}
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.CacheMetrics
import com.digitalasset.canton.participant.admin.party.OnboardingClearanceScheduler
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.protocol.ParticipantTopologyTerminateProcessing
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.LsuCallback
import com.digitalasset.canton.participant.topology.client.MissingKeysAlerter
import com.digitalasset.canton.store.SequencedEventStore
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.cache.TopologyStateWriteThroughCache
import com.digitalasset.canton.topology.client.*
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  InitialTopologySnapshotValidator,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  NoPackageDependencies,
  PackageDependencyResolver,
  TopologyStore,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerOutboxQueue,
  SynchronizerTopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

class TopologyComponentFactory(
    psid: PhysicalSynchronizerId,
    crypto: SynchronizerCrypto,
    clock: Clock,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    caching: CachingConfigs,
    batching: BatchingConfig,
    topology: TopologyConfig,
    participantId: ParticipantId,
    alphaOnlinePartyReplicationSupport: Option[AlphaOnlinePartyReplicationConfig],
    exitOnFatalFailures: Boolean,
    topologyStore: TopologyStore[SynchronizerStore],
    topologyCacheMetrics: CacheMetrics,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext) {

  private val topologyStateCache = new TopologyStateWriteThroughCache(
    topologyStore,
    batching.topologyCacheAggregator,
    cacheEvictionThreshold = topology.topologyStateCacheEvictionThreshold,
    maxCacheSize = topology.maxTopologyStateCacheItems,
    enableConsistencyChecks = topology.enableTopologyStateCacheConsistencyChecks,
    topologyCacheMetrics,
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  def createTopologyProcessorFactory(
      missingKeysAlerter: MissingKeysAlerter,
      sequencerConnectionSuccessorListener: SequencerConnectionSuccessorListener,
      onboardingClearanceScheduler: OnboardingClearanceScheduler,
      topologyClient: SynchronizerTopologyClientWithInit,
      recordOrderPublisher: RecordOrderPublisher,
      lsuCallback: LsuCallback,
      retrieveAndStoreMissingSequencerIds: TraceContext => EitherT[
        FutureUnlessShutdown,
        String,
        Unit,
      ],
      sequencedEventStore: SequencedEventStore,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      ledgerApiStore: LedgerApiStore,
  ): TopologyTransactionProcessor.Factory = new TopologyTransactionProcessor.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[EffectiveTime] => Unit
    )(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[TopologyTransactionProcessor] = {

      val participantTerminateProcessing = new ParticipantTopologyTerminateProcessing(
        recordOrderPublisher,
        topologyStore,
        initialRecordTime = recordOrderPublisher.initTimestamp,
        participantId,
        pauseSynchronizerIndexingDuringPartyReplication =
          alphaOnlinePartyReplicationSupport.nonEmpty,
        synchronizerPredecessor = synchronizerPredecessor,
        lsuCallback = lsuCallback,
        retrieveAndStoreMissingSequencerIds = retrieveAndStoreMissingSequencerIds,
        loggerFactory,
      )
      val terminateTopologyProcessingFUS =
        for {
          topologyEventPublishedOnInitialRecordTime <- FutureUnlessShutdown.outcomeF(
            ledgerApiStore.topologyEventOffsetPublishedOnRecordTime(
              psid.logical,
              recordOrderPublisher.initTimestamp,
            )
          )
          _ <- participantTerminateProcessing.scheduleMissingTopologyEventsAtInitialization(
            topologyEventPublishedOnInitialRecordTime =
              topologyEventPublishedOnInitialRecordTime.isDefined,
            traceContextForSequencedEvent = sequencedEventStore.traceContext(_),
            parallelism = batching.parallelism,
          )
        } yield participantTerminateProcessing

      terminateTopologyProcessingFUS.map { terminateTopologyProcessing =>
        val processor = new TopologyTransactionProcessor(
          crypto.pureCrypto,
          topologyStore,
          topologyStateCache,
          crypto.staticSynchronizerParameters,
          acsCommitmentScheduleEffectiveTime,
          terminateTopologyProcessing,
          futureSupervisor,
          exitOnFatalFailures = exitOnFatalFailures,
          timeouts,
          loggerFactory,
        )
        processor.subscribe(missingKeysAlerter.attachToTopologyProcessor())
        processor.subscribe(sequencerConnectionSuccessorListener)
        processor.subscribe(onboardingClearanceScheduler)
        processor.subscribe(topologyClient)
        processor
      }
    }
  }

  def createTopologyManager(
      participantId: ParticipantId,
      syncPersistentState: SyncPersistentState,
      ledgerApiStore: Eval[LedgerApiStore],
      packageMetadataView: PackageMetadataView,
      crypto: SynchronizerCrypto,
      synchronizerLoggerFactory: NamedLoggerFactory,
      disableOptionalTopologyChecks: Boolean,
      dispatchQueueBackpressureLimit: NonNegativeInt,
      disableUpgradeValidation: Boolean,
  ): SynchronizerTopologyManager = {
    val synchronizerOutboxQueue = new SynchronizerOutboxQueue(loggerFactory)
    val topologyManager: SynchronizerTopologyManager = new SynchronizerTopologyManager(
      participantId.uid,
      clock = clock,
      crypto = crypto,
      staticSynchronizerParameters = crypto.staticSynchronizerParameters,
      topologyCacheAggregatorConfig = batching.topologyCacheAggregator,
      topologyConfig = topology,
      store = syncPersistentState.topologyStore,
      outboxQueue = synchronizerOutboxQueue,
      disableOptionalTopologyChecks = disableOptionalTopologyChecks,
      dispatchQueueBackpressureLimit = dispatchQueueBackpressureLimit,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts = timeouts,
      futureSupervisor = futureSupervisor,
      loggerFactory = synchronizerLoggerFactory,
    ) with ParticipantTopologyValidation {

      // override, so that the logger used is SynchronizerTopologyManager, and not SyncEphemeralStateFactoryImpl$$anon$1
      override protected def classForLogger: Class[?] = classOf[SynchronizerTopologyManager]

      override def validatePackageVetting(
          currentlyVettedPackages: Set[LfPackageId],
          nextPackageIds: Set[LfPackageId],
          dryRunSnapshot: Option[PackageMetadata],
          forceFlags: ForceFlags,
      )(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, TopologyManagerError, Unit] =
        validatePackageVetting(
          currentlyVettedPackages,
          nextPackageIds,
          packageMetadataView,
          dryRunSnapshot,
          forceFlags,
          disableUpgradeValidation,
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
          acsInspections = () => Map(syncPersistentState.lsid -> syncPersistentState.acsInspection),
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
          () => Map(syncPersistentState.lsid -> syncPersistentState.reassignmentStore),
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
          acsInspections = () => Map(syncPersistentState.lsid -> syncPersistentState.acsInspection),
        )
    }
    topologyManager
  }

  def createInitialTopologySnapshotValidator()(implicit
      executionContext: ExecutionContext,
      materializer: Materializer,
  ): InitialTopologySnapshotValidator =
    new InitialTopologySnapshotValidator(
      crypto.pureCrypto,
      topologyStore,
      batching.topologyCacheAggregator,
      topology,
      Some(crypto.staticSynchronizerParameters),
      timeouts,
      loggerFactory,
    )

  def createTopologyClient(
      packageDependencyResolver: PackageDependencyResolver,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    WriteThroughCacheSynchronizerTopologyClient.create(
      clock,
      crypto.staticSynchronizerParameters,
      topologyStore,
      topologyStateCache,
      synchronizerUpgradeTime = synchronizerPredecessor.map(_.upgradeTime),
      packageDependencyResolver,
      caching,
      topology,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )()
  def createTopologySnapshot(
      asOf: CantonTimestamp,
      packageDependencyResolver: PackageDependencyResolver,
      preferCaching: Boolean,
  )(implicit executionContext: ExecutionContext): TopologySnapshot = {
    val snapshot = new StoreBasedTopologySnapshot(
      psid,
      asOf,
      topologyStore,
      packageDependencyResolver,
      loggerFactory,
    )
    if (preferCaching) {
      new WriteThroughCacheTopologySnapshot(
        psid,
        topologyStateCache,
        topologyStore,
        packageDependencyResolver,
        asOf,
        loggerFactory,
      )
    } else
      snapshot
  }

  def createHeadTopologySnapshot()(implicit
      executionContext: ExecutionContext
  ): TopologySnapshot =
    createTopologySnapshot(CantonTimestamp.MaxValue, NoPackageDependencies, preferCaching = false)
}
