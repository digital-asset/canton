// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TopologyConfig
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.SynchronizerPredecessor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
  IndexedTopologyStoreId,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{MonadUtil, StampedLockWithHandle}
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.unused
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Read-only interface to the [[SyncPersistentStateManager]] */
trait SyncPersistentStateLookup {
  def getAll: Map[PhysicalSynchronizerId, SyncPersistentState]

  /** Return the [[com.digitalasset.canton.participant.store.LogicalSyncPersistentState]] for all
    * [[com.digitalasset.canton.topology.SynchronizerId]]
    */
  def getAllLogical: Map[SynchronizerId, LogicalSyncPersistentState]

  /** Return the latest [[com.digitalasset.canton.participant.store.SyncPersistentState]] (wrt to
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]) for each
    * [[com.digitalasset.canton.topology.SynchronizerId]]
    */
  def getAllLatest: Map[SynchronizerId, SyncPersistentState] =
    getAll.values.toSeq
      .groupBy1(_.psid.logical)
      .view
      .mapValues(_.maxBy1(_.psid))
      .toMap

  def latestKnownPsid(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId]
  def latestKnownProtocolVersion(synchronizerId: SynchronizerId): Option[ProtocolVersion]

  def topologyFactoryFor(psid: PhysicalSynchronizerId): Option[TopologyComponentFactory]

  def get(psid: PhysicalSynchronizerId): Option[SyncPersistentState]

  def connectionConfig(psid: PhysicalSynchronizerId): Option[StoredSynchronizerConnectionConfig]

  /** Return the latest [[com.digitalasset.canton.participant.store.SyncPersistentState]] (wrt to
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]) for `synchronizerAlias`
    */
  def getLatest(synchronizerAlias: SynchronizerAlias): Option[SyncPersistentState] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(getAllLatest.get)

  def getAllFor(id: SynchronizerId): Seq[SyncPersistentState]

  def synchronizerIdsForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]]

  def allKnownLsids: Set[SynchronizerId] = getAll.keySet.map(_.logical)

  def synchronizerIdForAlias(synchronizerAlias: SynchronizerAlias): Option[SynchronizerId] =
    synchronizerIdsForAlias(synchronizerAlias).map(_.head1.logical)
  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]

  def acsInspection(synchronizerId: SynchronizerId): Option[AcsInspection]
  def acsInspection(synchronizerAlias: SynchronizerAlias): Option[AcsInspection] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(acsInspection)

  def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore]
  def reassignmentStore(synchronizerAlias: SynchronizerAlias): Option[ReassignmentStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(reassignmentStore)

  def acsCommitmentStore(synchronizerId: SynchronizerId): Option[AcsCommitmentStore]
  def acsCommitmentStore(synchronizerAlias: SynchronizerAlias): Option[AcsCommitmentStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(acsCommitmentStore)

  def activeContractStore(synchronizerId: SynchronizerId): Option[ActiveContractStore]
  def activeContractStore(synchronizerAlias: SynchronizerAlias): Option[ActiveContractStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(activeContractStore)
}

/** Manages participant-relevant state for a synchronizer that needs to survive reconnects
  *
  * Factory for [[com.digitalasset.canton.participant.store.SyncPersistentState]]. Tries to discover
  * existing persistent states or create new ones and checks consistency of synchronizer parameters
  * and unique contract key synchronizers
  */
class SyncPersistentStateManager(
    participantId: ParticipantId,
    aliasResolution: SynchronizerAliasResolution,
    val storage: Storage,
    val indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    parameters: ParticipantNodeParameters,
    topologyConfig: TopologyConfig,
    val synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    synchronizerCryptoFactory: StaticSynchronizerParameters => SynchronizerCrypto,
    clock: Clock,
    ledgerApiStore: Eval[LedgerApiStore],
    val contractStore: Eval[ContractStore],
    participantMetrics: ParticipantMetrics,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncPersistentStateLookup
    with StaticSynchronizerParametersGetter
    with AutoCloseable
    with NamedLogging {

  private val lock = new StampedLockWithHandle()

  /** Creates [[com.digitalasset.canton.participant.store.SyncPersistentState]]s for all known
    * synchronizer aliases provided that the synchronizer parameters and a sequencer offset are
    * known. Does not check for unique contract key synchronizer constraints. Must not be called
    * concurrently with itself or other methods of this class.
    */
  def initializePersistentStates()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = lock.withWriteLockHandle { implicit lockHandle =>
    def getStaticSynchronizerParameters(synchronizerId: PhysicalSynchronizerId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, StaticSynchronizerParameters] =
      EitherT
        .fromOptionF(
          SynchronizerConnectivityStatusStore(
            storage,
            synchronizerId,
            parameters.processingTimeouts,
            loggerFactory,
          ).lastParameters,
          "No synchronizer parameters in store",
        )

    def initializePhysicalPersistentState(
        psid: PhysicalSynchronizerId
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      for {

        psidIndexed <- EitherT.right(
          IndexedPhysicalSynchronizer.indexed(indexedStringStore)(psid)
        )
        indexedTopologyStoreId <- EitherT.right(
          IndexedTopologyStoreId.indexed(indexedStringStore)(
            TopologyStoreId.SynchronizerStore(psid)
          )
        )
        staticSynchronizerParameters <- getStaticSynchronizerParameters(psid)
        storedSynchronizerConnectionConfig <- EitherT
          .fromEither[FutureUnlessShutdown](synchronizerConnectionConfigStore.get(psid))
          .leftMap(_.toString)
        persistentState = createPhysicalPersistentState(
          psidIndexed,
          indexedTopologyStoreId,
          staticSynchronizerParameters,
          storedSynchronizerConnectionConfig.predecessor,
        )
        _ = logger.debug(s"Discovered existing state for $psid")
      } yield {
        val synchronizerId = persistentState.psid

        val previous = physicalPersistentStates.putIfAbsent(synchronizerId, persistentState)
        if (previous.isDefined)
          throw new IllegalArgumentException(
            s"synchronizer state already exists for $synchronizerId"
          )
      }

    MonadUtil.sequentialTraverse_(aliasResolution.logicalSynchronizerIds) { lsid =>
      for {
        // first create the logical store
        synchronizerIdIndexed <- IndexedSynchronizer.indexed(indexedStringStore)(lsid)
        _ = logicalPersistentStates
          .getOrElseUpdate(lsid, createLogicalPersistentState(synchronizerIdIndexed))
        // then create all corresponding physical stores
        _ <- MonadUtil.sequentialTraverse_(aliasResolution.physicalSynchronizerIds(lsid)) { psid =>
          initializePhysicalPersistentState(psid)
            .valueOr(error => logger.debug(s"No state for $psid discovered: $error"))
        }
      } yield ()
    }
  }

  private def getSynchronizerIdx(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[IndexedSynchronizer] =
    IndexedSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

  private def getPhysicalSynchronizerIdx(
      synchronizerId: PhysicalSynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[IndexedPhysicalSynchronizer] =
    IndexedPhysicalSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

  private def getSynchronizerTopologyStoreId(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[IndexedTopologyStoreId] =
    IndexedTopologyStoreId.indexed(indexedStringStore)(
      TopologyStoreId.SynchronizerStore(synchronizerId)
    )

  /** Retrieves the [[com.digitalasset.canton.participant.store.SyncPersistentState]] from the
    * [[com.digitalasset.canton.participant.sync.SyncPersistentStateManager]] for the given
    * synchronizer if there is one. Otherwise creates a new
    * [[com.digitalasset.canton.participant.store.SyncPersistentState]] for the synchronizer and
    * registers it with the [[com.digitalasset.canton.participant.sync.SyncPersistentStateManager]].
    * Checks that the [[com.digitalasset.canton.protocol.StaticSynchronizerParameters]] are the same
    * as what has been persisted (if so) and enforces the unique contract key synchronizer
    * constraints.
    *
    * Must not be called concurrently with itself or other methods of this class.
    */
  def lookupOrCreatePersistentState(
      psid: PhysicalSynchronizerId,
      synchronizerParameters: StaticSynchronizerParameters,
      predecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SyncPersistentState] =
    lock.withWriteLockHandle { implicit writeLockHandle =>
      for {
        physicalSynchronizerIdx <- EitherT.right(getPhysicalSynchronizerIdx(psid))
        indexedTopologyStoreId <- EitherT.right(getSynchronizerTopologyStoreId(psid))
        synchronizerIdx <- EitherT.right(getSynchronizerIdx(psid.logical))
        logical =
          logicalPersistentStates.getOrElse(
            synchronizerIdx.synchronizerId,
            createLogicalPersistentState(synchronizerIdx),
          )
        physical = {
          physicalPersistentStates.getOrElse(
            physicalSynchronizerIdx.psid,
            createPhysicalPersistentState(
              physicalSynchronizerIdx,
              indexedTopologyStoreId,
              synchronizerParameters,
              predecessor,
            ),
          )
        }

        _ <- checkAndUpdateSynchronizerParameters(
          psid,
          physical.connectivityStatusStore,
          synchronizerParameters,
        )
        _ <- checkPredecessor(psid, physical.topologyStore.predecessor, predecessor)
      } yield {
        // only put the logical store into the map if we really also have a physical store.
        logicalPersistentStates
          .putIfAbsent(synchronizerIdx.synchronizerId, logical)
          // since we have the write lock, either `physical` already came from the Map,
          // and therefore setting it again is a noop,
          // or it was absent, and therefore putIfAbsent will set it.
          .discard
        physicalPersistentStates
          .putIfAbsent(physical.psid, physical)
          // since we have the write lock, either `physical` already came from the Map,
          // and therefore setting it again is a noop,
          // or it was absent, and therefore putIfAbsent will set it.
          .discard
        new SyncPersistentState(logical, physical, psidLoggerFactory(physical.psid))
      }
    }

  private def createLogicalPersistentState(
      synchronizerIdx: IndexedSynchronizer
  )(implicit writeLockHandle: lock.WriteLockHandle): LogicalSyncPersistentState =
    mkLogicalPersistentState(synchronizerIdx)

  private def createPhysicalPersistentState(
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      indexedTopologyStoreId: IndexedTopologyStoreId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      predecessor: Option[SynchronizerPredecessor],
  )(implicit writeLockHandle: lock.WriteLockHandle): PhysicalSyncPersistentState =
    mkPhysicalPersistentState(
      physicalSynchronizerIdx,
      indexedTopologyStoreId,
      staticSynchronizerParameters,
      predecessor,
    )

  private def checkAndUpdateSynchronizerParameters(
      psid: PhysicalSynchronizerId,
      connectivityStatusStore: SynchronizerConnectivityStatusStore,
      newParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext,
      @unused writeLockHandle: lock.WriteLockHandle,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    for {
      oldParametersO <- EitherT.right(connectivityStatusStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing synchronizer parameters for synchronizer $psid: $newParameters")
          EitherT.right[SynchronizerRegistryError](
            connectivityStatusStore.setParameters(newParameters)
          )
        case Some(oldParameters) =>
          EitherT.cond[FutureUnlessShutdown](
            oldParameters == newParameters,
            (),
            SynchronizerRegistryError.ConfigurationErrors.SynchronizerParametersChanged
              .Error(oldParametersO, newParameters): SynchronizerRegistryError,
          )
      }
    } yield ()

  private def checkPredecessor(
      psid: PhysicalSynchronizerId,
      existingPredecessor: Option[SynchronizerPredecessor],
      newPredecessor: Option[SynchronizerPredecessor],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    // Only flag when the caller asserts a predecessor that conflicts with the one the topology
    // store was initialized with. A `None` from the caller just means "don't know", not "no predecessor".
    EitherT.cond[FutureUnlessShutdown](
      newPredecessor.forall(existingPredecessor.contains),
      (),
      SynchronizerRegistryError.ConfigurationErrors.SynchronizerPredecessorMismatch
        .Error(psid, existingPredecessor, newPredecessor): SynchronizerRegistryError,
    )

  override def acsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
    logicalPersistentStates.get(synchronizerId).map(_.acsInspection)

  override def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore] =
    logicalPersistentStates.get(synchronizerId).map(_.reassignmentStore)

  override def acsCommitmentStore(synchronizerId: SynchronizerId): Option[AcsCommitmentStore] =
    logicalPersistentStates.get(synchronizerId).map(_.acsCommitmentStore)

  override def activeContractStore(synchronizerId: SynchronizerId): Option[ActiveContractStore] =
    logicalPersistentStates.get(synchronizerId).map(_.activeContractStore)

  override def staticSynchronizerParameters(
      synchronizerId: PhysicalSynchronizerId
  ): Option[StaticSynchronizerParameters] =
    get(synchronizerId).map(_.staticSynchronizerParameters)

  override def latestKnownPsid(
      synchronizerId: SynchronizerId
  ): Option[PhysicalSynchronizerId] =
    getAll.keySet.filter(_.logical == synchronizerId).maxOption

  private val logicalPersistentStates: concurrent.Map[SynchronizerId, LogicalSyncPersistentState] =
    TrieMap[SynchronizerId, LogicalSyncPersistentState]()

  private val physicalPersistentStates
      : concurrent.Map[PhysicalSynchronizerId, PhysicalSyncPersistentState] =
    TrieMap[PhysicalSynchronizerId, PhysicalSyncPersistentState]()

  override def get(psid: PhysicalSynchronizerId): Option[SyncPersistentState] =
    lock.withReadLock[Option[SyncPersistentState]](
      for {
        logical <- logicalPersistentStates.get(psid.logical)
        physical <- physicalPersistentStates.get(psid)
      } yield new SyncPersistentState(logical, physical, psidLoggerFactory(physical.psid))
    )

  override def connectionConfig(
      psid: PhysicalSynchronizerId
  ): Option[StoredSynchronizerConnectionConfig] =
    synchronizerConnectionConfigStore.get(psid).toOption

  override def getAll: Map[PhysicalSynchronizerId, SyncPersistentState] =
    lock.withReadLock[Map[PhysicalSynchronizerId, SyncPersistentState]] {
      val psidToState = for {
        lsid <- logicalPersistentStates.keys
        state <- getAllFor(lsid)
      } yield state.psid -> state
      psidToState.toMap
    }

  override def getAllLogical: Map[SynchronizerId, LogicalSyncPersistentState] =
    // just take a snapshot of the map
    logicalPersistentStates.toMap

  override def getAllFor(id: SynchronizerId): Seq[SyncPersistentState] =
    lock.withReadLock[Seq[SyncPersistentState]](
      for {
        logical <- logicalPersistentStates.get(id).toList
        physical <- physicalPersistentStates.values.filter(_.psid.logical == id).toSeq
      } yield new SyncPersistentState(logical, physical, psidLoggerFactory(physical.psid))
    )

  override def synchronizerIdForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[SynchronizerId] =
    aliasResolution.synchronizerIdForAlias(synchronizerAlias)

  override def synchronizerIdsForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]] =
    aliasResolution.synchronizerIdsForAlias(synchronizerAlias)

  override def aliasForSynchronizerId(synchronizerId: SynchronizerId): Option[SynchronizerAlias] =
    aliasResolution.aliasForSynchronizerId(synchronizerId)

  private def psidLoggerFactory(psid: PhysicalSynchronizerId) =
    loggerFactory.append("psid", psid.toString)
  private def lsidLoggerFactory(lsid: SynchronizerId) =
    loggerFactory.append("synchronizerId", lsid.toString)

  private def mkLogicalPersistentState(
      synchronizerIdx: IndexedSynchronizer
  )(implicit @unused writeLockHandle: lock.WriteLockHandle): LogicalSyncPersistentState =
    LogicalSyncPersistentState
      .create(
        storage,
        synchronizerIdx,
        parameters,
        indexedStringStore,
        acsCounterParticipantConfigStore,
        ledgerApiStore,
        contractStore,
        lsidLoggerFactory(synchronizerIdx.synchronizerId),
        futureSupervisor,
      )

  private def mkPhysicalPersistentState(
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      indexedTopologyStoreId: IndexedTopologyStoreId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      predecessor: Option[SynchronizerPredecessor],
  )(implicit @unused writeLockHandle: lock.WriteLockHandle): PhysicalSyncPersistentState =
    PhysicalSyncPersistentState
      .create(
        storage,
        physicalSynchronizerIdx,
        indexedTopologyStoreId,
        staticSynchronizerParameters,
        synchronizerCryptoFactory(staticSynchronizerParameters),
        parameters,
        predecessor,
        psidLoggerFactory(physicalSynchronizerIdx.psid),
        futureSupervisor,
      )

  override def topologyFactoryFor(
      psid: PhysicalSynchronizerId
  ): Option[TopologyComponentFactory] =
    get(psid).map(state =>
      new TopologyComponentFactory(
        psid,
        synchronizerCryptoFactory(state.staticSynchronizerParameters),
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.enableAdditionalConsistencyChecks,
        parameters.batchingConfig,
        topologyConfig,
        participantId,
        parameters.alphaOnlinePartyReplicationSupport,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        state.topologyStore,
        topologyCacheMetrics = participantMetrics.topologyCache,
        loggerFactory.append("psid", psid.toString),
      )
    )

  override def close(): Unit =
    LifeCycle.close((physicalPersistentStates.values.toSeq :+ aliasResolution)*)(logger)
}
