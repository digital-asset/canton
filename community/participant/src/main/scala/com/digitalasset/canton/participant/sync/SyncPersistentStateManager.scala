// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.environment.{
  StoreBasedSynchronizerTopologyInitializationCallback,
  SynchronizerTopologyInitializationCallback,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.StampedLockWithHandle
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.unused
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Read-only interface to the [[SyncPersistentStateManager]] */
trait SyncPersistentStateLookup {
  def getAll: Map[SynchronizerId, SyncPersistentState]

  /** Return the latest [[com.digitalasset.canton.participant.store.SyncPersistentState]] (wrt to
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]) for each
    * [[com.digitalasset.canton.topology.SynchronizerId]]
    */
  def getAllLatest: Map[SynchronizerId, SyncPersistentState] =
    getAll.values.toSeq
      .groupBy1(_.physicalSynchronizerId.logical)
      .view
      .mapValues(_.maxBy1(_.physicalSynchronizerId))
      .toMap

  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]

  def acsInspection(synchronizerAlias: SynchronizerAlias): Option[AcsInspection]

  def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore]
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
    storage: Storage,
    val indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    parameters: ParticipantNodeParameters,
    synchronizerCryptoFactory: StaticSynchronizerParameters => SynchronizerCrypto,
    clock: Clock,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val contractStore: Eval[ContractStore],
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncPersistentStateLookup
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
    def getStaticSynchronizerParameters(synchronizerId: SynchronizerId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, StaticSynchronizerParameters] =
      EitherT
        .fromOptionF(
          SynchronizerParameterStore(
            storage,
            synchronizerId,
            parameters.processingTimeouts,
            loggerFactory,
          ).lastParameters,
          "No synchronizer parameters in store",
        )

    aliasResolution.physicalSynchronizerIds.toList.parTraverse_ { psid =>
      val resultE = for {
        synchronizerIdIndexed <- EitherT.right(
          IndexedSynchronizer.indexed(indexedStringStore)(psid.logical)
        )
        staticSynchronizerParameters <- getStaticSynchronizerParameters(psid.logical)
        persistentState = createPersistentState(synchronizerIdIndexed, staticSynchronizerParameters)
        _ = logger.debug(s"Discovered existing state for $psid")
      } yield {
        val synchronizerId = persistentState.synchronizerIdx.synchronizerId
        val previous = persistentStates.putIfAbsent(synchronizerId, persistentState)
        if (previous.isDefined)
          throw new IllegalArgumentException(
            s"synchronizer state already exists for $synchronizerId"
          )
      }

      resultE.valueOr(error => logger.debug(s"No state for $psid discovered: $error"))
    }
  }

  def getSynchronizerIdx(
      synchronizerId: SynchronizerId
  ): FutureUnlessShutdown[IndexedSynchronizer] =
    IndexedSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

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
      synchronizerAlias: SynchronizerAlias,
      indexedSynchronizer: IndexedSynchronizer,
      synchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SyncPersistentState] =
    lock.withWriteLockHandle { implicit writeLockHandle =>
      val persistentState = createPersistentState(indexedSynchronizer, synchronizerParameters)
      for {
        _ <- checkAndUpdateSynchronizerParameters(
          synchronizerAlias,
          persistentState.parameterStore,
          synchronizerParameters,
        )
      } yield {
        persistentStates
          .putIfAbsent(persistentState.synchronizerIdx.synchronizerId, persistentState)
          .getOrElse(persistentState)
      }
    }

  private def createPersistentState(
      indexedSynchronizer: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit writeLockHandle: lock.WriteLockHandle): SyncPersistentState =
    persistentStates.getOrElse(
      indexedSynchronizer.synchronizerId,
      mkPersistentState(indexedSynchronizer, staticSynchronizerParameters),
    )

  private def checkAndUpdateSynchronizerParameters(
      alias: SynchronizerAlias,
      parameterStore: SynchronizerParameterStore,
      newParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext,
      @unused writeLockHandle: lock.WriteLockHandle,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    for {
      oldParametersO <- EitherT.right(parameterStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing synchronizer parameters for synchronizer $alias: $newParameters")
          EitherT.right[SynchronizerRegistryError](parameterStore.setParameters(newParameters))
        case Some(oldParameters) =>
          EitherT.cond[FutureUnlessShutdown](
            oldParameters == newParameters,
            (),
            SynchronizerRegistryError.ConfigurationErrors.SynchronizerParametersChanged
              .Error(oldParametersO, newParameters): SynchronizerRegistryError,
          )
      }
    } yield ()

  def acsInspection(synchronizerAlias: SynchronizerAlias): Option[AcsInspection] =
    for {
      // Taking any of the PSIds is fine since ACS is logical
      psid <- synchronizerIdsForAlias(synchronizerAlias).map(_.head1)
      res <- get(psid)
    } yield res.acsInspection

  override def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore] =
    for {
      // Taking any of the PSIds is fine since ACS is logical
      psid <- aliasResolution.physicalSynchronizerIds(synchronizerId).headOption
      res <- get(psid)
    } yield res.reassignmentStore

  // TODO(#25483): This should be per PSId
  def staticSynchronizerParameters(
      synchronizerId: SynchronizerId
  ): Option[StaticSynchronizerParameters] =
    get(synchronizerId).map(_.staticSynchronizerParameters)

  def protocolVersionFor(synchronizerId: SynchronizerId): Option[ProtocolVersion] =
    staticSynchronizerParameters(synchronizerId).map(_.protocolVersion)

  private val persistentStates: concurrent.Map[SynchronizerId, SyncPersistentState] =
    TrieMap[SynchronizerId, SyncPersistentState]()

  // TODO(#25483) This should be physical
  def get(synchronizerId: SynchronizerId): Option[SyncPersistentState] =
    lock.withReadLock[Option[SyncPersistentState]](persistentStates.get(synchronizerId))

  // TODO(#25483) This should be physical
  def get(synchronizerId: PhysicalSynchronizerId): Option[SyncPersistentState] =
    lock.withReadLock[Option[SyncPersistentState]](persistentStates.collectFirst {
      case (id, state) if id == synchronizerId.logical => state
    })

  override def getAll: Map[SynchronizerId, SyncPersistentState] =
    // no lock needed here. just return the current snapshot
    persistentStates.toMap

  def getByAlias(synchronizerAlias: SynchronizerAlias): Option[SyncPersistentState] =
    for {
      synchronizerId <- synchronizerIdForAlias(synchronizerAlias)
      res <- get(synchronizerId)
    } yield res

  def synchronizerIdForAlias(synchronizerAlias: SynchronizerAlias): Option[SynchronizerId] =
    aliasResolution.synchronizerIdForAlias(synchronizerAlias)
  def aliasForSynchronizerId(synchronizerId: SynchronizerId): Option[SynchronizerAlias] =
    aliasResolution.aliasForSynchronizerId(synchronizerId)

  def synchronizerIdsForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]] =
    aliasResolution.synchronizerIdsForAlias(synchronizerAlias)

  private def mkPersistentState(
      indexedSynchronizer: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit @unused writeLockHandle: lock.WriteLockHandle): SyncPersistentState =
    SyncPersistentState
      .create(
        participantId,
        storage,
        indexedSynchronizer,
        staticSynchronizerParameters,
        clock,
        synchronizerCryptoFactory(staticSynchronizerParameters),
        parameters,
        indexedStringStore,
        acsCounterParticipantConfigStore,
        packageDependencyResolver,
        ledgerApiStore,
        contractStore,
        loggerFactory,
        futureSupervisor,
      )

  def topologyFactoryFor(
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
  ): Option[TopologyComponentFactory] =
    get(synchronizerId).map(state =>
      new TopologyComponentFactory(
        synchronizerId,
        protocolVersion,
        synchronizerCryptoFactory(state.staticSynchronizerParameters),
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        participantId,
        parameters.unsafeOnlinePartyReplication,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        state.topologyStore,
        loggerFactory.append("synchronizerId", synchronizerId.toString),
      )
    )

  def synchronizerTopologyStateInitFor(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Option[
    SynchronizerTopologyInitializationCallback
  ]] =
    get(synchronizerId) match {
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Option[SynchronizerTopologyInitializationCallback]](
          SynchronizerRegistryError.SynchronizerRegistryInternalError.InvalidState(
            s"topology factory for synchronizer $synchronizerId is unavailable"
          )
        )

      case Some(state) =>
        EitherT.right(
          state.topologyStore
            .findFirstTrustCertificateForParticipant(participantId)
            .map(trustCert =>
              // only if the participant's trustCert is not yet in the topology store do we have to initialize it.
              // The callback will fetch the essential topology state from the sequencer
              Option.when(trustCert.isEmpty)(
                new StoreBasedSynchronizerTopologyInitializationCallback(
                  participantId
                )
              )
            )
        )

    }

  override def close(): Unit =
    LifeCycle.close(persistentStates.values.toSeq :+ aliasResolution: _*)(logger)
}
