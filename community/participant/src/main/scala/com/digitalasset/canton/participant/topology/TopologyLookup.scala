// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TopologyManagerError.InvalidQueryTime
import com.digitalasset.canton.topology.admin.grpc.PsidLookup
import com.digitalasset.canton.topology.client.{
  StoreBasedSynchronizerTopologyClient,
  SynchronizerTopologyClient,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{NoPackageDependencies, TopologyStore}
import com.digitalasset.canton.topology.{
  PhysicalSynchronizerId,
  Synchronizer,
  SynchronizerId,
  SynchronizerTopologyManager,
  TopologyManagerError,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.ExecutionContext

/** @param topologyManagerO
  *   Get the topology manager for the given psid. Returns None if the psid is unknown or if the
  *   node is not connected to the synchronizer.
  * @param topologyClientO
  *   Get the topology client for the given psid. Returns None if the psid is unknown or if the node
  *   is not connected to the synchronizer.
  * @param psidLookup
  *   Retrieve the only active psid for a given lsid. Returns None if the lsid is unknown or if
  *   there is no active synchronizer.
  * @param syncPersistentStateO
  *   Retrieve the persistent state for the given psid. Returns None if the psid is unknown
  */
final class TopologyLookup(
    clock: Clock,
    topologyConfig: TopologyConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    private val topologyManagerO: PhysicalSynchronizerId => Option[SynchronizerTopologyManager],
    private val topologyClientO: PhysicalSynchronizerId => Option[SynchronizerTopologyClient],
    private val psidLookup: PsidLookup,
    private val syncPersistentStateO: PhysicalSynchronizerId => Option[SyncPersistentState],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Waits until a snapshot is available or construct one if the node is not connected to the
    * synchronizer
    */
  def maybeOfflineAwaitTopologySnapshot(synchronizerId: SynchronizerId, ts: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, TopologySnapshot] =
    for {
      client <- maybeOfflineTopologyClient(synchronizerId)
      approximateTs = client.approximateTimestamp
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        ts <= approximateTs,
        ParticipantTopologyManagerError.IdentityManagerParentError(
          InvalidQueryTime.Reject(
            synchronizerId,
            topologyKnownUntil = approximateTs,
            queryTime = ts,
          )
        ),
      )

      snapshot <- EitherT.liftF(client.awaitSnapshot(ts))
    } yield snapshot

  /** Returns the approximate timestamp for the given synchronizer. If the node is not connected to
    * the synchronizer, uses the known topology.
    */
  def maybeOfflineApproximateTimestamp(
      synchronizer: Synchronizer
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, CantonTimestamp] = for {
    psid <- synchronizer match {
      case lsid: SynchronizerId => activePsidFor(lsid).toEitherT[FutureUnlessShutdown]
      case psid: PhysicalSynchronizerId =>
        EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](psid)
    }

    snapshot <- topologyClientFor(psid).biflatMap(
      _ => offlineTopologyClient(psid).map(_.approximateTimestamp).toEitherT[FutureUnlessShutdown],
      topologyClient =>
        EitherT.rightT[FutureUnlessShutdown, ParticipantTopologyManagerError](
          topologyClient.approximateTimestamp
        ),
    )
  } yield snapshot

  /** Returns the approximate snapshot for the given synchronizer. If the node is not connected to
    * the synchronizer, uses the known topology.
    */
  def maybeOfflineApproximateSnapshot(
      synchronizer: Synchronizer
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, TopologySnapshot] =
    for {
      client <- maybeOfflineTopologyClient(synchronizer)
      snapshot <- EitherT.liftF(client.currentSnapshotApproximation)
    } yield snapshot

  private def offlineTopologyClient(
      psid: PhysicalSynchronizerId
  )(implicit
      traceContext: TraceContext
  ): Either[ParticipantTopologyManagerError, SynchronizerTopologyClient] =
    syncPersistentStateFor(psid).map { syncPersistentState =>
      new StoreBasedSynchronizerTopologyClient(
        clock = clock,
        staticSynchronizerParameters = syncPersistentState.staticSynchronizerParameters,
        store = syncPersistentState.topologyStore,
        packageDependencyResolver = NoPackageDependencies,
        topologyConfig = topologyConfig,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
        loggerFactory = loggerFactory,
      )
    }

  private def maybeOfflineTopologyClient(synchronizer: Synchronizer)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, SynchronizerTopologyClient] =
    for {
      psid <- synchronizer match {
        case lsid: SynchronizerId => activePsidFor(lsid).toEitherT[FutureUnlessShutdown]
        case psid: PhysicalSynchronizerId =>
          EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](psid)
      }
      client <- topologyClientFor(psid).biflatMap(
        _ => offlineTopologyClient(psid).toEitherT[FutureUnlessShutdown],
        topologyClient =>
          EitherT.pure[FutureUnlessShutdown, ParticipantTopologyManagerError](topologyClient),
      )
    } yield client

  /** Retrieves the topology store for a given synchronizer.
    * @param synchronizer
    *   If logical, the corresponding active physical synchronizer id is used.
    */
  def topologyStore(
      synchronizer: Synchronizer
  )(implicit
      traceContext: TraceContext
  ): Either[ParticipantTopologyManagerError, TopologyStore[SynchronizerStore]] = for {
    psid <- synchronizer match {
      case lsid: SynchronizerId => activePsidFor(lsid)
      case psid: PhysicalSynchronizerId => Right(psid)
    }
    store <- syncPersistentStateFor(psid).map(_.topologyStore)
  } yield store

  /** Returns the topology manager for the given psid. Fails if the node is not connected to the
    * synchronizer.
    */
  def topologyManager(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    SynchronizerTopologyManager,
  ] =
    EitherT.fromOption[FutureUnlessShutdown](
      topologyManagerO(psid),
      ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
      ),
    )

  private def activePsidFor(lsid: SynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[ParticipantTopologyManagerError, PhysicalSynchronizerId] =
    psidLookup
      .activePsidFor(lsid)
      .toRight(
        ParticipantTopologyManagerError.IdentityManagerParentError(
          TopologyManagerError.TopologyStoreUnknown.NoActiveSynchronizer(lsid)
        )
      )

  private def syncPersistentStateFor(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Either[ParticipantTopologyManagerError.IdentityManagerParentError, SyncPersistentState] =
    syncPersistentStateO(psid).toRight(
      ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
      )
    )

  /** Returns the topology manager for the given psid. Fails if the node is not connected to the
    * synchronizer.
    */
  private def topologyClientFor(psid: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    ParticipantTopologyManagerError,
    SynchronizerTopologyClient,
  ] =
    EitherT.fromOption[FutureUnlessShutdown](
      topologyClientO(psid),
      ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
      ),
    )
}
