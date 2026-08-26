// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.{
  CachingTopologySnapshot,
  StoreBasedTopologySnapshot,
  SynchronizerTopologyClient,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.NoPackageDependencies
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

/** This interface encapsulates the logic for topology access for digest processors.
  */
trait DigestProcessorTopologyLookup {

  /** Returns a synchronizer topology client for the synchronizer at the given `timestamp`. Tries to
    * reuse the provided `previousTopologyClientO`, if the participant is currently not connected to
    * the synchronizer, and the cached topology client is suitable to serve the topology at the
    * given `timestamp`. If there is no "live" topology client, and the `previousTopologyClientO`
    * can't be reused, a new offline topology client will be created.
    *
    * The [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] may pass the
    * [[com.digitalasset.canton.topology.client.SynchronizerTopologyClient]] from a previous
    * invocation to this method, to improve cache utilization by possibly reusing an offline
    * topology client.
    *
    * The provided `previousTopologyClientO` can be used again, if:
    *   1. the requested timestamp can be served by the known topology state of the cached topology
    *      client
    *   1. the active physical synchronizer at the timestamp is not a successor of the synchronizer
    *      of the cached topology client
    */
  def topologyClientForRunningDigestProcessor(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
      previousTopologyClientO: Option[SynchronizerTopologyClient],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SynchronizerTopologyClient]

  /** Creates a topology snapshot for the given `synchronizerId` at the given `timestamp`. Since the
    * ACS digest reinitialization uses the same topology state for the entire reinitialization
    * process, the returned snapshot
    *   1. has a caching behavior, to avoid reading topology state repeatedly from the database, and
    *   1. does <strong>not</strong> use the live topology state cache, to avoid cache thrashing
    */
  def topologySnapshotForReinitialization(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Option[TopologySnapshot]
}

class DigestProcessorTopologyLookupImpl(
    ledgerApiStore: LedgerApiStore,
    sync: CantonSyncService,
    cachingConfigs: CachingConfigs,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends DigestProcessorTopologyLookup
    with NamedLogging {

  def topologyClientForRunningDigestProcessor(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
      previousTopologyClientO: Option[SynchronizerTopologyClient],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SynchronizerTopologyClient] = {
    val activePsidAtTimestamp = sync.activePsidLookup
      .activePsidAt(synchronizerId, timestamp)
      .valueOr(ErrorUtil.invalidState(_))

    // try to get a topology client for a connected synchronizer.
    val connectedClient =
      sync.lookupTopologyClient(activePsidAtTimestamp)

    @inline def reuseCachedClient = previousTopologyClientO.filter(cached =>
      timestamp <= cached.topologyKnownUntilTimestamp && activePsidAtTimestamp <= cached.psid
    )

    /* Creates an "offline" topology client that can possibly serve the topology state up until the
     * current synchronizer's ledger end. See the comment around `createTopologyClient` below, for
     * a more nuanced discussion.
     */
    @inline def createOfflineCachedClient =
      ledgerApiStore.ledgerEnd
        .flatMap(
          _.synchronizerIndices.get(synchronizerId).map(_.recordTime)
        )
        .map { cleanSynchronizerRecordTime =>
          // creating a new topology factory also creates a new topology state cache that will only be used
          // for this offline topology client
          val topologyFactory = sync.syncPersistentStateManager
            .topologyFactoryFor(activePsidAtTimestamp)
            .getOrElse(
              ErrorUtil.invalidState(
                s"unable to find persistent state for active $activePsidAtTimestamp at $timestamp"
              )
            )
          topologyFactory
            .createTopologyClient(
              NoPackageDependencies,
              synchronizerPredecessor = None,
              // In an offline catch-up scenario that crosses an LSU upgrade time,
              // the `cleanSynchronizerRecordTime` could be after the upgrade time from `psid` to the successor of `psid`.
              // However, we check for every timestamp that the psid of the cached synchronizer
              // can actually service the topology for the active psid at the requested timestamp.
              // Overshooting here allows us to forgo an additional check for the upgrade time of an LsuAnnouncement.
              cleanSynchronizerRecordTime = Some(cleanSynchronizerRecordTime),
            )
        }

    /* the order between the first two is significiant in the scenario where the digest processor
      was in an offline catch-up mode and the participants reconnects to the synchronizer:
      1. `connectedClient` first: the digest processor will start using the
         connected topology client (and therefore the connected topology state cache) as soon as the
         participant connects to the synchronizer, even if the digest processor might still be far behind ledger end.
         This might cause some thrashing on the topology cache.

      2. `reuseCachedClient` first: that the digest processor will use the offline topology client with its
         own cache (separate from the connected topology state cache) until the record time of the synchronizer's
         ledger end when it started the offline catch-up.
     */
    connectedClient
      .orElse(reuseCachedClient)
      .map(FutureUnlessShutdown.pure)
      .orElse(createOfflineCachedClient)
      .getOrElse(
        ErrorUtil.invalidState(
          s"Unable to get topology snapshot for $synchronizerId at $timestamp"
        )
      )
  }

  def topologySnapshotForReinitialization(
      synchronizerId: SynchronizerId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Option[TopologySnapshot] = {
    val psid = sync.activePsidLookup
      .activePsidAt(synchronizerId, timestamp)
      .valueOr(ErrorUtil.invalidState(_))
    sync.syncPersistentStateManager.get(psid).map { state =>
      val loggerFactoryWithSynchronizer = loggerFactory.append("psid", psid.toString)
      val snapshot = new StoreBasedTopologySnapshot(
        psid,
        timestamp,
        state.topologyStore,
        NoPackageDependencies,
        loggerFactoryWithSynchronizer,
      )
      new CachingTopologySnapshot(
        snapshot,
        cachingConfigs,
        loggerFactoryWithSynchronizer,
        futureSupervisor,
      )
    }
  }
}
