// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.InvalidQueryTime
import com.digitalasset.canton.topology.admin.grpc.PsidLookupAt
import com.digitalasset.canton.topology.client.{
  StoreBasedSynchronizerTopologyClient,
  SynchronizerTopologyClient,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{NoPackageDependencies, TopologyStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import scala.concurrent.ExecutionContext

/** Provides accept to topology in an offline fashion.
  *
  * Note that since it does not provide any caching, it should only be used for infrequent query.
  */
trait OfflineTopologyLookup {
  def offlineAwaitTopologySnapshot(synchronizerId: SynchronizerId, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot]
}

/** @param psidLookup
  *   Retrieve the only active psid for a given lsid. Returns None if the lsid is unknown or if
  *   there is no active synchronizer.
  * @param topologyStoreO
  *   Retrieve the topology store for the given psid. Returns None if the psid is unknown.
  * @param staticSynchronizerParametersO
  *   Retrieve the topology store for the given psid. Returns None if the psid is unknown.
  * @param cleanSynchronizerRecordTime
  *   Retrieve the latest clean record time of the synchronizer. Returns None if the psid is
  *   unknown. The fact, that this is a clean record time, means there must have been a sequenced
  *   time greater than or equal to the record time. As a consequence, the clean record time serves
  *   as the lower bound for sequenced time to initialize the topology client.
  */
final class OfflineTopologyLookupImpl(
    clock: Clock,
    topologyConfig: TopologyConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    private val psidLookup: PsidLookupAt,
    private val topologyStoreO: PhysicalSynchronizerId => Option[TopologyStore[SynchronizerStore]],
    private val staticSynchronizerParametersO: PhysicalSynchronizerId => Option[
      StaticSynchronizerParameters
    ],
    cleanSynchronizerRecordTime: SynchronizerId => Option[CantonTimestamp],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends OfflineTopologyLookup
    with NamedLogging {

  /** Returns a topology snapshot for the given synchronizer at the specified time.
    *
    * Returns an error if (lsid, ts) cannot be mapped to a physical synchronizer or if the topology
    * is not yet known up to ts.
    */
  override def offlineAwaitTopologySnapshot(lsid: SynchronizerId, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot] =
    for {
      psid <- EitherT.fromEither[FutureUnlessShutdown](
        psidLookup
          .activePsidAt(lsid, ts)
          .leftMap(err => TopologyManagerError.InternalError.Unexpected(err): TopologyManagerError)
      )
      snapshot <- withOfflineTopologyClient(psid) { client =>
        val topologyKnownUntil = client.topologyKnownUntilTimestamp
        for {
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            ts <= topologyKnownUntil,
            InvalidQueryTime.Reject(
              lsid,
              topologyKnownUntil = topologyKnownUntil,
              queryTime = ts,
            ),
          )
          snapshot <- EitherT.liftF(client.awaitSnapshot(ts))
        } yield snapshot
      }
    } yield snapshot

  /** If the node is not connected to the synchronizer, the client is built from the persisted
    * topology store and closed once `use` has completed.
    */
  private def withOfflineTopologyClient[A](psid: PhysicalSynchronizerId)(
      f: SynchronizerTopologyClient => EitherT[FutureUnlessShutdown, TopologyManagerError, A]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, A] =
    offlineTopologyClient(psid).flatMap(offlineClient =>
      f(offlineClient).thereafter(_ => LifeCycle.close(offlineClient)(logger))
    )

  private def offlineTopologyClient(
      psid: PhysicalSynchronizerId
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, SynchronizerTopologyClient] =
    for {
      topologyStore <- EitherT.fromEither[FutureUnlessShutdown](
        topologyStoreO(psid).toRight(
          TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
        )
      )

      staticSynchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
        staticSynchronizerParametersO(psid).toRight(
          TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(psid))
        )
      )

      client = new StoreBasedSynchronizerTopologyClient(
        clock = clock,
        staticSynchronizerParameters = staticSynchronizerParameters,
        store = topologyStore,
        packageDependencyResolver = NoPackageDependencies,
        topologyConfig = topologyConfig,
        timeouts = timeouts,
        futureSupervisor = futureSupervisor,
        loggerFactory = loggerFactory,
      )
      _ <- EitherT.liftF(
        client.updateKnownTimestampsDuringStartup(cleanSynchronizerRecordTime =
          cleanSynchronizerRecordTime(psid.logical)
        )
      )

    } yield client
}
