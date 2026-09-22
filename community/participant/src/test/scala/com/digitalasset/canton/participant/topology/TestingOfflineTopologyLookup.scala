// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{SynchronizerId, TopologyManagerError}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Serve a snapshot for timestamp `ts` if there is a corresponding entry in the map (regardless of
  * the synchronizer)
  */
final class TestingOfflineTopologyLookup(snapshots: Map[CantonTimestamp, TopologySnapshot])(implicit
    val executionContext: ExecutionContext
) extends OfflineTopologyLookup {
  override def offlineAwaitTopologySnapshot(synchronizerId: SynchronizerId, ts: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot] = snapshots
    .get(ts)
    .fold(
      EitherT.liftF[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot](
        FutureUnlessShutdown.failed(new RuntimeException("Failing lookup"))
      )
    )(EitherT.pure[FutureUnlessShutdown, TopologyManagerError](_))
}

final class FailingOfflineTopologyLookup()(implicit executionContext: ExecutionContext)
    extends OfflineTopologyLookup {
  override def offlineAwaitTopologySnapshot(synchronizerId: SynchronizerId, ts: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot] =
    EitherT.liftF[FutureUnlessShutdown, TopologyManagerError, TopologySnapshot](
      FutureUnlessShutdown.failed(new RuntimeException("Failing lookup"))
    )
}
