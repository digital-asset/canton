// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbSynchronizerConnectivityStatusStore
import com.digitalasset.canton.participant.store.memory.InMemorySynchronizerConnectivityStatusStore
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait SynchronizerConnectivityStatusStore {

  /** Sets new synchronizer parameters. Calls with the same argument are idempotent.
    *
    * @return
    *   The future fails with an [[java.lang.IllegalArgumentException]] if different synchronizer
    *   parameters have been stored before.
    */
  def setParameters(newParameters: StaticSynchronizerParameters)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the last set synchronizer parameters, if any. */
  def lastParameters(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StaticSynchronizerParameters]]

  def setTopologyInitialized()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  def isTopologyInitialized(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]
}

object SynchronizerConnectivityStatusStore {
  def apply(
      storage: Storage,
      synchronizerId: PhysicalSynchronizerId,
      processingTimeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SynchronizerConnectivityStatusStore =
    storage match {
      case _: MemoryStorage =>
        new InMemorySynchronizerConnectivityStatusStore(
        )
      case db: DbStorage =>
        new DbSynchronizerConnectivityStatusStore(
          synchronizerId,
          db,
          processingTimeouts,
          loggerFactory,
        )
    }
}
