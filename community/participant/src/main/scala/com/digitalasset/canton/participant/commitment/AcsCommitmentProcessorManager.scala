// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle, RunOnClosing}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.CantonSyncService.SyncServiceHandle
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Used to manage all running digest processors for the participant.
  */
class AcsCommitmentProcessorManager(
    digestProcessorFactory: DigestProcessorFactory,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val synchronizers =
    mutable.Map[SynchronizerId, SynchronizerCommitmentState]()

  private val lock = new Mutex()

  def getOrCreate(synchronizerId: SynchronizerId): SynchronizerCommitmentState =
    lock.exclusive {
      synchronizers.getOrElseUpdate(
        synchronizerId,
        SynchronizerCommitmentState(
          new DigestProcessorManager(
            synchronizerId,
            digestProcessorFactory,
            exitOnFatalFailures,
            futureSupervisor,
            timeouts,
            loggerFactory.append("synchronizer", synchronizerId.toString),
          )
        ),
      )
    }

  private def getAllAndClear(): Seq[SynchronizerCommitmentState] = lock.exclusive {
    val tmp = synchronizers.values.toSeq
    synchronizers.clear()
    tmp
  }

  override protected def onClosed(): Unit = {
    def closeSynchronizer(sync: SynchronizerCommitmentState): Unit =
      LifeCycle.close(sync.digestProcessorManager)(logger)
    getAllAndClear().foreach(closeSynchronizer)
  }

  /** Subscribe to new synchronizer connections.
    */
  def subscribeToSynchronizerConnections(sync: SyncServiceHandle)(implicit
      traceContext: TraceContext
  ): Unit =
    // whenever the participant connects to a synchronizer, start the digest processor
    synchronizeWithClosingSync("subscribe to synchronizer connections") {
      val handle = sync.subscribeToConnections {
        _.withTraceContext { implicit traceContext => synchronizerId =>
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            getOrCreate(synchronizerId).digestProcessorManager.startRunningDigestProcessor(),
            s"failed to start running digest processor for $synchronizerId",
          )
        }
      }
      runOnOrAfterClose_(new RunOnClosing {
        override def name: String = "unsubscribing synchronizer connection listener"
        override def done: Boolean = false
        override def run()(implicit traceContext: TraceContext): Unit = handle.close()
      })
    }.onShutdown(())
}

final case class SynchronizerCommitmentState(digestProcessorManager: DigestProcessorManager)
