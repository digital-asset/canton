// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.nameof.NameOf.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  RunOnClosing,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor
import com.digitalasset.canton.participant.config.AcsCommitmentConfig
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.sync.CantonSyncService.SyncServiceHandle
import com.digitalasset.canton.participant.topology.TopologyLookup
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  EitherTUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  Mutex,
}
import org.apache.pekko.stream.Materializer

import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Used to manage all running digest processors for the participant.
  */
class AcsDigestProcessorManager(
    participantId: ParticipantId,
    topologyLookup: TopologyLookup,
    acsDigestStoreLookup: SynchronizerId => Option[AcsDigestStore],
    internalIndexService: InternalIndexService,
    pureCrypto: CryptoPureApi,
    stringInterning: StringInterning,
    acsCommitmentConfig: AcsCommitmentConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable {

  private val digestProcessors =
    mutable.Map[SynchronizerId, RunningDigestProcessor]()

  private val lock = new Mutex()

  def get(synchronizerId: SynchronizerId): Option[RunningDigestProcessor] =
    lock.exclusive(digestProcessors.get(synchronizerId))

  /** Adds and starts a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]]
    * for the given `synchronizerId`, if there doesn't already exist one for the `synchronizerId`.
    *
    * This method is safe to call multiple times for a `synchronizerId`. At most one digest
    * processor will be running at the same time.
    */
  def startDigestProcessorForSynchronizer(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    synchronizeWithClosing(s"$functionFullName($synchronizerId)") {
      val rdp = new RunningDigestProcessor(
        participantId,
        synchronizerId,
        acsCommitmentConfig,
        indexService = internalIndexService,
        getTopologySnapshot = tracedTimestamp =>
          EitherTUtil.toFutureUnlessShutdown(
            topologyLookup
              .maybeOfflineAwaitTopologySnapshot(synchronizerId, tracedTimestamp.value)(
                tracedTimestamp.traceContext
              )
              // TODO(#33084): cleanup error types
              .leftMap(_.asGrpcError)
          ),
        acsDigestStore = acsDigestStoreLookup(synchronizerId)
          .getOrElse(ErrorUtil.invalidState("AcsDigestStore not initialized")),
        stringInterning = stringInterning,
        hashOps = pureCrypto,
        timeouts,
        loggerFactory.append("synchronizer", synchronizerId.toString),
      )

      val shouldStart = lock.exclusive {
        if (digestProcessors.contains(synchronizerId)) {
          logger.info(s"Digest processor for $synchronizerId has already been started.")
          false
        } else {
          digestProcessors.put(synchronizerId, rdp).discard
          true
        }
      }
      MonadUtil.when(shouldStart)(rdp.start())
    }

  private def getAllAndClear(): Seq[RunningDigestProcessor] = lock.exclusive {
    val tmp = digestProcessors.values.toSeq
    digestProcessors.clear()
    tmp
  }

  /** Stops and removes all digest processors.
    */
  def stopAndRemoveAll(): Unit =
    getAllAndClear().foreach(rdp => LifeCycle.close(rdp)(logger))

  override protected def onClosed(): Unit =
    // in practice, the digest processors should be stopped and removed via the CantonSyncService.onInternalIndexServiceUnregistered,
    // but let's leave this here for safety
    stopAndRemoveAll()

  /** Subscribe to new synchronizer connections.
    */
  def subscribeToSynchronizerConnections(sync: SyncServiceHandle)(implicit
      traceContext: TraceContext
  ): Unit =
    // whenever the participant connects to a synchronizer, start the digest processor
    synchronizeWithClosingSync("subscribe to synchronizer connections") {
      val handle = sync.subscribeToConnections {
        _.withTraceContext { implicit traceContext => synchronizerId =>
          // no need to check whether the digest processor is already running or not. this is done
          // by acsDigestProcessorsManager.
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            startDigestProcessorForSynchronizer(synchronizerId),
            s"failed to start running digest processor for $synchronizerId",
          )
        }
      }
      runOnClose(new RunOnClosing {
        override def name: String = "unsubscribing synchronizer connection listener"
        override def done: Boolean = false
        override def run()(implicit traceContext: TraceContext): Unit = handle.close()
      })
        // We don't want to cancel the RunOnClosing task early, and
        // the call to runOnClose is guarded by synchronizeWithClosingSync.
        // Therefore, the lifecycle handle can be discarded.
        .discard
    }.onShutdown(())

}
