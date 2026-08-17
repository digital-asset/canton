// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.functor.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  RunOnClosing,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.{
  TickListener,
  TickSignaller,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.signalling.{EventSignaller, LocalEventSignaller}
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.Done
import org.apache.pekko.stream.{KillSwitch, Materializer}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Used to manage all running digest processors for the participant.
  */
class AcsCommitmentProcessorManager(
    digestProcessorFactory: DigestProcessorFactory,
    matcherFactory: ReceivedAcsCommitmentMatcherFactory,
    aliasForSynchronizerId: SynchronizerId => Option[SynchronizerAlias],
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable {

  private val synchronizers =
    mutable.Map[SynchronizerId, SynchronizerCommitmentState]()

  private val lock = new Mutex()

  def getOrCreate(
      synchronizerId: SynchronizerId
  )(implicit traceContext: TraceContext): SynchronizerCommitmentState =
    lock.exclusive {
      synchronizers.getOrElseUpdate(
        synchronizerId, {
          val synchronizerLoggerFactory =
            loggerFactory.append("synchronizer", synchronizerId.toString)
          val signaller = new LocalEventSignaller[TickListener, Offset](
            "subscriber",
            timeouts,
            synchronizerLoggerFactory,
          )
          val alias = aliasForSynchronizerId(synchronizerId).getOrElse(
            ErrorUtil.invalidArgument(s"No alias for synchronizer ID $synchronizerId")
          )
          val digestProcessorManager = new DigestProcessorManager(
            alias,
            synchronizerId,
            digestProcessorFactory,
            signaller,
            exitOnFatalFailures,
            futureSupervisor,
            timeouts,
            synchronizerLoggerFactory,
          )
          val matcherF = matcherFactory.startMatcherPipeline(
            alias,
            synchronizerId,
            signaller,
          )
          SynchronizerCommitmentState(digestProcessorManager, signaller, matcherF)
        },
      )
    }

  private def getAllAndClear(): Seq[(SynchronizerId, SynchronizerCommitmentState)] =
    lock.exclusive {
      val tmp = synchronizers.toSeq
      synchronizers.clear()
      tmp
    }

  override protected def onClosed(): Unit =
    LifeCycle.close(getAllAndClear().flatMap(closeSynchronizerCommitmentState))(logger)

  /** Subscribe to new synchronizer connections.
    */
  def subscribeToSynchronizerConnections(sync: CantonSyncService)(implicit
      traceContext: TraceContext
  ): Unit =
    // whenever the participant connects to a synchronizer, start the digest processor
    synchronizeWithClosingSync("subscribe to synchronizer connections") {
      val handle = sync.subscribeToConnections {
        _.withTraceContext { implicit traceContext => synchronizerId =>
          logger.info(s"Starting commitment processor pipeline for synchronizer $synchronizerId")
          FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
            {
              val syncState = getOrCreate(synchronizerId)
              val connectedSynchronizerO = sync.readyConnectedSynchronizerById(synchronizerId)
              if (connectedSynchronizerO.isEmpty) {
                logger.warn(s"Cannot start ACS commitment sender for synchronizer $synchronizerId")
              }
              val senderO = connectedSynchronizerO.flatMap { connectedSynchronizer =>
                Option.when(
                  connectedSynchronizer.psid.protocolVersion >= ProtocolVersion.acsCommitmentRedesign
                )(connectedSynchronizer.ephemeral.acsCommitmentSender)
              }
              for {
                _ <- syncState.digestProcessorManager.startRunningDigestProcessor()
                _ = senderO.foreach(
                  _.startPipeline(
                    syncState.tickSignaller
                      .readSignals(TickListener.TickOnlyListener, "ACS commitment sender")
                      .map(_.signal)
                  )
                )
              } yield ()
            },
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

  private def closeSynchronizerCommitmentState(
      item: (SynchronizerId, SynchronizerCommitmentState)
  ): Seq[AutoCloseable] = {
    import TraceContext.Implicits.Empty.*
    val (synchronizerId, state) = item
    val matcherClose = AsyncCloseable(
      s"ReceivedAcsCommitmentMatcher($synchronizerId)",
      state.matcherF
        .flatMap { case (killSwitch, doneF) =>
          killSwitch.shutdown()
          FutureUnlessShutdown.outcomeF(doneF.void)
        }
        .onShutdown(()),
      timeouts.shutdownProcessing,
    )

    Seq(
      state.digestProcessorManager,
      matcherClose,
      state.tickSignaller,
    )
  }
}

/** This class holds various components for processing the ACS commitments for a particular
  * synchronizer.
  * @param digestProcessorManager
  *   for managing digest processors
  * @param tickSignaller
  *   for connecting the various components with
  *   [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointWritten]]
  *   signals for ticks
  */
final case class SynchronizerCommitmentState(
    digestProcessorManager: DigestProcessorManager,
    tickSignaller: TickSignaller,
    matcherF: FutureUnlessShutdown[(KillSwitch, Future[Done])],
)

object SynchronizerCommitmentState {
  // The subscriber key type is Unit, because:
  // - the event signaller supports multiple subscribers for the same key, and
  // - the checkpoints aren't really "assigned" to a specific set of subscribers
  type TickSignaller = EventSignaller[TickListener, Offset]

  sealed trait TickListener extends Product with Serializable
  object TickListener {
    case object TickOnlyListener extends TickListener
    case object TicksAndReceivedCommitmentCheckpointsListener extends TickListener
  }

}
