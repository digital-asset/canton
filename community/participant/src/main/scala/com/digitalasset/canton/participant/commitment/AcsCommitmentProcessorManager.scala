// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.functor.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.health.{
  ComponentHealthState,
  CompositeHealthComponent,
  DelegatingMutableHealthComponent,
  HealthComponent,
}
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  HasRunOnClosing,
  LifeCycle,
  OnShutdownRunner,
  RunOnClosing,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.{
  TickListener,
  TickSignaller,
}
import com.digitalasset.canton.participant.metrics.CommitmentMetrics
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
    metricsLookup: SynchronizerAlias => CommitmentMetrics,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable {

  private val healthComponent = new DelegatingMutableHealthComponent[SynchronizerId](
    loggerFactory,
    AcsCommitmentProcessorManager.healthName,
    timeouts,
    states => ComponentHealthState.reduceToWorstStateOrOk(states.values),
    AcsCommitmentHealthState.Stopped.componentHealthState,
  )

  def health: HealthComponent = healthComponent

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
          val alias = aliasForSynchronizerId(synchronizerId).getOrElse(
            ErrorUtil.invalidArgument(s"No alias for synchronizer ID $synchronizerId")
          )
          val signaller = new LocalEventSignaller[TickListener, Offset](
            "subscriber",
            timeouts,
            synchronizerLoggerFactory,
          )
          val signallerHealth = createSignallerHealth(
            synchronizerId,
            signaller,
            metricsLookup(alias),
            synchronizerLoggerFactory,
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
          val (matcherHealth, matcherF) = matcherFactory.startMatcherPipeline(
            alias,
            synchronizerId,
            signaller,
          )

          val state = SynchronizerCommitmentState(
            synchronizerId = synchronizerId,
            digestProcessorManager = digestProcessorManager,
            tickSignaller = signaller,
            signallerHealth = signallerHealth,
            matcherF = matcherF,
            matcherHealth = matcherHealth,
            loggerFactory = synchronizerLoggerFactory,
          )

          healthComponent.set(synchronizerId, state)
          state
        },
      )
    }

  private def createSignallerHealth(
      synchronizerId: SynchronizerId,
      signaller: LocalEventSignaller[TickListener, Offset],
      metrics: CommitmentMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext) = {
    val signallerHealth = AcsCommitmentComponentHealthReporter(
      s"commitment-tick-signaller-$synchronizerId",
      metrics.tickSignallerHealth,
      loggerFactory,
    )
    signallerHealth.reportHealth(AcsCommitmentHealthState.Started)
    signaller.runOnOrAfterClose_(new RunOnClosing {
      override def name: String = s"update signaller health $synchronizerId"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit =
        signallerHealth.reportHealth(AcsCommitmentHealthState.Stopped)
    })
    signallerHealth
  }

  private def getAllAndClear(): Seq[(SynchronizerId, SynchronizerCommitmentState)] =
    lock.exclusive {
      val tmp = synchronizers.toSeq
      synchronizers.clear()
      tmp
    }

  override protected def onClosed(): Unit = {
    val closeables = getAllAndClear().flatMap(closeSynchronizerCommitmentState) :+ healthComponent
    LifeCycle.close(closeables)(logger)
  }

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
    healthComponent.remove(synchronizerId)
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

object AcsCommitmentProcessorManager {
  val healthName = "acs-commitment-processor-manager"
}

/** This class holds various components for processing the ACS commitments for a particular
  * synchronizer.
  * @param digestProcessorManager
  *   for managing digest processors
  * @param tickSignaller
  *   for connecting the various components with
  *   [[com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointWritten]]
  *   signals for ticks
  * @param signallerHealth
  *   the [[com.digitalasset.canton.participant.commitment.AcsCommitmentComponentHealthReporter]]
  *   for reporting the health of the tick signaller, whose health is aggregated into the ACS
  *   commitment processing health for the given synchronizer.
  * @param matcherF
  *   the future that completes once the ACS commitment matcher has started up.
  * @param matcherHealth
  *   the [[com.digitalasset.canton.participant.commitment.AcsCommitmentComponentHealthReporter]]
  *   for reporting the health of the commitment matcher, whose health is aggregated into the ACS
  *   commitment processing health for the given synchronizer.
  */
final case class SynchronizerCommitmentState(
    synchronizerId: SynchronizerId,
    digestProcessorManager: DigestProcessorManager,
    tickSignaller: TickSignaller,
    signallerHealth: AcsCommitmentComponentHealthReporter,
    matcherF: FutureUnlessShutdown[(KillSwitch, Future[Done])],
    matcherHealth: AcsCommitmentComponentHealthReporter,
    override protected val loggerFactory: NamedLoggerFactory,
) extends CompositeHealthComponent[String, HealthComponent]
    with NamedLogging {

  setDependency(matcherHealth.name, matcherHealth.healthComponent)
  setDependency(signallerHealth.name, signallerHealth.healthComponent)
  setDependency(digestProcessorManager.health.name, digestProcessorManager.health)

  override protected def combineDependentStates: ComponentHealthState =
    ComponentHealthState.reduceToWorstStateOrOk(getDependencies.values.map(_.getState))
  override def name: String = s"commitment-state-$synchronizerId"
  override protected def initialHealthState: ComponentHealthState =
    AcsCommitmentHealthState.NotInitialized.componentHealthState
  override protected def associatedHasRunOnClosing: HasRunOnClosing =
    new OnShutdownRunner.PureOnShutdownRunner(logger)
}

object SynchronizerCommitmentState {
  type TickSignaller = EventSignaller[TickListener, Offset]

  sealed trait TickListener extends Product with Serializable
  object TickListener {
    case object TickOnlyListener extends TickListener
    case object TicksAndReceivedCommitmentCheckpointsListener extends TickListener
  }

}
