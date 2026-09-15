// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.health.{HealthComponent, MutableHealthComponent}
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  DelayUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DigestProcessorManager(
    synchronizerAlias: SynchronizerAlias,
    synchronizerId: SynchronizerId,
    digestProcessorFactory: DigestProcessorFactory,
    tickSignaller: TickSignaller,
    exitOnFatalFailures: Boolean,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val healthComponent: MutableHealthComponent = MutableHealthComponent(
    loggerFactory,
    s"digest-processor-manager-$synchronizerId",
    timeouts,
  )
  def health: HealthComponent = healthComponent

  private val currentProcessorRef = new AtomicReference[Option[DigestProcessor]](None)

  @VisibleForTesting
  def currentProcessor: Option[DigestProcessor] = currentProcessorRef.get()

  private val sequentialQueue = new SimpleExecutionQueue(
    s"digest-processor-manager-$synchronizerId",
    futureSupervisor,
    timeouts,
    loggerFactory,
    logTaskTiming = true,
    crashOnFailure = exitOnFatalFailures,
  )

  /** Ensures that a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is
    * running.
    *
    *   - If there is no digest processor currently running, start a new
    *     [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]].
    *   - If a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is already
    *     running or starting up, do nothing.
    *   - If a [[com.digitalasset.canton.participant.commitment.ReinitializingDigestProcessor]] is
    *     already running or starting up, do nothing. A new running digest processor will be started
    *     automatically after the reinitialization completes.
    */
  def startRunningDigestProcessorAsync()(implicit
      traceContext: TraceContext
  ): Unit =
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      startRunningDigestProcessor(),
      s"Failed to start running digest processor for $synchronizerId",
    )

  /** Starts a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] or joins
    * one that is starting up.
    *
    * @return
    *   The `startingFuture` of the new or currently starting
    *   [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]], except if the
    *   current processor is a
    *   [[com.digitalasset.canton.participant.commitment.ReinitializingDigestProcessor]], in which
    *   case the returned futures is just a completed future. A
    *   [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] will be
    *   automatically started after the reinitialization completes.
    */
  @VisibleForTesting
  private[canton] def startRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val currentProcessorF = sequentialQueue.execute(
      currentProcessorRef.get() match {
        case None =>
          val processor = createAndStartRunningDigestProcessor()
          Future.successful(processor.startingFuture)
        case Some(oldProcessor) =>
          if (!oldProcessor.isStartingOrStarted) {
            // Explicitly stop the old processor in the case that it has not even yet been started
            logger.info(s"Stopping $oldProcessor before starting new running digest processor")
            stopProcessorIgnoringShutdown(oldProcessor).map { _ =>
              createAndStartRunningDigestProcessor().startingFuture
            }
          } else
            oldProcessor match {
              case p: RunningDigestProcessor =>
                // nothing to do, there is already a running digest processor
                Future.successful(p.startingFuture)
              case _: ReinitializingDigestProcessor =>
                // nothing to do, because the reinitializing digest processor will automatically start a
                // running digest processor once it finishes reinitialization
                Future.successful(FutureUnlessShutdown.unit)
            }
      },
      "start running digest processor",
    )
    currentProcessorF.flatten
  }

  def reinitializeIfEmptyAndStartRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    MonadUtil.whenM(digestProcessorFactory.needsReinitialization(synchronizerId))(
      startReinitializationDigestProcessor().void
    )

  /** Starts digest reinitialization for this manager's `synchronizerId`.
    *
    * Returns immediately with the target reinitialization timestamp while the processor starts. If
    * a reinitialization is already in progress, immediately returns its reinit timestamp. If a
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is running when a
    * new reinitialization is started, it is stopped and the future completes only afterwards.
    */
  def startReinitializationDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[CantonTimestamp] =
    sequentialQueue.execute(
      currentProcessorRef.get() match {
        case None =>
          val reinitDP = createAndStartReinitProcessor()
          Future.successful(reinitDP.reinitializingTimepoint.recordTime)
        case Some(oldProcessor) =>
          if (!oldProcessor.isStartingOrStarted) {
            logger.info(
              s"Stopping $oldProcessor before starting reinitialization"
            )
            // Explicitly stop the old processor in the case that it has not even yet been started
            stopProcessorIgnoringShutdown(oldProcessor).map { _ =>
              val processor = createAndStartReinitProcessor()
              processor.reinitializingTimepoint.recordTime
            }
          } else {
            oldProcessor match {
              case reinitProcessor: ReinitializingDigestProcessor =>
                logger.info(
                  "A digest reinitialization is already in progress. Joining ongoing run."
                )
                Future.successful(reinitProcessor.reinitializingTimepoint.recordTime)
              case otherProcessor: RunningDigestProcessor =>
                logger.info(s"Stopping $otherProcessor before starting reinitialization")
                stopProcessorIgnoringShutdown(otherProcessor).map { _ =>
                  val reinitDP = createAndStartReinitProcessor()
                  reinitDP.reinitializingTimepoint.recordTime
                }
            }
          }
      },
      "start reinitialization digest processor",
    )

  private def createAndStartRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): RunningDigestProcessor = {
    val rdp = digestProcessorFactory
      .createRunningDigestProcessor(synchronizerAlias, synchronizerId, tickSignaller)
    startAsync(rdp, delayStartOfFollowUpProcessor = true)
  }

  private def createAndStartReinitProcessor(
  )(implicit
      traceContext: TraceContext
  ): ReinitializingDigestProcessor = {
    val reinitDp =
      digestProcessorFactory.createReinitializingDigestProcessor(synchronizerAlias, synchronizerId)
    startAsync(reinitDp, delayStartOfFollowUpProcessor = false)
  }

  private def startAsync(processor: DigestProcessor, delayStartOfFollowUpProcessor: Boolean)(
      implicit traceContext: TraceContext
  ): processor.type = {
    currentProcessorRef.set(Some(processor))
    healthComponent.set(processor.health)
    processor.startAsync()
    scheduleRunningDigestProcessorOnCompletion(processor, delayStartOfFollowUpProcessor)
    processor
  }

  private def scheduleRunningDigestProcessorOnCompletion(
      processor: BaseDigestProcessor,
      delayStartOfFollowUpProcessor: Boolean,
  )(implicit traceContext: TraceContext): Unit =
    processor.completionFuture.onComplete {
      case Success(_) =>
        import scala.concurrent.duration.*
        val maybeDelayedF =
          if (delayStartOfFollowUpProcessor) {
            // This short delay prevents a busy retry loop in case of a repeated start and
            // shutdown of running digest processors.
            DelayUtil
              .delayIfNotClosing("schedule-followup-running-digest-processor", 1.second, this)
          } else FutureUnlessShutdown.unit
        val scheduledF =
          maybeDelayedF.flatMap { _ =>
            sequentialQueue.execute(
              {
                scheduleRunningDigestProcessorOnCompletionOnQueue(processor)
                Future.unit
              },
              "scheduled follow-up task after digest processor completion",
            )
          }
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          scheduledF,
          "Follow-up task scheduling failed",
        )
      case Failure(_) =>
      // nothing to do, the follow-up task should only be scheduled if the processor terminated successfully
    }

  private def scheduleRunningDigestProcessorOnCompletionOnQueue(
      expectedProcessor: BaseDigestProcessor
  )(implicit
      traceContext: TraceContext
  ): Unit =
    currentProcessorRef.get() match {
      case None =>
        ErrorUtil.invalidState(
          "Follow-up task scheduling did not find a digest processor in the state"
        )
      case Some(currentProcessor) =>
        if (currentProcessor != expectedProcessor)
          logger.info(
            s"Processor $expectedProcessor has finished. Not starting a follow-up running digest processor because the processor has changed to $currentProcessor"
          )
        else {
          logger.info(
            s"Processor $expectedProcessor finished. Starting a running digest processor."
          )
          createAndStartRunningDigestProcessor().discard
        }
    }

  private def stopProcessorIgnoringShutdown(proc: BaseDigestProcessor)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // Don't let the termination of the old processor with `AbortedDueToShutdown` prevent the startup of a new processor
    proc.stop().onShutdown {
      logger.debug(
        s"Currently running digest processor $proc stopped with $AbortedDueToShutdown"
      )
    }

  override protected def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    LifeCycle.close(
      sequentialQueue,
      AsyncCloseable(
        "current digest processor",
        currentProcessorRef.get().traverse_(_.stop().onShutdown(())),
        timeouts.shutdownProcessing,
      ),
    )(logger)
  }
}
