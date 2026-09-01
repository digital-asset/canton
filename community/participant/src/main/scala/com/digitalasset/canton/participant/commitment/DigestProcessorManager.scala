// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.foldable.*
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
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.TickSignaller
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, SimpleExecutionQueue}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

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

  import DigestProcessorManager.*

  private val healthComponent: MutableHealthComponent = MutableHealthComponent(
    loggerFactory,
    s"digest-processor-manager-$synchronizerId",
    timeouts,
  )
  def health: HealthComponent = healthComponent

  private val state: AtomicReference[State] =
    new AtomicReference[State](State.initial)

  def currentProcessor: Option[DigestProcessor] = state.get().currentDigestProcessor

  private val sequentialQueue = new SimpleExecutionQueue(
    s"digest-processor-manager-$synchronizerId",
    futureSupervisor,
    timeouts,
    loggerFactory,
    logTaskTiming = true,
    crashOnFailure = exitOnFatalFailures,
  )

  /** Ensures that a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is
    * running or will be running after reinitialization completes. The returned future completes
    * once a running digest processor pipeline is up and running.
    *
    *   - If there is no digest processor currently running, start a new
    *     [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]].
    *   - If a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is already
    *     running or starting up, do nothing.
    *   - If a [[com.digitalasset.canton.participant.commitment.ReinitializingDigestProcessor]] is
    *     already running or starting up, register the start of a running digest processor after the
    *     reinitialization completes.
    */
  def startRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val currentProcessorF = sequentialQueue.executeUS(
      state.get() match {
        case State.Empty =>
          val processor = createAndStartRunningDigestProcessor()
          FutureUnlessShutdown.pure(processor.startingFuture)
        case State.WithProcessor(oldProcessor, followUp) =>
          if (!oldProcessor.isStartingOrStarted) {
            // Explicitly stop the old processor in the case that it has not even yet been started
            logger.info(s"Stopping $oldProcessor before starting new running digest processor")
            stopProcessorIgnoringShutdown(oldProcessor).map { _ =>
              val processor = createAndStartRunningDigestProcessor()
              processor.startingFuture
            }
          } else
            oldProcessor match {
              case _: RunningDigestProcessor =>
                FutureUnlessShutdown.pure(oldProcessor.startingFuture)
              case _: ReinitializingDigestProcessor =>
                val promise = PromiseUnlessShutdown.unsupervised[Unit]()
                val newFollowUp = followUp.mergeWith(
                  FollowUpProcessor.StartRunningDigestProcessor(promise, traceContext)
                )
                state.set(State.WithProcessor(oldProcessor, newFollowUp))
                FutureUnlessShutdown.pure(promise.futureUS)
            }
      },
      "start running digest processor",
    )
    currentProcessorF.flatten
  }

  def reinitializeIfEmptyAndStartRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    for {
      needReinitialization <- digestProcessorFactory.needsReinitialization(synchronizerId)
      _ <-
        if (needReinitialization) startReinitializationDigestProcessor()
        else FutureUnlessShutdown.unit
      _ <- startRunningDigestProcessor()
    } yield ()

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
    sequentialQueue.executeUS(
      state.get() match {
        case State.Empty =>
          val reinitDP = createAndStartReinitProcessor()
          FutureUnlessShutdown.pure(reinitDP.reinitializingTimepoint.recordTime)
        case State.WithProcessor(oldProcessor, _) =>
          if (!oldProcessor.isStartingOrStarted) {
            logger.info(
              s"Stopping $oldProcessor before starting new reinitialization digest processor"
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
                FutureUnlessShutdown.pure(reinitProcessor.reinitializingTimepoint.recordTime)
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
    startAsync(rdp)
  }

  private def createAndStartReinitProcessor()(implicit
      traceContext: TraceContext
  ): ReinitializingDigestProcessor = {
    val reinitDp =
      digestProcessorFactory.createReinitializingDigestProcessor(synchronizerAlias, synchronizerId)
    startAsync(reinitDp)
  }

  private def startAsync(processor: DigestProcessor)(implicit
      traceContext: TraceContext
  ): processor.type = {
    state.set(State.WithProcessor(processor, FollowUpProcessor.NoFollowUpProcessor))
    healthComponent.set(processor.health)
    processor.startAsync()
    scheduleFollowUpOnCompletion(processor)
    processor
  }

  private def scheduleFollowUpOnCompletion(
      processor: BaseDigestProcessor
  )(implicit traceContext: TraceContext): Unit =
    processor.completionFuture.onComplete { _ =>
      val scheduledF = sequentialQueue.executeUS(
        scheduleFollowUpOnCompletionOnQueue(processor),
        "scheduled follow-up task after digest processor completion",
      )
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        scheduledF,
        "Follow-up task scheduling failed",
      )
    }

  private def scheduleFollowUpOnCompletionOnQueue(expectedProcessor: BaseDigestProcessor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    state.get() match {
      case State.Empty =>
        ErrorUtil.invalidState(
          "Follow-up task scheduling did not find a digest processor in the state"
        )
      case State.WithProcessor(currentProcessor, followUp) =>
        followUp match {
          case FollowUpProcessor.NoFollowUpProcessor =>
            logger.info(
              s"Processor $expectedProcessor has finished. No follow-up processors to be started"
            )

          case FollowUpProcessor.StartRunningDigestProcessor(promise, tc) =>
            implicit val traceContext: TraceContext = tc
            if (currentProcessor != expectedProcessor)
              logger.info(
                s"Processor $expectedProcessor has finished. Skipping to start follow-up digest processor because the processor has changed to $currentProcessor"
              )
            else {
              logger.info(
                s"Processor $expectedProcessor finished. Restarting running digest processor."
              )
              val rdp = createAndStartRunningDigestProcessor()
              promise.completeWithUS(rdp.startingFuture).discard
            }
        }

    }
    FutureUnlessShutdown.unit
  }

  private def stopProcessorIgnoringShutdown(proc: BaseDigestProcessor)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // Don't let the termination of the old processor with `AbortedDueToShutdown` prevent the startup of a new processor
    proc.stop().transformOnShutdown {
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
        state.get().currentDigestProcessor.traverse_(_.stop().onShutdown(())),
        timeouts.shutdownProcessing,
      ),
    )(logger)
  }
}

object DigestProcessorManager {
  private sealed trait State extends Product with Serializable {
    def currentDigestProcessor: Option[DigestProcessor]
  }
  private object State {
    case object Empty extends State {
      override def currentDigestProcessor: Option[DigestProcessor] = None
    }

    final case class WithProcessor(
        processor: DigestProcessor,
        followUp: FollowUpProcessor,
    ) extends State {
      override def currentDigestProcessor: Option[DigestProcessor] = Some(processor)
    }

    def initial: State = Empty
  }

  /** A task that should be executed after the current
    * [[com.digitalasset.canton.participant.commitment.DigestProcessor]] has stopped.
    */
  private sealed trait FollowUpProcessor extends Product with Serializable {

    /** Merges two follow-up tasks into one. */
    def mergeWith(other: FollowUpProcessor): FollowUpProcessor
  }
  private object FollowUpProcessor {
    case object NoFollowUpProcessor extends FollowUpProcessor {
      override def mergeWith(other: FollowUpProcessor): FollowUpProcessor = other
    }

    final case class StartRunningDigestProcessor(
        startedPromise: PromiseUnlessShutdown[Unit],
        traceContext: TraceContext,
    ) extends FollowUpProcessor {
      override def mergeWith(other: FollowUpProcessor): FollowUpProcessor = other match {
        case NoFollowUpProcessor => this
        case StartRunningDigestProcessor(otherPromise, _) =>
          startedPromise.completeWith(otherPromise.future)
          other
      }
    }
  }
}
