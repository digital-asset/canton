// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.foldable.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
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
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, SimpleExecutionQueue}

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

  private val currentProcessorRef: AtomicReference[Option[BaseDigestProcessor]] =
    new AtomicReference[Option[BaseDigestProcessor]](None)

  def currentProcessor: Option[BaseDigestProcessor] = currentProcessorRef.get()

  private val sequentialQueue = new SimpleExecutionQueue(
    s"digest-processor-manager-$synchronizerId",
    futureSupervisor,
    timeouts,
    loggerFactory,
    logTaskTiming = true,
    crashOnFailure = exitOnFatalFailures,
  )

  /** Stops the currently running digest processor and starts a new
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]].
    */
  def startRunningDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequentialQueue.executeUS(
      {
        val oldProc = currentProcessorRef.get()
        for {
          _ <- oldProc.traverse_(stopProcessorIgnoringShutdown)
          rdp = digestProcessorFactory
            .createRunningDigestProcessor(synchronizerAlias, synchronizerId, tickSignaller)
          _ = currentProcessorRef.set(Some(rdp))
          _ <- rdp.start()
        } yield ()
      },
      "start running digest processor",
    )

  /** Starts digest reinitialization for this manager's `synchronizerId`.
    *
    * Returns immediately with the target reinitialization timestamp once the processor starts. If a
    * reinitialization is already in progress, joins the ongoing run and returns its reinit
    * timestamp. If a [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is
    * running when a new reinitialization is started, it is stopped.
    *
    * If `runningDigestProcessorShouldStartAfter` is true, a new
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is automatically
    * kicked off asynchronously after reinitialization completes. Default is true.
    */
  def startReinitializationDigestProcessor(
      runningDigestProcessorShouldStartAfter: Boolean = true
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    sequentialQueue
      .executeUS(
        currentProcessorRef.get() match {
          case Some(reinitDp: ReinitializingDigestProcessor) if reinitDp.isStartingOrStarted =>
            logger.info("A digest reinitialization is already in progress. Joining ongoing run.")
            FutureUnlessShutdown
              .pure(reinitDp.reinitializingTimepoint.map(_.recordTime))

          case Some(otherProcessor) =>
            logger.info(s"Stopping $otherProcessor before starting reinitialization")
            for {
              _ <- stopProcessorIgnoringShutdown(otherProcessor)
              res <- createAndStartReinitProcessor(runningDigestProcessorShouldStartAfter)
            } yield res

          case None =>
            createAndStartReinitProcessor(runningDigestProcessorShouldStartAfter)
        },
        "start reinitialization digest processor",
      )

  private def createAndStartReinitProcessor(runningDigestProcessorShouldStartAfter: Boolean)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = {
    val reinitDp = digestProcessorFactory.createReinitializingDigestProcessor(
      synchronizerAlias,
      synchronizerId,
    )
    currentProcessorRef.set(Some(reinitDp))

    reinitDp
      .start()
      .map { _ =>
        reinitDp.completionFuture
          .onComplete { _ =>
            if (runningDigestProcessorShouldStartAfter) {
              FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
                sequentialQueue.executeUS(
                  if (currentProcessorRef.get().contains(reinitDp)) {
                    logger.info(
                      s"Reinitialization finished for $synchronizerId. Restarting running digest processor."
                    )
                    val runningDp = digestProcessorFactory.createRunningDigestProcessor(
                      synchronizerAlias,
                      synchronizerId,
                      tickSignaller,
                    )
                    currentProcessorRef.set(Some(runningDp))
                    runningDp.start()
                  } else {
                    logger.debug(
                      s"Reinitialization finished for $synchronizerId, but current processor has changed. " +
                        s"Skipping automatic restart."
                    )
                    FutureUnlessShutdown.unit
                  },
                  "auto-restart running digest processor after reinitialization",
                ),
                s"failed to auto-restart running digest processor for $synchronizerId",
              )
            } else if (!runningDigestProcessorShouldStartAfter) {
              logger.info(
                s"Reinitialization finished for $synchronizerId. " +
                  s"Skipping automatic restart because running digest processor was not active prior to reinitialization."
              )
            }
          }
        reinitDp.reinitializingTimepoint.map(_.recordTime)
      }
  }

  private def stopProcessorIgnoringShutdown(proc: BaseDigestProcessor)(implicit
      traceContext: TraceContext
  ) =
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
        currentProcessorRef.get().traverse_(_.stop().onShutdown(())),
        timeouts.shutdownProcessing,
      ),
    )(logger)
  }
}
