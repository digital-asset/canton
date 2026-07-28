// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.syntax.foldable.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SimpleExecutionQueue

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

class DigestProcessorManager(
    synchronizerId: SynchronizerId,
    digestProcessorFactory: DigestProcessorFactory,
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
        import cats.syntax.foldable.*
        val oldProc = currentProcessorRef.get()
        for {
          _ <- oldProc.traverse_(stopProcessorIgnoringShutdown)
          rdp = digestProcessorFactory.createRunningDigestProcessor(synchronizerId)
          _ = currentProcessorRef.set(Some(rdp))
          _ <- rdp.start()
        } yield ()
      },
      "start running digest processor",
    )

  /** Starts the reinitialization of the digests for the given `synchronizerId`. Does nothing, if
    * another reinitialization is already in progress. If a
    * [[com.digitalasset.canton.participant.commitment.RunningDigestProcessor]] is currently
    * running, then it is first stopped before starting the reinitialization.
    */
  def startReinitializationDigestProcessor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequentialQueue.executeUS(
      currentProcessorRef.get() match {
        case None =>
          val rdp = digestProcessorFactory.createReinitializingDigestProcessor(synchronizerId)
          currentProcessorRef.set(Some(rdp))
          rdp.start()
        case Some(proc) if proc.isReinitializingProcessor && proc.isStartingOrStarted =>
          logger.info("A digest reinitialization is already in progress.")
          proc.startingFuture
        case Some(otherProcessor) =>
          logger.info(s"Stopping $otherProcessor before starting reinitialization")
          for {
            _ <- stopProcessorIgnoringShutdown(otherProcessor)
            rdp = digestProcessorFactory.createReinitializingDigestProcessor(synchronizerId)
            _ = currentProcessorRef.set(Some(rdp))
            _ <- rdp.start()
          } yield ()
      },
      "start reinitialization digest processor",
    )

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
