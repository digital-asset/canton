// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{AsyncCloseable, FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Terminated}
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}

import scala.concurrent.Promise

/** Wrapper class that cleanly closes a projection and reports if it dies while still expected to
  * run.
  */
class CloseableProjection(
    projectionId: ProjectionId,
    projectionRef: ActorRef[ProjectionBehavior.Command],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit system: ActorSystem[?])
    extends NamedLogging
    with FlagCloseable {

  private val terminated = Promise[Done]()

  // Watch from construction: the projection can die before anyone tries to close it.
  watchProjectionTermination()

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*

    projectionRef ! ProjectionBehavior.Stop
    LifeCycle.close(
      AsyncCloseable(s"projection-$projectionId", terminated.future, timeouts.closing)
    )(logger)
  }

  /** Watches the projection actor for this wrapper's whole lifetime and completes `terminated` when
    * the projection has wound down and terminated.
    */
  private def watchProjectionTermination(): Unit =
    system
      .systemActorOf[Nothing](
        Behaviors.setup[Nothing] { context =>
          context.watch(projectionRef)
          Behaviors.receiveSignal[Nothing] { case (_, Terminated(`projectionRef`)) =>
            if (!isClosing) {
              noTracingLogger.error(
                s"Projection $projectionId terminated unexpectedly and stopped processing events"
              )
            }
            terminated.trySuccess(Done).discard
            Behaviors.stopped
          }
        },
        s"projection-watcher-for-${projectionId.name}-${java.util.UUID.randomUUID()}",
      )
      .discard
}
