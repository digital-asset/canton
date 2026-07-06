// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import cats.Eval
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{AsyncCloseable, FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Terminated}
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}

import scala.concurrent.{Future, Promise}

/** Wrapper class that can cleanly close a projection
  */
class CloseableProjection(
    projectionId: ProjectionId,
    projectionRef: ActorRef[ProjectionBehavior.Command],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit system: ActorSystem[?])
    extends NamedLogging
    with FlagCloseable {

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*

    LifeCycle.close(
      AsyncCloseable(s"projection-$projectionId", stopAndAwait.value, timeouts.closing)
    )(logger)
  }

  /** Sends a Stop signal to a projection actor and returns a Future that completes when the
    * projection has cleanly wound down and terminated. Wrap into Eval.later so it gets memoized
    * even if onClosed is called multiple times
    */
  private def stopAndAwait: Eval[Future[Done]] = Eval.later {
    val promise = Promise[Done]()

    // Spawn a tiny, ephemeral watcher actor whose sole purpose is to monitor the death of the projection
    system
      .systemActorOf[Nothing](
        Behaviors.setup[Nothing] { context =>
          context.watch(projectionRef)

          // Send the graceful stop signal
          projectionRef ! ProjectionBehavior.Stop

          Behaviors.receiveSignal[Nothing] { case (_, Terminated(`projectionRef`)) =>
            promise.trySuccess(Done).discard
            Behaviors.stopped
          }
        },
        s"projection-shutdown-watcher-for-${projectionId.name}-${java.util.UUID.randomUUID()}",
      )
      .discard

    promise.future
  }
}
