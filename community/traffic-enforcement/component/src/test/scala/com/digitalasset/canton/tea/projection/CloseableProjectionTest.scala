// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, DispatcherSelector}
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.Future
import scala.concurrent.duration.*

class CloseableProjectionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val testKit: ActorTestKit = ActorTestKit(getClass.getSimpleName)
  private implicit val system: ActorSystem[Nothing] = testKit.system

  override def afterAll(): Unit =
    try testKit.shutdownTestKit()
    finally super.afterAll()

  private val projectionId: ProjectionId = ProjectionId("debit-ingestion", "grpc-stream")

  /** A stand-in for a real projection actor. It forwards every command it receives to `observer`
    * (so the tests can assert on the interaction) and shuts itself down when asked to
    * [[ProjectionBehavior.Stop]], mimicking how a real projection winds down.
    */
  private def stoppableProjection(
      observer: ActorRef[ProjectionBehavior.Command]
  ): Behavior[ProjectionBehavior.Command] =
    Behaviors.receiveMessage { command =>
      observer ! command
      command match {
        case ProjectionBehavior.Stop => Behaviors.stopped
        case _ => Behaviors.same
      }
    }

  private def closeableProjection(
      projectionRef: ActorRef[ProjectionBehavior.Command]
  ): CloseableProjection =
    new CloseableProjection(projectionId, projectionRef, loggerFactory, timeouts)

  "CloseableProjection" should {

    "stop the underlying projection and wait until it has terminated" in {
      val commands = testKit.createTestProbe[ProjectionBehavior.Command]()
      val deathWatch = testKit.createTestProbe()
      val projectionRef = testKit.spawn(stoppableProjection(commands.ref))

      val closeable = closeableProjection(projectionRef)

      // close() blocks until the projection has cleanly wound down.
      closeable.close()

      commands.expectMessage(ProjectionBehavior.Stop)
      deathWatch.expectTerminated(projectionRef, 10.seconds)
    }

    "complete the close even if the projection has already terminated" in {
      val commands = testKit.createTestProbe[ProjectionBehavior.Command]()
      val projectionRef = testKit.spawn(stoppableProjection(commands.ref))

      // Kill the projection up-front: closing must not block waiting for a death that has already
      // happened (the watcher sees the already-dead actor and completes immediately).
      testKit.stop(projectionRef)

      val closeable = closeableProjection(projectionRef)
      closeable.close()

      succeed
    }

    "be idempotent across repeated close calls" in {
      val commands = testKit.createTestProbe[ProjectionBehavior.Command]()
      val projectionRef = testKit.spawn(stoppableProjection(commands.ref))

      val closeable = closeableProjection(projectionRef)
      closeable.close()
      // A second close is a no-op: it neither throws nor sends another Stop to the projection.
      closeable.close()

      commands.expectMessage(ProjectionBehavior.Stop)
      commands.expectNoMessage()
    }

    "complete the close while the projection is still processing a message" in {
      val processingStarted = new CountDownLatch(1)
      val allowCompletion = new CountDownLatch(1)

      // A projection that is busy the moment it starts: it blocks until `allowCompletion` is
      // released, modelling an in-flight message still being processed when close() races in. The
      // Stop sent by close() therefore lands in the mailbox behind that in-flight work.
      val busyProjection: Behavior[ProjectionBehavior.Command] =
        Behaviors.setup { _ =>
          processingStarted.countDown()
          allowCompletion.await()
          Behaviors.receiveMessage {
            case ProjectionBehavior.Stop => Behaviors.stopped
            case _ => Behaviors.same
          }
        }

      // Run the blocking projection on the blocking dispatcher so it cannot starve the watcher.
      val projectionRef = testKit.spawn(busyProjection, DispatcherSelector.blocking())
      assert(processingStarted.await(10, TimeUnit.SECONDS), "projection did not start processing")

      val closeable = closeableProjection(projectionRef)

      // close() blocks, so run it on a separate thread to observe that it is held up while the
      // projection is still busy.
      val closed = Future(closeable.close())

      // While the projection is processing, the queued Stop cannot be honored, so the close must
      // not complete yet.
      always(durationOfSuccess = 500.millis) {
        closed.isCompleted shouldBe false
      }

      // Let the in-flight processing finish: the projection now honors the Stop and terminates, so
      // the close completes.
      allowCompletion.countDown()
      closed.futureValue
    }
  }
}
