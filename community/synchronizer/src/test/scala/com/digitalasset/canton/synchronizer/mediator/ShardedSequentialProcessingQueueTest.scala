// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.util.{
  ContinueAfterFailure,
  GarbageCollectedShardedSequentialProcessingQueue,
  LoggerUtil,
  NonGarbageCollectedShardedSequentialProcessingQueue,
  ShardedSequentialProcessingQueue,
  StopAfterFailure,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.*

sealed trait ShardedSequentialProcessingQueueTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {
  protected def newQueue[Ident: Pretty](): ShardedSequentialProcessingQueue[Ident]

  import com.digitalasset.canton.logging.pretty.PrettyInstances.*

  protected implicit val pString: Pretty[String] = prettyString

  protected def processingQueue(): Unit = {
    "execute actions for the same ID sequentially" in {
      val queue = newQueue[String]()
      val id = "request-1"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCanComplete = PromiseUnlessShutdown.unsupervised[Unit]()
      val secondActionStarted = new AtomicBoolean(false)

      // Enqueue first action
      val f1 = queue.executeUS(id)(
        {
          firstActionStarted.outcome_(())
          firstActionCanComplete.futureUS
        },
        "task",
      )

      // Ensure first action has started
      firstActionStarted.futureUS.flatMap { _ =>
        // Enqueue second action
        val f2 = queue.executeUS(id)(
          {
            secondActionStarted.set(true)
            FutureUnlessShutdown.unit
          },
          "task",
        )

        // Verify second action hasn't started yet because f1 is pending
        secondActionStarted.get() shouldBe false

        // Complete the first action
        firstActionCanComplete.outcome_(())

        for {
          _ <- f1
          _ <- f2
        } yield {
          secondActionStarted.get() shouldBe true
          queue.isQueueEmpty(id) shouldBe true
        }
      }.futureValueUS
    }

    "execute actions for different IDs concurrently" in {
      val queue = newQueue[String]()
      val id1 = "id-1"
      val id2 = "id-2"

      val id1Started = PromiseUnlessShutdown.unsupervised[Unit]()
      val id1Blocked = PromiseUnlessShutdown.unsupervised[Unit]()

      // Start action for ID 1 (blocked)
      val f1 = queue.executeUS(id1)(
        {
          id1Started.outcome_(())
          id1Blocked.futureUS
        },
        "task",
      )

      id1Started.futureUS.futureValueUS
      // Action for ID 2 should be able to run and complete immediately even if ID 1 is blocked
      queue.executeUS(id2)(FutureUnlessShutdown.unit, "task").futureValueUS
      queue.isQueueEmpty(id2) shouldBe true

      // ID 2 completed
      id1Blocked.outcome_(()) // Now release ID 1
      f1.futureValueUS
      queue.isQueueEmpty(id1) shouldBe true
    }

    "not deadlock" in {
      val processingQueue = newQueue[Int]()
      val counter = new AtomicLong(1000)
      val workF = (1 to counter.intValue()).toList.parTraverse { _ =>
        processingQueue.executeUS(0)(
          FutureUnlessShutdown.unit.map(_ => counter.decrementAndGet().discard),
          "task",
        )
      }
      workF.futureValueUS
      counter.get shouldBe 0L
    }
  }
}

final class GarbageCollectedShardedSequentialProcessingQueueTest
    extends ShardedSequentialProcessingQueueTest {

  override protected def newQueue[Ident: Pretty](): ShardedSequentialProcessingQueue[Ident] =
    new GarbageCollectedShardedSequentialProcessingQueue[Ident]()

  "GarbageCollectedShardedSequentialProcessingQueue" should {
    behave like processingQueue()

    "stop processing the queue if a previous action fails" in {
      val queue = new GarbageCollectedShardedSequentialProcessingQueue[String]()
      val id = "error-test"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCompletes = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.executeUS(id)(
        {
          firstActionStarted.outcome_(())
          firstActionCompletes.futureUS
        },
        "task",
      )

      // Wait for the first action to start and then register another action
      firstActionStarted.futureUS.futureValueUS
      val f2executed = new AtomicBoolean(false)
      val f2 = queue.executeUS(id)(
        FutureUnlessShutdown.unit.map(_ => f2executed.set(true)),
        "task",
      )
      val exception = new RuntimeException("Boom!")
      firstActionCompletes.failure(exception)

      f1.failed.futureValueUS shouldBe exception
      // f2 should fail with the same exception
      f1.failed.futureValueUS shouldBe f2.failed.futureValueUS
      // and f2 should not have been run
      f2executed.get() shouldBe false
      // but still the queue should be cleaned up.
      queue.processingQueuePerId.get(id) shouldBe None

    }

    "clean up the map entry only if it's the last future in the chain" in {
      val queue = new GarbageCollectedShardedSequentialProcessingQueue[String]()
      val id = "cleanup-test"

      val p1 = PromiseUnlessShutdown.unsupervised[Unit]()
      val p2 = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.executeUS(id)(p1.futureUS, "task")
      val f2 = queue.executeUS(id)(p2.futureUS, "task")

      // The map should contain the future for f2 (the latest one)
      queue.processingQueuePerId.get(id) should not be empty

      p1.outcome_(())
      f1.futureValueUS
      // After f1 completes, the map should STILL contain an entry because f2 is pending
      queue.processingQueuePerId.get(id) should not be empty

      p2.outcome_(())
      f2.futureValueUS
      // After f2 completes, the map should be empty
      queue.processingQueuePerId.get(id) shouldBe None
    }

    "scale linearly with a large number of unique IDs" in {
      val queue = new GarbageCollectedShardedSequentialProcessingQueue[Int]
      val numUniqueIds = 10000
      val actionsPerId = 5

      val startTime = System.nanoTime()

      // Dispatch 50,000 total actions across 10,000 unique IDs
      val allTasks = (1 to numUniqueIds).toList.parTraverse_ { id =>
        (1 to actionsPerId).toList.parTraverse_ { _ =>
          queue.executeUS(id)(FutureUnlessShutdown.unit, "task")
        }
      }

      allTasks.futureValueUS
      val duration = (System.nanoTime() - startTime).nanos

      // Basic validation: All map entries should be cleared
      queue.processingQueuePerId shouldBe empty

      // Optional: Log performance metrics
      logger.info(
        s"Processed ${numUniqueIds * actionsPerId} tasks across $numUniqueIds IDs in ${LoggerUtil
            .roundDurationForHumans(duration)}"
      )

      // Ensure it doesn't take an unreasonable amount of time (e.g., > 10s for 50k simple tasks)
      // On a Macbook Pro M2 Max, this takes <1 s
      duration should be < 10.seconds
    }
  }

}

final class NonGarbageCollectedShardedSequentialProcessingQueueTest
    extends ShardedSequentialProcessingQueueTest {

  override protected def newQueue[Ident: Pretty](): ShardedSequentialProcessingQueue[Ident] =
    new NonGarbageCollectedShardedSequentialProcessingQueue[Ident](
      name = "test-queue",
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      logTaskTiming = false,
      failureMode = StopAfterFailure,
    )

  "ShardedSequentialProcessingQueue" should {
    behave like processingQueue()

    "stop processing the queue if a previous action fails" in {
      val queue = newQueue[String]()
      val id = "error-test"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCompletes = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.executeUS(id)(
        {
          firstActionStarted.outcome_(())
          firstActionCompletes.futureUS
        },
        "first task",
      )

      // Wait for the first action to start and then register another action
      firstActionStarted.futureUS.futureValueUS
      val f2executed = new AtomicBoolean(false)
      val f2 = queue.executeUS(id)(
        FutureUnlessShutdown.unit.map(_ => f2executed.set(true)),
        "second task",
      )

      loggerFactory.assertLogs(
        {
          val exception = new RuntimeException("Boom!")
          firstActionCompletes.failure(exception)

          f1.failed.futureValueUS shouldBe exception
          // f2 should fail with the same exception
          f1.failed.futureValueUS shouldBe f2.failed.futureValueUS
          // and f2 should not have been run
          f2executed.get() shouldBe false
        },
        _.errorMessage shouldBe "Task 'second task' will not run because of failure of previous task",
      )

    }

    "allow to continue processing the queue if a previous action fails" in {
      val queue = new NonGarbageCollectedShardedSequentialProcessingQueue[String](
        name = "test-queue",
        futureSupervisor = FutureSupervisor.Noop,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
        logTaskTiming = false,
        failureMode = ContinueAfterFailure,
      )
      val id = "error-test"

      val firstActionStarted = PromiseUnlessShutdown.unsupervised[Unit]()
      val firstActionCompletes = PromiseUnlessShutdown.unsupervised[Unit]()

      val f1 = queue.executeUS(id)(
        {
          firstActionStarted.outcome_(())
          firstActionCompletes.futureUS
        },
        "first task",
      )

      // Wait for the first action to start and then register another action
      firstActionStarted.futureUS.futureValueUS
      val f2executed = new AtomicBoolean(false)
      val f2 = queue.executeUS(id)(
        FutureUnlessShutdown.unit.map(_ => f2executed.set(true)),
        "second task",
      )

      val exception = new RuntimeException("Boom!")
      firstActionCompletes.failure(exception)

      f1.failed.futureValueUS shouldBe exception
      f2.futureValueUS
      f2executed.get() shouldBe true
    }
  }
}
