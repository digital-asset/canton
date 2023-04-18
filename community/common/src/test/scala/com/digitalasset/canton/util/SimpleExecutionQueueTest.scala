// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}

class SimpleExecutionQueueTest extends AsyncWordSpec with BaseTest {

  class MockTask(name: String) {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    val started = new AtomicBoolean(false)
    private val promise: Promise[String] = Promise[String]()

    def run(): Future[String] = {
      started.set(true)
      promise.future
    }

    def complete(): Unit = promise.success(name)

    def fail(): Unit = promise.failure(new RuntimeException(s"mocked failure for $name"))
  }

  def simpleExecutionQueueTests(mk: () => SimpleExecutionQueue): Unit = {
    "only run one future at a time" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task1Result = queue.execute(task1.run(), "Task1")
      val task2Result = queue.execute(task2.run(), "Task2")

      task2.started.get() should be(false)
      task1.complete()

      for {
        _ <- task1Result
        _ = task1.started.get() shouldBe true
        // queue one while running
        task3Result = queue.execute(task3.run(), "Task3")
        // check that if a task fails subsequent tasks will still be run
        _ = task2.fail()
        _ <- task2Result.failed
        _ = task2.started.get() shouldBe true
        _ = task3.complete()
        _ <- task3Result
      } yield task3.started.get() should be(true)
    }

    "not run a future in case of a previous failure" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task4 = new MockTask("task4")
      val task1Result = queue.execute(task1.run(), "Task1")
      val task2Result = queue.executeUnlessFailed(task2.run(), "Task2")
      val task3Result = queue.execute(task3.run(), "Task3")
      val task4Result = queue.executeUnlessFailed(task4.run(), "Task4")

      task1.fail()
      task3.complete()
      for {
        task2Res <- task2Result.failed
        _ = task2.started.get() shouldBe false
        _ = task2Res.getMessage shouldBe "mocked failure for task1"
        task4Res <- task4Result.failed
        _ = task4.started.get() shouldBe false
        _ = task4Res.getMessage shouldBe "mocked failure for task1"
        task1Res <- task1Result.failed
        _ = task1Res.getMessage shouldBe "mocked failure for task1"
        task3Res <- task3Result
      } yield {
        task3Res shouldBe "task3"
      }
    }

    "correctly propagate failures" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task1Result = queue.executeUnlessFailed(task1.run(), "Task1")
      val task2Result = queue.execute(task2.run(), "Task2")
      val task3Result = queue.executeUnlessFailed(task3.run(), "Task3")

      task1.fail()
      task2.complete()
      for {
        task1Res <- task1Result.failed
        task2Res <- task2Result
        task3Res <- task3Result.failed
      } yield {
        task1Res.getMessage shouldBe "mocked failure for task1"
        task2Res shouldBe "task2"
        task3Res.getMessage shouldBe "mocked failure for task1"
      }
    }

    "flush never fails" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task1Result = queue.execute(task1.run(), "Task1")

      val flush0 = queue.flush()
      flush0.isCompleted shouldBe false
      task1.fail()
      for {
        _ <- task1Result.failed
        _ <- queue.flush()
      } yield {
        flush0.isCompleted shouldBe true
      }
    }

    "list the outstanding tasks" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task4 = new MockTask("task4")

      val task1Result = queue.executeUnlessFailed(task1.run(), "Task1")
      val task2Result = queue.execute(task2.run(), "Task2")
      val task3Result = queue.executeUnlessFailed(task3.run(), "Task3")
      val task4Result = queue.execute(task4.run(), "Task4")

      queue.queued shouldBe Seq("sentinel (completed)", "Task1", "Task2", "Task3", "Task4")
      task1.fail()
      for {
        _ <- task1Result.failed
        queue1 = queue.queued
        _ = task2.complete()
        _ <- task2Result
        _ <- task3Result.failed
        queue3 = queue.queued
        _ = task4.complete()
        _ <- task4Result
        queue4 = queue.queued
      } yield {
        queue1 shouldBe Seq("Task1 (completed)", "Task2", "Task3", "Task4")
        queue3 shouldBe Seq("Task3 (completed)", "Task4")
        queue4 shouldBe Seq("Task4 (completed)")
      }

    }
  }

  "SimpleExecutionQueue" when {
    "not logging task timing" should {
      behave like simpleExecutionQueueTests(() =>
        new SimpleExecutionQueue(
          logTaskTiming = false
        )
      )
    }

    "logging task timing" should {
      behave like simpleExecutionQueueTests(() =>
        new SimpleExecutionQueue(
          logTaskTiming = true
        )
      )
    }
  }
}
