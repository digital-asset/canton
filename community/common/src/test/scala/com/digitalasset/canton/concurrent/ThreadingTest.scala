// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import cats.syntax.foldable._
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Semaphore, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, blocking}

class ThreadingTest extends AnyWordSpec with BaseTest {

  lazy val expectedNumberOfParallelTasks: Int = Threading.detectNumberOfThreads(logger)
  val expectedNumberOfParallelTasksWrappedInBlocking: Int = 200
  val numberOfTasksToMakeExecutionContextBusy: Int = 200

  val numberOfExtraTasks: Int = 20

  "A new execution context" when {

    "nothing else is happening" must {
      s"provide at least $expectedNumberOfParallelTasks threads" in {
        withTaskRunnerOnNewEc(expectedNumberOfParallelTasks, wrapInBlocking = false) { taskRunner =>
          taskRunner.startTasks()
          taskRunner.assertTasksRunning()
        }
      }

      s"provide at least $expectedNumberOfParallelTasksWrappedInBlocking threads for blocking calls" in {
        withTaskRunnerOnNewEc(
          expectedNumberOfParallelTasksWrappedInBlocking,
          wrapInBlocking = true,
        ) { taskRunner =>
          taskRunner.startTasks()
          taskRunner.assertTasksRunning()
        }
      }
    }

    "global execution context is busy" must {
      def withGlobalEcBusy(body: => Unit): Unit =
        withTaskRunner(
          s"global-$numberOfTasksToMakeExecutionContextBusy-blocking",
          numberOfTasksToMakeExecutionContextBusy,
          wrapInBlocking = true,
          ExecutionContext.global,
        ) { taskRunner =>
          taskRunner.startTasks()
          taskRunner.assertTasksRunning()
          body
        }

      s"provide at least $expectedNumberOfParallelTasks threads" in {
        withGlobalEcBusy {

          withTaskRunnerOnNewEc(expectedNumberOfParallelTasks, wrapInBlocking = false) {
            taskRunner =>
              taskRunner.startTasks()

              taskRunner.assertTasksRunning()
          }
        }
      }

      s"provide at least $expectedNumberOfParallelTasksWrappedInBlocking threads for blocking calls" in {
        withGlobalEcBusy {

          withTaskRunnerOnNewEc(
            expectedNumberOfParallelTasksWrappedInBlocking,
            wrapInBlocking = true,
          ) { taskRunner =>
            taskRunner.startTasks()

            taskRunner.assertTasksRunning()
          }
        }
      }
    }

    "another new execution context is busy" must {

      s"provide at least $expectedNumberOfParallelTasks threads" in {
        withTaskRunnerOnNewEc(numberOfTasksToMakeExecutionContextBusy, wrapInBlocking = true) {
          taskRunner =>
            taskRunner.startTasks()
            taskRunner.assertTasksRunning()

            withTaskRunnerOnNewEc(expectedNumberOfParallelTasks, wrapInBlocking = false) {
              taskRunner =>
                taskRunner.startTasks()

                taskRunner.assertTasksRunning()
            }
        }
      }

      s"provide at least $expectedNumberOfParallelTasksWrappedInBlocking threads for blocking calls" in {
        withTaskRunnerOnNewEc(numberOfTasksToMakeExecutionContextBusy, wrapInBlocking = true) {
          taskRunner =>
            taskRunner.startTasks()
            taskRunner.assertTasksRunning()

            withTaskRunnerOnNewEc(
              expectedNumberOfParallelTasksWrappedInBlocking,
              wrapInBlocking = true,
            ) { taskRunner =>
              taskRunner.startTasks()

              taskRunner.assertTasksRunning()
            }
        }
      }
    }

    def withTaskRunnerOnNewEc(numberOfTasksToRun: Int, wrapInBlocking: Boolean)(
        body: TaskRunner => Unit
    ): Unit =
      withNewExecutionContext { ec =>
        val description =
          if (wrapInBlocking) s"ec-$numberOfTasksToRun-blocking" else s"ec-$numberOfTasksToRun"
        withTaskRunner(description, numberOfTasksToRun, wrapInBlocking, ec)(body)
      }

    def withNewExecutionContext(body: ExecutionContext => Unit): Unit =
      ResourceUtil.withResource(
        ExecutorServiceExtensions(
          Threading.newExecutionContext("threading-test-execution-context", logger)
        )(logger, DefaultProcessingTimeouts.testing)
      ) { case ExecutorServiceExtensions(ec) =>
        body(ec)
      }

    def withTaskRunner(
        description: String,
        numberOfTasksToRun: Int,
        wrapInBlocking: Boolean,
        ec: ExecutionContext,
    )(
        body: TaskRunner => Unit
    ): Unit = ResourceUtil.withResource(
      new TaskRunner(description, numberOfTasksToRun, wrapInBlocking)(ec)
    )(body)

    class TaskRunner(
        val description: String,
        val numberOfTasksToRun: Int,
        val wrapInBlocking: Boolean,
    )(implicit
        val ec: ExecutionContext
    ) extends AutoCloseable {

      private val running = new Semaphore(0)
      private val blocker = new Semaphore(0)
      private val closed = new AtomicBoolean(false)

      private val taskFuture: AtomicReference[Option[Future[Unit]]] = new AtomicReference(None)

      def startTasks(): Unit = {
        // Reset semaphores to be on the safe side
        blocker.drainPermits()
        running.drainPermits()

        // Start computation
        val idle = taskFuture.compareAndSet(
          None, {
            val blockingTasks = (0 until numberOfTasksToRun).toList.traverse_ { i =>
              Future {
                logger.debug(s"$description: Starting task $i...")
                if (closed.get()) {
                  logger.warn(s"$description: Task $i started after closing. Aborting...")
                } else {
                  // Only do this, if the runner has not been closed.
                  // So that tasks running after close are not counted.
                  running.release()

                  logger.info(
                    s"$description: Started task $i. (Total: ${running.availablePermits()})\n$ec"
                  )

                  if (wrapInBlocking)
                    blocking {
                      blocker.acquire()
                    }
                  else
                    blocker.acquire()

                  logger.debug(s"$description: Terminated task $i")
                }
              }
            }

            logger.info(s"$description: Starting $numberOfExtraTasks extra tasks...")

            // Run some extra tasks to keep submitting to the fork join pool.
            // This is necessary, because the fork join pool occasionally fails to create a worker thread.
            // It is ok to do so in this test, because there are plenty of extra tasks in production.
            val extraTasks = (0 until numberOfExtraTasks).toList.traverse_ { i =>
              Future { logger.debug(s"$description: Running extra task $i...") }
            }

            Some(for {
              r <- blockingTasks
              _ <- extraTasks
            } yield r)
          },
        )

        // Fail test, if some computation has already been running
        withClue(s"No tasks running by this task runner:") {
          idle shouldEqual true
        }
      }

      def assertTasksRunning(): Unit = {
        val runningTasks =
          if (running.tryAcquire(numberOfTasksToRun, 10, TimeUnit.SECONDS)) numberOfTasksToRun
          else running.availablePermits()

        logger.info(s"$description: Found $runningTasks running tasks.\n$ec")

        withClue(s"Number of tasks running in parallel:") {
          runningTasks shouldEqual numberOfTasksToRun
        }
      }

      override def close(): Unit = {
        logger.info(s"$description: Initiating shutdown...")
        closed.set(true)
        blocker.release(numberOfTasksToRun)
        withClue(s"Tasks properly terminating") {
          taskFuture.get().map(_.futureValue)
        }
        taskFuture.set(None)
      }
    }
  }

  "The parallel ExecutionContext" must {
    "be stack-safe in general" in {
      logger.debug("Entering 'the parallel ExecutionContext should be stack-safe in general'...")

      val parallelExecutionContext =
        Threading.newExecutionContext("threading-test-execution-context", logger)

      def rec(n: Int): Future[Int] = {
        Future
          .successful(n)
          .flatMap(i => if (i > 0) rec(i - 1) else Future.successful(0))(parallelExecutionContext)
      }

      try {
        rec(100000).futureValue
      } finally {
        parallelExecutionContext.shutdown()
      }
    }
  }
}
