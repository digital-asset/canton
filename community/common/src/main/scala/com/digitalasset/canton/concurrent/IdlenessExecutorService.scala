// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import java.util.concurrent.*
import scala.annotation.tailrec
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.FiniteDuration

trait IdlenessExecutorService extends ExecutorService {

  /** Waits until all threads in the executor service are idle.
    * The current thread may help in processing submitted tasks.
    * The method may be conservative: it can return false even
    * if all threads are idle at the end of the `duration`.
    *
    * @param timeout The maximum time to wait.
    *                This time may be exceeded up to the run-time of the longest running task in the pool.
    * @return true if all threads are idle; false if the timeout elapsed
    */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.While",
      "com.digitalasset.canton.RequireBlocking",
    )
  )
  def awaitIdleness(timeout: FiniteDuration): Boolean = {
    // Check whether this is idle for 5 consecutive times.
    // We check several times, as awaitIdlenessOnce may incorrectly indicate idleness.
    val deadline = timeout.fromNow
    var idleCount = 0
    var remainingTime = deadline.timeLeft
    while (remainingTime.toMillis > 0 && idleCount < 5) {
      // Do not use `blocking` because we do not want the execution context to spawn new threads now
      Thread.sleep(1L)
      if (awaitIdlenessOnce(remainingTime))
        idleCount += 1
      else
        idleCount = 0
      remainingTime = deadline.timeLeft
    }
    idleCount == 5
  }

  protected[concurrent] def awaitIdlenessOnce(timeout: FiniteDuration): Boolean

  /** Return the thread pool's queue size. */
  def queueSize: Int
}

trait ExecutionContextIdlenessExecutorService
    extends ExecutionContextExecutorService
    with IdlenessExecutorService {
  def name: String
}

abstract class ExecutorServiceDelegate(val executorService: ExecutorService)
    extends ExecutionContextExecutorService {
  override def shutdown(): Unit = executorService.shutdown()
  override def shutdownNow(): java.util.List[Runnable] = executorService.shutdownNow()
  override def isShutdown: Boolean = executorService.isShutdown
  override def isTerminated: Boolean = executorService.isTerminated
  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    executorService.awaitTermination(timeout, unit)
  override def submit[T](task: Callable[T]): Future[T] = executorService.submit(task)
  override def submit[T](task: Runnable, result: T): Future[T] =
    executorService.submit(task, result)
  override def submit(task: Runnable): Future[_] = executorService.submit(task)
  override def invokeAll[T](
      tasks: java.util.Collection[_ <: Callable[T]]
  ): java.util.List[Future[T]] =
    executorService.invokeAll(tasks)
  override def invokeAll[T](
      tasks: java.util.Collection[_ <: Callable[T]],
      timeout: Long,
      unit: TimeUnit,
  ): java.util.List[Future[T]] = executorService.invokeAll(tasks, timeout, unit)
  override def invokeAny[T](tasks: java.util.Collection[_ <: Callable[T]]): T =
    executorService.invokeAny(tasks)
  override def invokeAny[T](
      tasks: java.util.Collection[_ <: Callable[T]],
      timeout: Long,
      unit: TimeUnit,
  ): T =
    executorService.invokeAny(tasks, timeout, unit)
  override def execute(command: Runnable): Unit = executorService.execute(command)
}

class ForkJoinIdlenessExecutorService(
    pool: ForkJoinPool,
    reporter: Throwable => Unit,
    override val name: String,
) extends ExecutorServiceDelegate(pool)
    with ExecutionContextIdlenessExecutorService {
  override def reportFailure(cause: Throwable): Unit = reporter(cause)

  override protected[concurrent] def awaitIdlenessOnce(timeout: FiniteDuration): Boolean = {
    pool.awaitQuiescence(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

  override def queueSize: Int = pool.getQueuedSubmissionCount

  override def toString: String = s"ForkJoinIdlenessExecutorService-$name: $pool"
}

class ThreadPoolIdlenessExecutorService(
    pool: ThreadPoolExecutor,
    reporter: Throwable => Unit,
    override val name: String,
) extends ExecutorServiceDelegate(pool)
    with ExecutionContextIdlenessExecutorService {
  override def reportFailure(cause: Throwable): Unit = reporter(cause)

  override def queueSize: Int = pool.getQueue.size

  override protected[concurrent] def awaitIdlenessOnce(timeout: FiniteDuration): Boolean = {
    val deadline = timeout.fromNow
    val minSleep = 1L
    val maxSleep = Math.max(timeout.toMillis >> 2, minSleep)

    @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
    @tailrec def go(sleep: Long): Boolean = {
      if (deadline.isOverdue())
        false
      else if (pool.getQueue.isEmpty && pool.getActiveCount == 0)
        true
      else {
        // Do not use `blocking` because we do not want the execution context to spawn new threads now
        Thread.sleep(sleep)
        go(Math.min(sleep * 2, maxSleep))
      }
    }

    go(minSleep)
  }
}
