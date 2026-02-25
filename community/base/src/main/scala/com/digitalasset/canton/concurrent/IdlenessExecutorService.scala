// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.daml.executors.executors.NamedExecutionContextExecutorService

import java.util.concurrent.*
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

trait IdlenessExecutorService extends ExecutorService {

  /** Waits until all threads in the executor service are idle. The current thread may help in
    * processing submitted tasks. The method may be conservative: it can return false even if all
    * threads are idle at the end of the `duration`.
    *
    * @param timeout
    *   The maximum time to wait. This time may be exceeded up to the run-time of the longest
    *   running task in the pool.
    * @return
    *   true if all threads are idle; false if the timeout elapsed
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

}

abstract class ExecutionContextIdlenessExecutorService(
    delegate: ExecutorService,
    name: String,
    reporter: Throwable => Unit,
) extends NamedExecutionContextExecutorService(delegate, name, reporter)
    with IdlenessExecutorService {

  def queueSize: Long
}

class ForkJoinIdlenessExecutorService(
    pool: ForkJoinPool,
    monitoredPool: ExecutorService,
    reporter: Throwable => Unit,
    name: String,
) extends ExecutionContextIdlenessExecutorService(monitoredPool, name, reporter) {

  override protected[concurrent] def awaitIdlenessOnce(timeout: FiniteDuration): Boolean =
    pool.awaitQuiescence(timeout.toMillis, TimeUnit.MILLISECONDS)

  override def toString: String = s"ForkJoinIdlenessExecutorService-$name: $pool"

  override def queueSize: Long = pool.getQueuedTaskCount
}

class ThreadPoolIdlenessExecutorService(
    pool: ThreadPoolExecutor,
    monitoredPool: ExecutorService,
    reporter: Throwable => Unit,
    name: String,
) extends ExecutionContextIdlenessExecutorService(monitoredPool, name, reporter) {

  override protected[concurrent] def awaitIdlenessOnce(timeout: FiniteDuration): Boolean = {
    val deadline = timeout.fromNow
    val minSleep = 1L
    val maxSleep = Math.max(timeout.toMillis >> 2, minSleep)

    @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
    @tailrec def go(sleep: Long): Boolean =
      if (deadline.isOverdue())
        false
      else if (pool.getQueue.isEmpty && pool.getActiveCount == 0)
        true
      else {
        // Do not use `blocking` because we do not want the execution context to spawn new threads now
        Thread.sleep(sleep)
        go(Math.min(sleep * 2, maxSleep))
      }

    go(minSleep)
  }

  override def queueSize: Long = pool.getQueue.size().toLong
}
