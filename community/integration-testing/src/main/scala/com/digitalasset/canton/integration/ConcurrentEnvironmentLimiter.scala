// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.util.Mutex
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.{Comparator, PriorityQueue}
import scala.annotation.tailrec

/** Although our integration tests are designed not to conflict with one another when running
  * concurrently, practically due to how resource heavy their setups are you will start encountering
  * problems if we run too many canton environments at once. To prevent this we have a global limit
  * on how many environments can be started concurrently. NOTE: The limit is per class loader,
  * therefore when running both enterprise and community integration tests, each has their own limit
  * as sbt creates a class loader per sub-project. This defaults to 3 but can be overridden using
  * the system property [[ConcurrentEnvironmentLimiter.IntegrationTestConcurrencyLimit]].
  * Integration test setups should create their environments within
  * [[ConcurrentEnvironmentLimiter.create]] and destroy them within
  * [[ConcurrentEnvironmentLimiter.destroy]]. A semaphore is used to block creations until a permit
  * is available, and then the permit is released after the environment is destroyed.
  *
  * Due to this approach tests will start but then be blocked until a permit is available. In some
  * cases for many minutes. This may well activate a slow poke notification in ScalaTest, however
  * these are left in place to support discovery of any tests that fail to halt entirely or run for
  * an abnormally long amount of time.
  */
object ConcurrentEnvironmentLimiter extends LazyLogging {

  private sealed trait State
  private object New extends State {
    override def toString: String = "New"
  }
  private object Queued extends State {
    override def toString: String = "Queued"
  }
  private object Running extends State {
    override def toString: String = "Running"
  }
  private object Failed extends State {
    override def toString: String = "Failed"
  }
  private object Done extends State {
    override def toString: String = "Done"
  }

  val IntegrationTestConcurrencyLimit = "canton-test.integration.concurrency"

  private val concurrencyLimit: Int = System.getProperty(IntegrationTestConcurrencyLimit, "2").toInt

  private final class Waiter(val name: String, val permits: Int, val seq: Int)

  /** Higher priority first */
  private val waiterOrdering: Comparator[Waiter] = (a: Waiter, b: Waiter) => {
    val ret = Integer.compare(b.permits, a.permits)
    if (ret == 0) Integer.compare(a.seq, b.seq) else ret
  }

  /** Configured to be fair so earlier started tests will be first to get environments */
  private val semaphore = new Semaphore(concurrencyLimit, true)
  private val waiters = new PriorityQueue[Waiter](waiterOrdering)
  private val lock = new Mutex()
  private val seqCounter = new AtomicInteger(0)

  /** contains a map from test name to state (queue, run) */
  private val active = new AtomicReference[Map[String, (State, Long)]](Map.empty)

  private def change(name: String, state: State, purge: Boolean = false): Unit = {
    val now = System.nanoTime()
    val current = active
      .getAndUpdate { cur =>
        if (purge) cur.removed(name)
        else cur.updated(name, (state, now))
      }
    val (currentState, currentTime) = current.getOrElse(name, (New, now))
    val currentSet = current.keySet
    val numSet = if (purge) (currentSet - name) else (currentSet + name)
    logger.debug(s"$name: $currentState => $state after ${TimeUnit.NANOSECONDS
        .toSeconds(now - currentTime)}s (${numSet.size} pending)")
  }

  private def getNumPermits(permits: PositiveInt): Int =
    Math.max(1, Math.min(permits.value, concurrencyLimit))

  /** Block an environment creation until a permit is available. */
  def create[A](name: String, permits: PositiveInt)(block: => A): A = {
    val waiter = new Waiter(name, permits.value, seqCounter.getAndIncrement())
    lock.exclusive {
      waiters.add(waiter)
    }
    change(name, Queued)
    val numPermits = getNumPermits(permits)
    @tailrec
    def acquireUntilHead(): Unit = {
      semaphore.acquire(numPermits)
      // if this waiter is not at the head of the queue, release the permit again
      // and retry
      lock.exclusive(
        Option.when(!waiters.isEmpty && waiters.peek() == waiter)(waiters.poll())
      ) match {
        case Some(head) =>
          if (head != waiter)
            logger.error("This should never happen: head of the queue is not the current waiter")
        case None =>
          // not at head of the queue, release the permit again and retry
          semaphore.release(numPermits)
          Thread.sleep(100)
          acquireUntilHead()
      }
    }
    scala.concurrent.blocking {
      acquireUntilHead()
    }
    change(name, Running)
    try block
    catch {
      // Creations can easily fail and throw.
      // Capture these and immediately release the permit as the destroy method will not be called.
      // This must also cover fatal errors such as a NoClassDefFoundError thrown while building the
      // environment definition: otherwise the permit leaks and every later environment creation
      // blocks forever on the semaphore, which shows up in CI as an sbt timeout.
      case e: Throwable =>
        change(name, Failed, purge = true)
        semaphore.release(numPermits)
        throw e
    }
  }

  /** Attempt to destroy an environment and ensure that the permit is released */
  def destroy[A](name: String, permits: PositiveInt)(block: => A): A =
    try block
    finally {
      change(name, Done, purge = true)
      semaphore.release(getNumPermits(permits))
    }
}
