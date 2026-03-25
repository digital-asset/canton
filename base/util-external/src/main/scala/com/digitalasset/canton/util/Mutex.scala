// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.util.Mutex.MutexBlocker

import java.lang.Thread.onSpinWait
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinPool.ManagedBlocker
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec

/** Lock to be used instead of blocking synchronized
  *
  * The fork join pool is leaking threads potentially with every invocation of blocking. Therefore,
  * only invoke it if really necessary.
  */
class Mutex {

  private val lock = new ReentrantLock()

  @deprecated("use exclusive, not synchronize", since = "3.4")
  def synchronized[T](body: => T): T = sys.error("use exclusive to distinguish")

  def exclusive[T](f: => T): T =
    // We perform lock inflation to reduce the chance of having to use expensive locks
    // First level: Immediate acquisition
    if (lock.tryLock()) {
      try {
        f
      } finally {
        lock.unlock()
      }
    } else {
      // Second level: Spinning - Trying to avoid parking the thread
      val maxSpins = Mutex.MaxSpins.get()
      @tailrec
      def retry(spins: Int): T =
        if (spins < maxSpins) {
          if (lock.tryLock()) {
            try {
              f
            } finally {
              lock.unlock()
            }
          } else {
            onSpinWait()
            retry(spins + 1)
          }
        } else {
          // 3. "Heavy Lock" (Inflation) - Trigger FJP compensation
          // Only at this point do we notify the ForkJoinPool.
          val blocker = new MutexBlocker(lock)
          ForkJoinPool.managedBlock(blocker)
          try {
            f
          } finally {
            lock.unlock()
          }
        }

      retry(0)
    }
}

object Mutex {
  def apply(): Mutex = new Mutex
  val MaxSpins = new AtomicInteger(3000)
  // Inner class to handle the official FJP integration
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private class MutexBlocker(lock: ReentrantLock) extends ManagedBlocker {
    private var acquired = false

    // FJP calls this to see if it even needs to block/compensate
    override def isReleasable: Boolean = {
      acquired = lock.tryLock()
      acquired
    }

    // FJP calls this if isReleasable returned false.
    // This is where thread compensation (spawning a new thread) happens.
    override def block(): Boolean = {
      if (!acquired) {
        lock.lock() // This is the heavy-weight park
        acquired = true
      }
      true
    }
  }
}
