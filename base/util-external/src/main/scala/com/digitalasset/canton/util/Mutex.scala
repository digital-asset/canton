// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.util.Mutex.MutexBlocker

import java.lang.Thread.onSpinWait
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinPool.ManagedBlocker
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

/** Lock to be used instead of blocking synchronized
  *
  * The fork join pool is leaking threads potentially with every invocation of blocking. Therefore,
  * only invoke it if really necessary.
  */
class Mutex {

  private val lock = new ReentrantLock()

  @deprecated("use exclusive, not synchronize", since = "3.4")
  def synchronized[T](body: => T): T = sys.error("use exclusive to distinguish")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.While",
      "org.wartremover.warts.Return",
      "org.wartremover.warts.Var",
    )
  )
  def exclusive[T](f: => T): T = {
    // We perform lock inflation to reduce the chance of having to use expensive locks
    // First level: Immediate acquisition
    if (lock.tryLock()) {
      try {
        return f
      } finally {
        lock.unlock()
      }
    }

    // Second level: Spinning - Trying to avoid parking the thread
    var spins = 0
    val MaxSpins = Mutex.MaxSpins.get()
    while (spins < MaxSpins) {
      if (lock.tryLock()) {
        try {
          return f
        } finally {
          lock.unlock()
        }
      }
      onSpinWait()
      spins += 1
    }

    // 3. "Heavy Lock" (Inflation) - Trigger FJP compensation
    // Only at this point do we notify the ForkJoinPool.
    val blocker = new MutexBlocker(lock)
    ForkJoinPool.managedBlock(blocker)

    // Once managedBlock returns, we are guaranteed to have the lock
    try {
      f
    } finally {
      lock.unlock()
    }
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
