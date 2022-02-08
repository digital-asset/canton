// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import java.util.concurrent.Semaphore

import scala.util.control.NonFatal

/** Although our integration tests are designed not to conflict with one another when running concurrently,
  * practically due to how resource heavy their setups are you will start encountering problems if we run
  * too many canton environments at once.
  * To prevent this we have a global limit on how many environments can be started concurrently.
  * NOTE: The limit is per class loader, therefore when running both enterprise and community integration tests, each
  * has their own limit as sbt creates a class loader per sub-project.
  * This defaults to 1 but can be overridden using the system property [[ConcurrentEnvironmentLimiter.IntegrationTestConcurrencyLimit]].
  * Integration test setups should create their environments within [[ConcurrentEnvironmentLimiter.create]]
  * and destroy them within [[ConcurrentEnvironmentLimiter.destroy]].
  * A semaphore is used to block creations until a permit is available, and then the permit is released after the environment is destroyed.
  *
  * Due to this approach tests will start but then be blocked until a permit is available. In some cases for many minutes.
  * This may well activate a slow poke notification in ScalaTest, however these are left in place to support discovery
  * of any tests that fail to halt entirely or run for an abnormally long amount of time.
  */
object ConcurrentEnvironmentLimiter {
  val IntegrationTestConcurrencyLimit = "canton-test.integration.concurrency"

  private val concurrencyLimit: Int = System.getProperty(IntegrationTestConcurrencyLimit, "1").toInt

  /** Configured to be fair so earlier started tests will be first to get environments */
  private val semaphore = new Semaphore(concurrencyLimit, true)

  /** Block an environment creation until a permit is available. */
  def create[A](block: => A): A = {
    scala.concurrent.blocking {
      semaphore.acquire()
    }
    try block
    catch {
      // creations can easily fail and throw
      // capture these and immediately release the permit as the destroy method will not be called
      case NonFatal(e) =>
        semaphore.release()
        throw e
    }
  }

  /** Attempt to destroy an environment and ensure that the permit is released */
  def destroy[A](block: => A): A =
    try block
    finally semaphore.release()
}
