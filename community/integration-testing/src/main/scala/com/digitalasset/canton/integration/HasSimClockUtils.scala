// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Utility methods for integration tests that use the simclock.
  */
trait HasSimClockUtils {
  this: CantonBaseIntegrationTest =>

  /** Run an operation while advancing the simclock until the future is completed. Useful when the
    * underlying operation cannot complete (or is flaky) when using a simclock.
    * @param deltaTime
    *   The amount of time to advance the clock at each iteration
    * @param maxPollInterval
    *   The maximum amount of time between clock advances
    */
  def runAsyncAndAdvanceClockUntilFinished[A](
      f: => A,
      clock: SimClock,
      deltaTime: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfMillis(10),
      maxPollInterval: FiniteDuration = 10.millis,
  )(implicit
      ec: ExecutionContext
  ): A = {
    val future = Future(f)
    eventually(maxPollInterval = maxPollInterval) {
      clock.advance(deltaTime.duration)
      future.isCompleted shouldBe true
    }
    // important to call this so that we don't swallow the exception in case of a failed future
    future.futureValue
  }
}
