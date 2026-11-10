// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration, SimClock}
import com.digitalasset.canton.tracing.TraceContext

import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*

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

object HasSimClockUtils {

  /** Advances to exactly `timestamp` in bounded steps, running `afterStep` after each advance so
    * callers can wait for nodes to observe time before advancing again. Does nothing if the target
    * has already been reached.
    *
    * @param maxStep
    *   The maximum duration of each advance, at least [[CantonTimestamp.TimeGranularity]]. The
    *   final step is shortened if necessary to reach `timestamp` exactly.
    * @param afterStep
    *   Runs synchronously after each advance, before the next step.
    */
  @tailrec
  def advanceClockInStepsTo(
      clock: SimClock,
      timestamp: CantonTimestamp,
      maxStep: PositiveFiniteDuration,
  )(afterStep: => Unit)(implicit traceContext: TraceContext): Unit = {
    require(
      maxStep.duration.compareTo(CantonTimestamp.TimeGranularity.toJava) >= 0,
      s"maxStep must be at least ${CantonTimestamp.TimeGranularity}",
    )
    if (clock.now < timestamp) {
      clock.advanceTo(clock.now.plus(maxStep.duration).min(timestamp))
      afterStep
      advanceClockInStepsTo(clock, timestamp, maxStep = maxStep)(afterStep)
    }
  }
}
