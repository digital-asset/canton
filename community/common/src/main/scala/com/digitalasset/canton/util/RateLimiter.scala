// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._

/** Utility class that allows clients to keep track of a rate limit.
  * Clients need to tell an instance whenever they intend to start a new task.
  * The instance will inform the client whether the task can be executed while still meeting the rate limit.
  *
  * Guarantees:
  * <ul>
  * <li>Maximum burst size: if `checkAndUpdateRate` is called `n` times in parallel, at most `max(2, maxTasksPerSecond / 5)` calls may return `true`.
  *     The upper bound mainly comes from the assumption that `System.nanoTime` has a resolution of `100.millis`.</li>
  * <li>Average rate: if `checkAndUpdateRate` is called at a rate of at least `maxTasksPerSecond` during `n` seconds,
  *     then the number of calls that return `true` divided by `n` is roughly `maxTasksPerSecond` when `n` goes towards infinity.
  *     See the unit tests to get a better understanding of the term "roughly".
  * </ul>
  *
  * @param maxTasksPerSecond the maximum number of tasks per second
  */
class RateLimiter(val maxTasksPerSecond: NonNegativeInt) {

  private val maxTasksPerSecondLong: Long = maxTasksPerSecond.unwrap.toLong

  // Cycles are at least 100 millis, because otherwise they may be below the resolution of `System.nanoTime`.
  // In that case, we would enforce a much lower rate than requested.
  private val (cycleDuration, maxTasksPerCycle): (FiniteDuration, Long) =
    if (maxTasksPerSecondLong == 0) (1.day, 0)
    else if (maxTasksPerSecondLong >= 10) (100.millis, maxTasksPerSecondLong / 10)
    else (1.second / maxTasksPerSecondLong, 1)

  // Upper bound on a burst: Worst case scenario:
  // - A new instance is created. This starts a new cycle.
  // - maxTasksPerCycle tasks land just before the end of the cycle.
  // - Another maxTasksPerCycle tasks land at the beginning of the next cycle.
  // - Total: 2 * maxTasksPerCycle <= 2 * max(1, maxTasksPerSecond / 10) <= max(2, maxTasksPerSecond / 5)
  //
  // Note that the upper bound is mainly driven by the lower bound of 100ms on the cycleDuration.

  // Remark: we could lower the upper bound on bursts from `max(maxTasksPerSecond / 5, max 2)` to `max(maxTasksPerSecond / 10, max 1)`
  // by storing all tasks in a queue. We don't do that because the improvement seems not to justify the overhead of having a queue.

  /** First component is the number of tasks in the current cycle.
    * Second component is the end point of the current cycle.
    */
  private val state: AtomicReference[(Long, Deadline)] = new AtomicReference(
    (0, cycleDuration.fromNow)
  )

  /** Call this before starting a new task.
    * @return whether the tasks can be executed while still meeting the rate limit
    */
  final def checkAndUpdateRate(): Boolean = {
    val now = Deadline.now

    val (oldCount, oldNextCycle) = state.getAndUpdate { case st @ (count, nextCycle) =>
      if (count < maxTasksPerCycle)
        (count + 1, nextCycle)
      else if (nextCycle <= now)
        (1, cycleDuration.fromNow)
      else
        st
    }

    oldCount < maxTasksPerCycle || oldNextCycle <= now
  }
}
