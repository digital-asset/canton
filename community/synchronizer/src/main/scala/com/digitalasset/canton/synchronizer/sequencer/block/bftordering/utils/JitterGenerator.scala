// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.util.retry.Jitter

import scala.concurrent.duration.FiniteDuration

/** A generator of jittered delays for retries, based on the number of attempts. The delay is
  * calculated using the provided `Jitter` implementation, and is at least `minimumDelay`. The
  * `initialDelay` is used as the starting point for the first attempt.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class JitterGenerator(
    jitter: Jitter,
    initialDelay: FiniteDuration,
    minimumDelay: FiniteDuration,
) {
  private var lastDelay: FiniteDuration = initialDelay
  private var lastAttempt: Int = 0

  def next(attempt: Int): FiniteDuration = {
    require(attempt > 0, s"Attempt number must be positive, got $attempt")
    require(
      attempt > lastAttempt,
      s"Attempt number must be greater than last attempt ($lastAttempt), got $attempt",
    )
    lastDelay = jitter(initialDelay, lastDelay, attempt)
    lastAttempt = attempt
    lastDelay.plus(minimumDelay)
  }
}
