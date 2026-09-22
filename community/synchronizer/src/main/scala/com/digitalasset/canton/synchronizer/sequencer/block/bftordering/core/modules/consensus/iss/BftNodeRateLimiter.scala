// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.util.RateLimiter

import scala.collection.mutable

class BftNodeRateLimiter(
    maxTasksPerSecond: NonNegativeNumeric[Double],
    maxBurstFactor: PositiveDouble,
    // Elapsed-time source in nanoseconds, injectable for tests.
    //  Defaults to `System.nanoTime()`, which (unlike a wall clock) is monotonic and unaffected by
    //  system time corrections. RateLimiter interprets this as elapsed nanoseconds, so a wall-clock-based
    //  source could go backwards, inflate the token debt, and suppress retransmissions until wall time
    //  catches up, recreating the view-change liveness failure this rate limiter must avoid.
    timeSource: => Long = System.nanoTime(),
) {
  private val rateLimiter: mutable.Map[BftNodeId, RateLimiter] = mutable.Map()

  def checkAndUpdateRate(node: BftNodeId): Boolean =
    rateLimiter
      .getOrElseUpdate(
        node,
        new RateLimiter(
          maxTasksPerSecond,
          maxBurstFactor,
          timeSource,
        ),
      )
      .checkAndUpdateRate()
}
