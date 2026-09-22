// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.logging.ErrorLoggingContext

object Utils {

  /** @param n
    *   needs to be positive
    */
  def largestSmallerOrEqualPowerOfTwo(n: Int): Int =
    Integer.highestOneBit(n)

  def elapsedMillis(started: Long): Long = (System.nanoTime() - started) / 1000000

  def wrapDbQuery[In, Out](
      f: In => Out
  )(
      log: Out => String
  )(implicit errorLoggingContext: ErrorLoggingContext): In => Out = { in =>
    val started = System.nanoTime()
    val result = f(in)
    errorLoggingContext.debug(
      s"DB Query for ${log(result)} took: ${elapsedMillis(started)}ms"
    )
    result
  }

}
