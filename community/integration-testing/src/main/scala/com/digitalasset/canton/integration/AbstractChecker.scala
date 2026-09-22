// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil

import scala.concurrent.duration.DurationLong

trait AbstractChecker { self: NamedLogging =>

  protected def timeIt[T](f: => T, message: String)(implicit traceContext: TraceContext): T = {
    val start = System.nanoTime()
    val result = f
    val elapsed = System.nanoTime() - start
    logger.info(
      s"${self.getClass.getSimpleName}: $message took: ${LoggerUtil.roundDurationForHumans(elapsed.nanos)}"
    )
    result
  }

}
