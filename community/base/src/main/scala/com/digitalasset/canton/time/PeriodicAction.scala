// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class PeriodicAction(
    clock: Clock,
    interval: NonNegativeFiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
    description: String,
)(check: TraceContext => FutureUnlessShutdown[Any])(implicit
    executionContext: ExecutionContext
) extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  TraceContext.withNewTraceContext(description)(setupNextCheck()(_))

  private def runCheck()(implicit traceContext: TraceContext): Unit =
    synchronizeWithClosing(s"run-$description")(check(traceContext))
      .onComplete(_ => setupNextCheck())

  private def setupNextCheck()(implicit traceContext: TraceContext): Unit =
    synchronizeWithClosingSync(s"setup-$description") {
      val _ =
        clock.scheduleAfterCancelledOnShutdown(_ => runCheck(), description, interval.duration)
    }.discard

}
