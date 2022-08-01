// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Utility for periodically calling the sequencer's health check.
  * This is done because when the sequencer computes the health status, it will notify
  * listeners in case the health status has changed.
  */
class SequencerPeriodicHealthCheck(
    clock: Clock,
    interval: NonNegativeFiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
)(check: TraceContext => Future[SequencerHealthStatus])(implicit
    executionContext: ExecutionContext,
    closeContext: CloseContext,
) extends NamedLogging {

  TraceContext.withNewTraceContext(setupNextCheck()(_))

  private def runCheck()(implicit traceContext: TraceContext): Unit =
    closeContext.flagCloseable
      .performUnlessClosing("run-health-check") {
        val status = check(traceContext)
        status onComplete { _ =>
          setupNextCheck()
        }
      }
      .discard

  private def setupNextCheck()(implicit traceContext: TraceContext): Unit =
    closeContext.flagCloseable
      .performUnlessClosing("setup-health-check") {
        val _ = clock.scheduleAfter(_ => runCheck(), interval.duration)
      }
      .discard
}
