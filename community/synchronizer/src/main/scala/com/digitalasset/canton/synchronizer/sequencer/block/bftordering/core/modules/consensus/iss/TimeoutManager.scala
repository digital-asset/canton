// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.TimeoutManager.TimeoutMetric
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
}
import com.digitalasset.canton.tracing.TraceContext

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration

/** Manages cancellable timeouts on behalf of another module; it is parametric in the type of the
  * timeout message to send to the owning module and in the type of the handle that represents a
  * cancellable timeout.
  */
class TimeoutManager[E <: Env[E], ParentModuleMessageT, TimeoutIdT](
    override val loggerFactory: NamedLoggerFactory,
    timeout: FiniteDuration,
    timeoutId: TimeoutIdT,
    timeoutMetric: Option[TimeoutMetric],
)(implicit metricsContext: MetricsContext)
    extends NamedLogging {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var timeoutCancellable: Option[(Instant, CancellableEvent)] = None

  def scheduleTimeout[TimeoutMessageT <: ParentModuleMessageT](
      timeoutEvent: TimeoutMessageT
  )(implicit
      context: E#ActorContextT[ParentModuleMessageT],
      traceContext: TraceContext,
  ): Unit = {
    val cancellableEvent = context.delayedEvent(timeout, timeoutEvent)
    val timeNow = Instant.now()
    timeoutCancellable match {
      case Some((previousTime, previousTimeout)) =>
        previousTimeout.cancel().discard
        val duration = Duration.between(previousTime, timeNow)
        logger.debug(
          s"Rescheduling timeout w/ duration: $timeout; previous event: $previousTimeout; new event: $timeoutEvent"
        )
        timeoutMetric.foreach(_.scheduleChangedAfter(duration))
      case None =>
        logger.debug(
          s"Scheduling new timeout w/ duration: $timeout; new event: $timeoutEvent"
        )
    }
    timeoutCancellable = Some(timeNow -> cancellableEvent)
  }

  def cancelTimeout()(implicit traceContext: TraceContext): Unit = {
    timeoutCancellable.foreach { case (previousTime, timeout) =>
      timeoutMetric.foreach(
        _.scheduleChangedAfter(
          Duration.between(previousTime, Instant.now())
        )
      )
      logger.debug(s"Canceling timeout w/ ID: $timeoutId")
      timeout.cancel().discard
    }
    timeoutCancellable = None
  }
}

object TimeoutManager {
  trait TimeoutMetric {
    def scheduleChangedAfter(duration: Duration): Unit
  }
}
