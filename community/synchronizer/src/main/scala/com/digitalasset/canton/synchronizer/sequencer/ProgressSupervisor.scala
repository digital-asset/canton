// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{PromiseUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration

/** ProgressSupervisor is a component that is meant to monitor anomalies in the node's operation,
  * (i.e. node not making progressing), not covered by the conventional health checks, based on
  * timing and expectations of processing progress of a data flow. It is meant to trigger alerts and
  * possibly remedial actions:
  *   - Bumping log levels to DEBUG, flushing buffered logs, collecting thread dumps, heap dumps,
  *     etc.
  *   - Restarting the node.
  *   - Taking a poison pill.
  *
  * It is safe for asynchronous usage, i.e., multiple threads can arm and disarm monitors
  * concurrently. Disarming before arming is safe. As long as both operations are performed within
  * the `logAfter` duration, no warn action will be issued.
  */
class ProgressSupervisor(
    logLevel: Level,
    logAfter: Duration,
    futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
    warnAction: => Unit = (),
) extends NamedLogging {

  logger.underlying.info(
    s"ProgressSupervisor created with log level $logLevel, log after $logAfter, future supervisor $futureSupervisor"
  )

  private val monitors: TrieMap[CantonTimestamp, PromiseUnlessShutdown[Unit]] = TrieMap.empty

  // Sometimes `disarm` overtakes `arm`, so we treat both ops the same, first - arming, second - disarming
  private def armOrDisarm(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    monitors
      .updateWith(timestamp) {
        case Some(promise) =>
          promise.success(UnlessShutdown.Outcome(()))
          None
        case None =>
          val promise = PromiseUnlessShutdown.supervised[Unit](
            s"self-subscription-observe-ts-$timestamp",
            futureSupervisor,
            logLevel = logLevel,
            logAfter = logAfter,
            warnAction = warnAction,
          )
          Some(promise)
      }
      .foreach { promise =>
        // start supervision only once the promise has made it into the map
        // due to promise.future being lazy, we need to explicitly poke it here
        promise.future.discard
      }

  def arm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Arming progress monitor for sequencing timestamp $timestamp")
    armOrDisarm(timestamp)
  }

  def disarm(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Disarming progress monitor for sequencing timestamp $timestamp")
    armOrDisarm(timestamp)
  }

}
