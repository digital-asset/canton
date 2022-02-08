// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, StackTraceUtil}

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{ExecutionContext, Future}

/** Debugging utility used to write at regular intervals the executor service queue size into the logfile
  *
  * Useful to debug starvation issues.
  */
class ExecutionContextMonitor(
    val loggerFactory: NamedLoggerFactory,
    interval: NonNegativeFiniteDuration,
    maxReports: Int,
    reportAsWarnings: Boolean,
    override val timeouts: ProcessingTimeout,
)(implicit scheduler: ScheduledExecutorService)
    extends NamedLogging
    with FlagCloseable {

  private def schedule(runnable: Runnable): Unit = {
    val ms = interval.toScala.toMillis
    val _ = scheduler.scheduleAtFixedRate(runnable, ms, ms, TimeUnit.MILLISECONDS)
  }

  private val scheduled = new AtomicBoolean(false)
  private val reported = new AtomicBoolean(false)
  private val reports = new AtomicInteger(0)

  import TraceContext.Implicits.Empty._

  def monitor(ec: ExecutionContextIdlenessExecutorService): Unit = {
    logger.debug(s"Monitoring ${ec.name}")
    val runnable = new Runnable {
      override def run(): Unit = {
        if (!isClosing) {
          // if we are still scheduled, complain!
          if (scheduled.getAndSet(true)) {
            if (!reported.getAndSet(true)) {
              reportIssue(ec)
            }
          }
          // if we aren't scheduled yet, schedule a future!
          else {
            implicit val myEc: ExecutionContext = ec
            FutureUtil.doNotAwait(
              Future {
                if (!scheduled.getAndSet(false)) {
                  logger.error(s"Are we monitoring the EC ${ec.name} twice?")
                }
                // reset the reporting
                if (reported.getAndSet(false)) {
                  emit(
                    s"Task runner ${ec.name} is just overloaded, but operating correctly. Task got executed in the meantime."
                  )
                }
              },
              "Monitoring future failed despite being trivial ...",
            )
          }
        }
      }
    }
    schedule(runnable)
  }

  private def emit(message: => String): Unit = {
    if (reportAsWarnings) {
      logger.warn(message)
    } else {
      logger.debug(message)
    }
  }

  private def reportIssue(ec: ExecutionContextIdlenessExecutorService): Unit = {
    def message(): String = {
      s"Task runner ${ec.name} is stuck or overloaded. My scheduled task has not been processed for at least ${interval.toScala} (queue-size=${ec.queueSize}).\n$ec"
    }
    val count = reports.incrementAndGet()
    if (count <= maxReports) {

      emit(message())
      val traces = StackTraceUtil.formatStackTrace(_.getName.startsWith(ec.name))
      logger.debug(s"Here is the stack-trace of threads for ${ec.name}:\n$traces")
      if (count == maxReports) {
        logger.info(s"Reached ${count} of issue reports. Shutting up now.")
      }
    }
  }

}
