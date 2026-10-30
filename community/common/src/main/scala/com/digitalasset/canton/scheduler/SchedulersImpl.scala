// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scheduler

import cats.syntax.parallel.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Represents one or more schedulers whose activeness and lifetime are managed together. Also
  * allows looking up each scheduler by name which is useful to wire up schedulers with grpc
  * services to handle schedule changes.
  */
class SchedulersImpl(
    val schedulers: Map[String, Scheduler],
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Schedulers
    with NamedLogging {

  override def start()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Starting ${schedulers.size} scheduler(s): ${schedulers.keys.mkString(",")}")
    schedulers.values.toList
      .parTraverse_(_.start())
      .thereafter {
        case Success(Outcome(_)) =>
          logger.debug(s"Started ${schedulers.size} scheduler(s)")
        case Success(AbortedDueToShutdown) =>
          logger.info(s"Aborted starting ${schedulers.size} scheduler(s) due to shutdown")
          stop()
        case Failure(t) =>
          logger.error(
            s"Failed to start all ${schedulers.size} scheduler(s). Stopping all to ensure no partial set of schedulers are running.",
            t,
          )
          // Stop successfully started schedulers if one failed
          if (NonFatal(t)) stop()
      }
  }

  override def stop()(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Stopping ${schedulers.size} scheduler(s): ${schedulers.keys.mkString(",")}")
    schedulers.values.foreach(_.stop())
    logger.info(s"Stopped ${schedulers.size} scheduler(s)")
  }

  override def close(): Unit = {
    stop()(TraceContext.todo)
    schedulers.values.foreach(_.close())
  }

  override def get(name: String): Option[Scheduler] = schedulers.get(name)
}
