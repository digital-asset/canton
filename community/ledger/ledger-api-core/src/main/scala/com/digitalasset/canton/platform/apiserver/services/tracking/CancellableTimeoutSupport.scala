// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.DiscardOps

import java.time.Duration
import java.util.{Timer, TimerTask}
import scala.concurrent.Promise
import scala.util.Try
import scala.util.control.NonFatal

trait CancellableTimeoutSupport {
  def scheduleOnce[T](
      duration: Duration,
      promise: Promise[T],
      onTimeout: => Try[T],
  )(implicit loggingContext: LoggingContext): AutoCloseable
}

object CancellableTimeoutSupport {
  def owner(timerThreadName: String): ResourceOwner[CancellableTimeoutSupport] =
    ResourceOwner
      .forTimer(() => new Timer(timerThreadName, true))
      .map(new CancellableTimeoutSupportImpl(_))
}

private[tracking] class CancellableTimeoutSupportImpl(timer: Timer)
    extends CancellableTimeoutSupport {
  private val logger = ContextualizedLogger.get(getClass)

  override def scheduleOnce[T](
      duration: Duration,
      promise: Promise[T],
      onTimeout: => Try[T],
  )(implicit loggingContext: LoggingContext): AutoCloseable = {
    val timerTask = new TimerTask {
      override def run(): Unit =
        try {
          promise.tryComplete(onTimeout).discard
        } catch {
          case NonFatal(e) =>
            val exceptionMessage =
              "Exception thrown in complete. Resources might have not been appropriately cleaned"
            logger.error(exceptionMessage, e)
        }
    }
    timer.schedule(timerTask, duration.toMillis)
    () => timerTask.cancel().discard
  }
}
