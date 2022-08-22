// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.Thereafter.syntax._

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Alert if a future does not complete within the prescribed duration
  *
  * We use future based synchronisation in some places, where we use a promise to only kick
  * off an action once a promise is completed. This can lead to deadlocks where something
  * does not start because we never complete the promise.
  * This leads to hard to debug situations. We can support debugging by tracking such futures.
  * As this is costly, we'll turn this off in production.
  *
  * @see HasFutureSupervision for a mixin
  */
trait FutureSupervisor {
  def supervised[T](description: => String, warnAfter: Duration = 10.seconds)(fut: Future[T])(
      implicit
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): Future[T]

  def supervisedUS[T](description: => String, warnAfter: Duration = 10.seconds)(
      fut: FutureUnlessShutdown[T]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[T] = FutureUnlessShutdown(supervised(description, warnAfter)(fut.unwrap))
}

object FutureSupervisor {

  object Noop extends FutureSupervisor {
    override def supervised[T](description: => String, warnAfter: Duration)(
        fut: Future[T]
    )(implicit
        errorLoggingContext: ErrorLoggingContext,
        executionContext: ExecutionContext,
    ): Future[T] = fut
  }

  class Impl(
      defaultWarningInterval: NonNegativeDuration
  )(implicit
      scheduler: ScheduledExecutorService
  ) extends FutureSupervisor {

    private case class ScheduledFuture(
        fut: Future[_],
        description: () => String,
        startNanos: Long,
        warnNanos: Long,
        errorLoggingContext: ErrorLoggingContext,
    ) {
      val warnCounter = new AtomicInteger(1)
      def alertNow(currentNanos: Long): Boolean = {
        val cur = warnCounter.get()
        if (currentNanos - startNanos > (warnNanos * cur) && !fut.isCompleted) {
          warnCounter.incrementAndGet().discard
          true
        } else false
      }
    }

    private val scheduled = new AtomicReference[Seq[ScheduledFuture]](Seq())
    private val defaultCheckMs = 1000L

    // schedule regular background checks
    scheduler.scheduleWithFixedDelay(
      () => checkSlow(),
      defaultCheckMs,
      defaultCheckMs,
      TimeUnit.MILLISECONDS,
    )

    private def checkSlow(): Unit = {
      val now = System.nanoTime()
      val cur = scheduled.updateAndGet(_.filterNot(_.fut.isCompleted))
      cur.filter(x => x.alertNow(now)).foreach { blocked =>
        val dur = Duration.fromNanos(now - blocked.startNanos)
        blocked.errorLoggingContext.logger
          .warn(
            s"${blocked.description()} has not completed after ${LoggerUtil.roundDurationForHumans(dur)}"
          )(
            blocked.errorLoggingContext.traceContext
          )
      }
    }

    def supervised[T](
        description: => String,
        warnAfter: Duration = defaultWarningInterval.duration,
    )(fut: Future[T])(implicit
        errorLoggingContext: ErrorLoggingContext,
        executionContext: ExecutionContext,
    ): Future[T] = {
      val itm =
        ScheduledFuture(
          fut,
          () => description,
          startNanos = System.nanoTime(),
          warnAfter.toNanos,
          errorLoggingContext,
        )
      scheduled.updateAndGet(x => x.filterNot(_.fut.isCompleted) :+ itm)
      fut.thereafter {
        case Failure(exception) =>
          errorLoggingContext.logger.warn(
            s"${description} failed with exception after ${elapsed(itm)}",
            exception,
          )(errorLoggingContext.traceContext)
        case Success(_) =>
          val time = elapsed(itm)
          if (time > warnAfter) {
            errorLoggingContext.logger.info(
              s"${description} succeed successfully but slow after $time"
            )(errorLoggingContext.traceContext)
          }
      }
    }

    private def elapsed(item: ScheduledFuture): Duration = {
      val dur = Duration.fromNanos(System.nanoTime() - item.startNanos)
      LoggerUtil.roundDurationForHumans(dur)
    }

  }
}
