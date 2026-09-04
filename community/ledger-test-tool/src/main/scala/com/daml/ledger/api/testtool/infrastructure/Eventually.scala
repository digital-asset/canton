// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.canton.lifecycle.HasSynchronizeWithClosing
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Success}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

final case class EventuallyException(
    assertionName: String,
    attempts: Int,
    elapsedMillis: Long,
    error: Throwable,
) extends Exception(error) {
  override def getMessage() =
    s"$assertionName failed after $attempts attempts and ${elapsedMillis}ms: ${error.getMessage()}"
}

object Eventually {

  private val defaultAttempts: Int = 18
  private val defaultFirstWaitTime: FiniteDuration = 10.millis
  private val defaultMaxWaitTime: FiniteDuration = 3.seconds

  /** Upper bound, because `Jitter.equal` can only shorten a sleep. */
  private[testtool] val DefaultMaxDeadline: FiniteDuration =
    maxDeadline(defaultAttempts, defaultFirstWaitTime, defaultMaxWaitTime)

  private[testtool] def maxDeadline(
      attempts: Int,
      firstWaitTime: FiniteDuration,
      maxWaitTime: FiniteDuration,
  ): FiniteDuration =
    Iterator
      .iterate(firstWaitTime)(delay => if (delay * 2 > maxWaitTime) maxWaitTime else delay * 2)
      .take(attempts)
      .foldLeft(Duration.Zero)(_ + _)

  private val eventuallyLogger = TracedLogger(LoggerFactory.getLogger(this.getClass))

  /** Sleeps double from `firstWaitTime` until they reach `maxWaitTime`: 10, 20, 40 ... 2560, then
    * 3s each. `Jitter.equal` shortens each sleep by up to half at random, so 18 attempts give a
    * deadline anywhere between 16 and 32 seconds.
    *
    * The 3 second cap matters as much as the budget. Without it the last sleep would be half the
    * whole budget, so a wait could be reported as failed many seconds after it actually succeeded,
    * and adding attempts would only widen that blind window.
    */
  def eventually[A](
      assertionName: String,
      attempts: Int = defaultAttempts,
      firstWaitTime: FiniteDuration = defaultFirstWaitTime,
      maxWaitTime: FiniteDuration = defaultMaxWaitTime,
  )(
      run: => Future[A]
  )(implicit ec: ExecutionContext): Future[A] = {
    implicit val tc: TraceContext = TraceContext.empty
    implicit val success: Success[A] = retry.Success.always
    val startedAtNanos = System.nanoTime()
    val attemptCount = new AtomicInteger(0)
    def elapsedMillis = (System.nanoTime() - startedAtNanos) / 1000000L
    retry
      .Backoff(
        logger = eventuallyLogger,
        hasSynchronizeWithClosing = HasSynchronizeWithClosing.NeverClosing,
        maxRetries = attempts,
        initialDelay = firstWaitTime,
        maxDelay = maxWaitTime,
        operationName = assertionName,
      )
      .applyFut(
        {
          attemptCount.incrementAndGet()
          run
        },
        AllExceptionRetryPolicy,
      )
      .map { result =>
        eventuallyLogger.debug(
          s"'$assertionName' succeeded after ${attemptCount.get()} attempts and ${elapsedMillis}ms"
        )
        result
      }
      .recoverWith { case t =>
        Future.failed(EventuallyException(assertionName, attemptCount.get(), elapsedMillis, t))
      }
  }
}
