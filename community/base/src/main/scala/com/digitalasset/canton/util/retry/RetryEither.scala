// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.flatMap.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.{CloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.LoggerUtil
import org.slf4j.event.Level

/** Simple form of the retry policies that operate on Either and not Future[T]. Only provides a
  * Pause-based retry.
  */
object RetryEither {
  def retry[A, B](
      maxRetries: Int,
      waitInMs: Long,
      operationName: String,
      stopOnLeft: Option[A => Boolean] = None,
      retryLogLevel: Level = Level.INFO,
      failLogLevel: Level = Level.WARN,
  )(
      body: => Either[A, B]
  )(implicit
      loggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, A, B] =
    retryUnlessShutdown(
      maxRetries,
      waitInMs,
      operationName,
      stopOnLeft,
      retryLogLevel,
      failLogLevel,
    )(
      EitherT(UnlessShutdown.Outcome(body))
    )

  /** Like [[retry]], but the wait between attempts starts at `initialWaitInMs` and doubles up to
    * `maxWaitInMs`. Use it when the check is cheap (e.g., reads in-memory state) and the condition
    * usually becomes true shortly after the first attempt, so that a fixed `maxWaitInMs` pause
    * would mostly be idle time.
    *
    * The total time spent waiting is bounded by `maxRetries * maxWaitInMs`, i.e., the same budget
    * that [[retry]] has with `waitInMs = maxWaitInMs`. `maxRetries = Int.MaxValue` retries forever.
    */
  def retryWithBackoff[A, B](
      maxRetries: Int,
      initialWaitInMs: Long,
      maxWaitInMs: Long,
      operationName: String,
      stopOnLeft: Option[A => Boolean] = None,
      retryLogLevel: Level = Level.INFO,
      failLogLevel: Level = Level.WARN,
  )(
      body: => Either[A, B]
  )(implicit
      loggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, A, B] = {
    require(
      0 < initialWaitInMs && initialWaitInMs <= maxWaitInMs,
      s"initialWaitInMs ($initialWaitInMs) must be positive and at most maxWaitInMs ($maxWaitInMs)",
    )
    val forever = maxRetries == Int.MaxValue
    val budgetInMs = if (forever) Long.MaxValue else maxRetries.toLong * maxWaitInMs
    // State: (remaining wait budget, next wait)
    (budgetInMs, initialWaitInMs).tailRecM { case (remainingInMs, nextWaitInMs) =>
      EitherT {
        closeContext.context
          .synchronizeWithClosingSync(operationName)(body)(loggingContext.traceContext)
          .map {
            _.map(Right(_)).leftFlatMap { err =>
              if (stopOnLeft.exists(fn => fn(err))) {
                LoggerUtil.logAtLevel(
                  failLogLevel,
                  s"Operation $operationName failed, stopping retries: $err",
                )
                Left(err)
              } else if (remainingInMs <= 0) {
                LoggerUtil.logAtLevel(
                  failLogLevel,
                  s"Operation $operationName failed, exhausted retries: $err",
                )
                Left(err)
              } else {
                val waitInMs = Math.min(nextWaitInMs, remainingInMs)
                LoggerUtil.logAtLevel(
                  retryLogLevel,
                  s"Operation $operationName failed, retrying in ${waitInMs}ms: $err",
                )
                Threading.sleep(waitInMs)
                val remaining = if (forever) remainingInMs else remainingInMs - waitInMs
                Right(Left((remaining, Math.min(nextWaitInMs * 2, maxWaitInMs))))
              }
            }
          }
      }
    }
  }

  def retryUnlessShutdown[A, B](
      maxRetries: Int,
      waitInMs: Long,
      operationName: String,
      stopOnLeft: Option[A => Boolean] = None,
      retryLogLevel: Level = Level.INFO,
      failLogLevel: Level = Level.WARN,
  )(
      body: => EitherT[UnlessShutdown, A, B]
  )(implicit
      loggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, A, B] =
    maxRetries.tailRecM { retryCount =>
      EitherT {
        closeContext.context
          .synchronizeWithClosingSync(operationName)(body)(loggingContext.traceContext)
          .flatMap {
            _.value.map {
              _.map(Right(_))
                .leftFlatMap { err =>
                  if (stopOnLeft.exists(fn => fn(err))) {
                    // Stop the retry attempts on this particular Left if stopOnLeft is true
                    LoggerUtil.logAtLevel(
                      failLogLevel,
                      s"Operation $operationName failed, stopping retries: $err",
                    )
                    Left(err)
                  } else if (retryCount <= 0) {
                    // Stop the recursion with the error if we exhausted the max retries
                    LoggerUtil.logAtLevel(
                      failLogLevel,
                      s"Operation $operationName failed, exhausted retries: $err",
                    )
                    Left(err)
                  } else {
                    // Retry the operation if it failed but we have retries left
                    LoggerUtil.logAtLevel(
                      retryLogLevel,
                      s"Operation $operationName failed, retrying in ${waitInMs}ms: $err",
                    )
                    Threading.sleep(waitInMs)
                    val nextRetry = if (retryCount == Int.MaxValue) Int.MaxValue else retryCount - 1
                    Right(Left(nextRetry))
                  }
                }
            }
          }
      }
    }

}
