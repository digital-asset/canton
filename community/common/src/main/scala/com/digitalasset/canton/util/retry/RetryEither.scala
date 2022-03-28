// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.syntax.either._
import cats.syntax.flatMap._
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.util.LoggerUtil
import org.slf4j.event.Level

/** Simple form of the retry policies that operate on Either and not Future[T].
  * Only provides a Pause-based retry.
  */
object RetryEither {

  def apply[A, B](
      maxRetries: Int,
      waitInMs: Long,
      operationName: String,
      logger: TracedLogger,
      stopOnLeft: Option[A => Boolean] = None,
      retryLogLevel: Level = Level.INFO,
      failLogLevel: Level = Level.WARN,
  )(
      body: => Either[A, B]
  )(implicit
      loggingContext: ErrorLoggingContext,
      closeContext: CloseContext,
  ): Either[A, B] = {
    maxRetries.tailRecM { retryCount =>
      body
        .map(Right(_))
        .leftFlatMap { err =>
          if (closeContext.flagCloseable.isClosing) {
            // Stop the retry attempts if caller is closing
            Left(err)
          } else if (stopOnLeft.exists(fn => fn(err))) {
            // Stop the retry attempts on this particular Left if stopOnLeft is true
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
