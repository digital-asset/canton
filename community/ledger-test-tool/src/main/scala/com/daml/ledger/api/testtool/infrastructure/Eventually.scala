// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.canton.lifecycle.HasSynchronizeWithClosing
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.{AllExceptionRetryPolicy, Success}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

final case class EventuallyException(assertionName: String, error: Throwable) extends Exception {
  override def getMessage() = s"$assertionName: ${error.getMessage()}"
}

object Eventually {

  private val eventuallyLogger = TracedLogger(LoggerFactory.getLogger(this.getClass))

  /*
  Runs provided closure with the exponential back-off retry strategy for a number of `attempts`.
   */
  def eventually[A](
      assertionName: String,
      attempts: Int = 10,
      firstWaitTime: FiniteDuration = 10.millis,
  )(
      run: => Future[A]
  )(implicit ec: ExecutionContext): Future[A] = {
    implicit val tc: TraceContext = TraceContext.empty
    implicit val success: Success[A] = retry.Success.always
    retry
      .Backoff(
        logger = eventuallyLogger,
        hasSynchronizeWithClosing = HasSynchronizeWithClosing.NeverClosing,
        maxRetries = attempts,
        initialDelay = firstWaitTime,
        maxDelay = 30.seconds,
        operationName = "test-tool-eventually",
      )
      .applyFut(run, AllExceptionRetryPolicy)
      .recoverWith { case t =>
        Future.failed(EventuallyException(assertionName, t))
      }
  }
}
