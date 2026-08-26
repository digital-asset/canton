// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.data.EitherT
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

package object metrics {

  implicit class TimerExtensions(val timer: Timer) extends AnyVal {

    def timeEitherT[E, A](ev: EitherT[Future, E, A]): EitherT[Future, E, A] =
      EitherT(Timed.future(timer, ev.value))

    def timeEitherFUS[E, A](
        ev: EitherT[FutureUnlessShutdown, E, A]
    ): EitherT[FutureUnlessShutdown, E, A] =
      EitherT(FutureUnlessShutdown(Timed.future(timer, ev.value.unwrap)))

    /** Times the execution of an evaluation and records the result in the provided timer. The
      * result is labeled according to the provided `labelMapping` function
      *
      * @param ev
      *   the `EitherT[FutureUnlessShutdown, E, A]` to be timed
      * @param labelMapping
      *   a function that maps the result of the `EitherT` to a label for the timer Note: the
      *   execution time of this function is included in the timing, so it need to be swift not to
      *   skew the metrics
      * @param labelKey
      *   the key for the label in the metrics context
      * @param shutdownStatus
      *   the label to use if the future is aborted due to shutdown
      * @param failedStatus
      *   the label to use if the future fails with an exception
      * @param context
      *   the metrics context to use for the timer
      * @param loggingContext
      *   the logging context to use for the timer
      * @tparam E
      *   the error type of the `EitherT`
      * @tparam A
      *   the success type of the `EitherT`
      * @return
      *   the original evaluation with timing applied
      */
    def timeEitherFUSWithLabels[E, A](
        ev: => EitherT[FutureUnlessShutdown, E, A],
        labelMapping: Either[E, A] => String = (ea: Either[E, A]) =>
          ea.fold(_ => "failure", _ => "success"),
        labelKey: String = "status",
        shutdownStatus: String = "fut_shutdown",
        failedStatus: String = "fut_failure",
    )(implicit
        context: MetricsContext = MetricsContext.Empty,
        loggingContext: ErrorLoggingContext,
    ): EitherT[FutureUnlessShutdown, E, A] = {
      implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.noTracingLogger)

      val handle = timer.startAsync()
      EitherT {
        val fut = ev.value.unwrap
        fut.onComplete { completion =>
          val status = completion match {
            case Success(Outcome(either)) => labelMapping(either)
            case Success(AbortedDueToShutdown) => shutdownStatus
            case Failure(_) => failedStatus
          }
          handle.stop()(MetricsContext(labelKey -> status))
        }
        FutureUnlessShutdown(fut)
      }
    }
  }
}
