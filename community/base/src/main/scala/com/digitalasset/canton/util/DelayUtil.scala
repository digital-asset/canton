// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}

/** Utility to create futures that succeed after a given delay.
  *
  * Inspired by the odelay library, but with a restricted interface to avoid hazardous effects that could be caused
  * by the use of a global executor service.
  *
  * TODO(i4245): Replace all usages by Clock.
  */
object DelayUtil extends NamedLogging {

  override protected val loggerFactory: NamedLoggerFactory =
    NamedLoggerFactory.unnamedKey("purpose", "global")

  // use a daemon thread for the executor as it doesn't get explicitly shutdown
  private val scheduledExecutorService =
    Threading.singleThreadScheduledExecutor("delay-util", logger, daemon = true)

  /** Creates a future that succeeds after the given delay.
    * The caller must make sure that the future is used only in execution contexts that have not yet been closed.
    * Use the `delay(FiniteDuration, FlagCloseable)` method if this might be an issue.
    *
    * Try to use `Clock` instead!
    */
  def delay(delay: FiniteDuration): Future[Unit] =
    this.delay(scheduledExecutorService, delay, _.success(()))

  /** Creates a future that succeeds after the given delay provided that `flagCloseable` has not yet been closed then.
    *
    * Try to use `Clock` instead!
    */
  def delay(name: String, delay: FiniteDuration, flagCloseable: FlagCloseable)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    this.delay(
      scheduledExecutorService,
      delay,
      { promise =>
        val _ = flagCloseable.performUnlessClosing(name)(promise.success(()))
      },
    )

  private[util] def delay(
      executor: ScheduledExecutorService,
      delay: FiniteDuration,
      complete: Promise[Unit] => Unit,
  ): Future[Unit] = {
    val promise = Promise[Unit]()
    executor.schedule((() => complete(promise)): Runnable, delay.length, delay.unit)
    promise.future
  }

  /** Creates a future that succeeds after the given delay provided that `flagCloseable` has not yet been closed then.
    * The future completes fast with UnlessShutdown.AbortedDueToShutdown if `flagCloseable` is already closing.
    */
  def delayIfNotClosing(name: String, delay: FiniteDuration, flagCloseable: FlagCloseable)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val promise = Promise[UnlessShutdown[Unit]]()
    val future = promise.future

    import com.digitalasset.canton.lifecycle.RunOnShutdown
    flagCloseable.runOnShutdown(new RunOnShutdown() {
      val name = "delayOrAborted-shutdown"
      def done = promise.isCompleted
      def run(): Unit = {
        val _ = promise.trySuccess(UnlessShutdown.AbortedDueToShutdown)
      }
    })

    val trySuccess: Runnable = { () =>
      val _ = promise.trySuccess(UnlessShutdown.Outcome(()))
    }

    scheduledExecutorService.schedule(trySuccess, delay.length, delay.unit)
    FutureUnlessShutdown(future)
  }
}
