// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter

import scala.util.Try

trait HasSynchronizeWithClosing extends HasRunOnClosing {

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has finished or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run.
    * @see
    *   HasRunOnClosing.isClosing
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def synchronizeWithClosingSync[A](name: String)(f: => A)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[A] =
    synchronizeWithClosingUS(name)(Try(f)).map(_.get)

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   The computation completes with
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise it is the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosing[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
      A: CanAbortDueToShutdown[F],
  ): F[A] = A.absorbOuter(synchronizeWithClosingUS(name)(f))

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the `synchronizeWithClosingPatience`
    * has elapsed.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock. DO NOT PUT
    * retries, especially indefinite ones, inside `f`.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise the result of running `f`.
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingUS[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): UnlessShutdown[F[A]]
}

object HasSynchronizeWithClosing {

  /** NeverClosing is a utility for things where we don't want or need closing, for example for very
    * short-lived tasks or in test code.
    */
  object NeverClosing extends HasSynchronizeWithClosing {
    private object DummyLifeCycleRegistrationHandle extends LifeCycleRegistrationHandle {
      override def cancel(): Boolean = false
      override def isScheduled: Boolean = false
    }

    override def synchronizeWithClosingUS[F[_], A](name: String)(
        f: => F[A]
    )(implicit traceContext: TraceContext, F: Thereafter[F]): UnlessShutdown[F[A]] =
      UnlessShutdown.Outcome(f)

    override def runOnClose(task: RunOnClosing): UnlessShutdown[LifeCycleRegistrationHandle] =
      UnlessShutdown.Outcome(DummyLifeCycleRegistrationHandle)

    override protected[this] def runTaskUnlessDone(task: RunOnClosing)(implicit
        traceContext: TraceContext
    ): Unit = ()

    override def isClosing: Boolean = false
  }
}
