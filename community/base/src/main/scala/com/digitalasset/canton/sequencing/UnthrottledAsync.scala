// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.Monoid
import com.digitalasset.canton.DoNotDiscardLikeFuture
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.ExecutionContext

/** A computation that should not count towards the asynchronous processing's throttling, but still
  * delay the cleaning of the sequencer counter
  */
@DoNotDiscardLikeFuture
sealed trait UnthrottledAsync extends Product with Serializable {
  def future: FutureUnlessShutdown[Unit]
}

/** UnthrottledAsync with no async computation.
  */
case object UnthrottledImmediate extends UnthrottledAsync {
  override def future: FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit
}

/** UnthrottledAsync with a FutureUnlessShutdown[Unit] as async computation.
  */
final case class UnthrottledAsyncF(future: FutureUnlessShutdown[Unit]) extends UnthrottledAsync

object UnthrottledAsync {
  def apply(future: FutureUnlessShutdown[Unit]): UnthrottledAsync = UnthrottledAsyncF(future)
  def immediate: UnthrottledAsync = UnthrottledImmediate

  implicit def monoidUnthrottledAsync(implicit ec: ExecutionContext): Monoid[UnthrottledAsync] =
    new Monoid[UnthrottledAsync] {
      private val fusMonoid = Monoid[FutureUnlessShutdown[Unit]]
      override def empty: UnthrottledAsync = immediate
      override def combine(x: UnthrottledAsync, y: UnthrottledAsync): UnthrottledAsync =
        (x, y) match {
          case (UnthrottledImmediate, other) => other
          case (other, UnthrottledImmediate) => other
          case (asyncF1: UnthrottledAsyncF, asyncF2: UnthrottledAsyncF) =>
            UnthrottledAsyncF(
              fusMonoid.combine(asyncF1.future, asyncF2.future)
            )
        }
    }
}
