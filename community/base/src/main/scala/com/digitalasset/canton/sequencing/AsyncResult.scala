// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.{Functor, Monoid, Semigroupal}
import com.digitalasset.canton.DoNotDiscardLikeFuture
import com.digitalasset.canton.lifecycle.{
  CanAbortDueToShutdown,
  FutureUnlessShutdown,
  UnlessShutdown,
}

import scala.concurrent.ExecutionContext
import scala.util.Try

/** The asynchronous part of processing an event (or of a stage of its processing). */
@DoNotDiscardLikeFuture
final case class AsyncResult[+T](unwrap: FutureUnlessShutdown[T]) {
  def transform[U](f: Try[UnlessShutdown[T]] => Try[UnlessShutdown[U]])(implicit
      ec: ExecutionContext
  ): AsyncResult[U] =
    AsyncResult(unwrap.transform(f))

  /** Analog to [[com.digitalasset.canton.util.Thereafter.thereafter]] We do not provide a
    * [[com.digitalasset.canton.util.Thereafter.thereafter]] instance because [[AsyncResult]]
    * doesn't take a type argument.
    */
  def thereafter(f: Try[UnlessShutdown[T]] => Unit)(implicit
      ec: ExecutionContext
  ): AsyncResult[T] =
    transform { res =>
      f(res)
      res
    }

  def map[U](f: T => U)(implicit
      ec: ExecutionContext
  ): AsyncResult[U] = AsyncResult(unwrap.map(f))

  def flatMapFUS[U](f: T => FutureUnlessShutdown[U])(implicit
      ec: ExecutionContext
  ): AsyncResult[U] = AsyncResult(unwrap.flatMap(f))
}

object AsyncResult {

  implicit def asyncResultFunctor(implicit
      executionContext: ExecutionContext
  ): Functor[AsyncResult] = new Functor[AsyncResult] {
    override def map[A, B](fa: AsyncResult[A])(f: A => B): AsyncResult[B] = fa.map(f)
  }

  implicit def asyncResultSemigroupal(implicit
      executionContext: ExecutionContext
  ): Semigroupal[AsyncResult] = new Semigroupal[AsyncResult] {
    override def product[A, B](fa: AsyncResult[A], fb: AsyncResult[B]): AsyncResult[(A, B)] =
      AsyncResult {
        for {
          a <- fa.unwrap
          b <- fb.unwrap
        } yield (a, b)
      }
  }

  /** No asynchronous processing. */
  val immediateUnit: AsyncResult[Unit] = AsyncResult(FutureUnlessShutdown.unit)

  /** No asynchronous processing. */
  val immediate: AsyncResult[UnthrottledAsync] = AsyncResult(
    FutureUnlessShutdown.pure(UnthrottledAsync.immediate)
  )

  def apply(f: FutureUnlessShutdown[Unit])(implicit
      ec: ExecutionContext
  ): AsyncResult[UnthrottledAsync] = AsyncResult(f.map(_ => UnthrottledAsync.immediate))

  def pure[T](t: T): AsyncResult[T] = AsyncResult(FutureUnlessShutdown.pure(t))

  implicit def monoidAsyncResult[A: Monoid](implicit ec: ExecutionContext): Monoid[AsyncResult[A]] =
    new Monoid[AsyncResult[A]] {
      override def empty: AsyncResult[A] = AsyncResult(Monoid[FutureUnlessShutdown[A]].empty)
      override def combine(x: AsyncResult[A], y: AsyncResult[A]): AsyncResult[A] =
        AsyncResult(Monoid[FutureUnlessShutdown[A]].combine(x.unwrap, y.unwrap))
    }

  implicit val asyncResultCanAbortDueToShutdown: CanAbortDueToShutdown[AsyncResult] =
    new CanAbortDueToShutdown[AsyncResult] {
      override def abort[A]: AsyncResult[A] = AsyncResult(FutureUnlessShutdown.abortedDueToShutdown)
    }
}
