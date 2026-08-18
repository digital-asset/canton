// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown

import scala.concurrent.Future

object EitherUtil {

  // if the condition is satisfied, return unit, otherwise, return the given A in Left.
  def condUnit[E](test: Boolean, left: => E): Either[E, Unit] =
    if (test) Right(()) else Left(left)

  implicit class RichEither[L, R](val either: Either[L, R]) extends AnyVal {

    /** @param f
      * @return
      *   this, after evaluation of the side effecting function f if this is a left.
      */
    def tapLeft(f: L => Unit): Either[L, R] = either match {
      case Left(value) =>
        f(value)
        either

      case Right(_) => either
    }

    /** @param f
      * @return
      *   this, after evaluation of the side effecting function f if this is a right.
      */
    def tapRight(f: R => Unit): Either[L, R] = either match {
      case Right(value) =>
        f(value)
        either

      case Left(_) => either
    }

    def toFuture(f: L => Throwable): Future[R] = either match {
      case Left(value) => Future.failed(f(value))
      case Right(value) => Future.successful(value)
    }

    def toFutureUS(f: L => Throwable): FutureUnlessShutdown[R] = either match {
      case Left(value) => FutureUnlessShutdown.failed(f(value))
      case Right(value) => FutureUnlessShutdown.pure(value)
    }
  }

  implicit class RichEitherObject(val either: Either.type) extends AnyVal {

    /** Similar to [[cats.syntax.EitherObjectOps.right]] but with partial application so that
      * callers can specify only the left type, but not the right type.
      *
      * Uses a capital `R` to avoid ambiguity with [[cats.syntax.EitherObjectOps.right]].
      */
    def Right[A]: EitherRightPartiallyApplied[A] = new EitherRightPartiallyApplied[A](either)

    def Left[B]: EitherLeftPartiallyApplied[B] = new EitherLeftPartiallyApplied[B](either)
  }

  final class EitherRightPartiallyApplied[A](private val either: Either.type) extends AnyVal {
    def apply[B](b: B): Either[A, B] = Right(b)
  }
  final class EitherLeftPartiallyApplied[B](private val either: Either.type) extends AnyVal {
    def apply[A](a: A): Either[A, B] = Left(a)
  }

  implicit class RichEitherIterable[L, R](val eithers: Iterable[Either[L, R]]) extends AnyVal {
    def collectLeft: Iterable[L] = eithers.collect { case Left(value) => value }
    def collectRight: Iterable[R] = eithers.collect { case Right(value) => value }
  }
}
