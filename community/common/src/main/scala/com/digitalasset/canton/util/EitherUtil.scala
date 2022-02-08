// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object EitherUtil {

  implicit class RichEither[L, R](val either: Either[L, R]) extends AnyVal {

    /** @param f
      * @return this, after evaluation of the side effecting function f if this is a left.
      */
    def tapLeft(f: L => Unit): Either[L, R] = either match {
      case Left(value) =>
        f(value)
        either

      case Right(_) => either
    }

    /** @param f
      * @return this, after evaluation of the side effecting function f if this is a right.
      */
    def tapRight(f: R => Unit): Either[L, R] = either match {
      case Right(value) =>
        f(value)
        either

      case Left(_) => either
    }
  }

  implicit class RichEitherIterable[L, R](val eithers: Iterable[Either[L, R]]) extends AnyVal {
    def collectLeft: Iterable[L] = eithers.collect { case Left(value) => value }
    def collectRight: Iterable[R] = eithers.collect { case Right(value) => value }
  }

  /** If `condition` is satisfied, return  `Right(())`, otherwise, return `Left(fail)`.
    */
  def condUnitE[L](condition: Boolean, fail: => L): Either[L, Unit] =
    Either.cond(condition, (), fail)
}
