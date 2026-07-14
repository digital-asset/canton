// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.show.*
import cats.{Functor, Show}
import com.daml.scalautil.ExceptionOps

object ErrorOps {

  implicit final class EitherThrowableWssExtras[R](private val self: Either[Throwable, R])
      extends AnyVal {
    def liftErr[M](f: String => M): Either[M, R] =
      self leftMap (e => f(ExceptionOps.getDescription(e)))
  }

  implicit final class EitherWssExtras[L, R](private val self: Either[L, R]) extends AnyVal {
    def liftErr[M](f: String => M)(implicit L: Show[L]): Either[M, R] =
      self leftMap (e => f(e.show))
  }

  implicit final class EitherTWssExtras[F[_]: Functor, L, R](private val self: EitherT[F, L, R])
      extends AnyRef {
    def liftErr[M](f: String => M)(implicit L: Show[L]): EitherT[F, M, R] =
      self leftMap (e => f(e.show))
  }
}
