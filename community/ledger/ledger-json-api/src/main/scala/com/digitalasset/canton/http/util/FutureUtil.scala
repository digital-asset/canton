// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import cats.data.EitherT
import cats.{Applicative, Functor}

import scala.concurrent.Future

object FutureUtil {
  def liftET[E]: LiftET[E] = new LiftET(0)
  final class LiftET[E](private val ignore: Int) extends AnyVal {
    def apply[F[_]: Functor, A](fa: F[A]): EitherT[F, E, A] = EitherT.right(fa)
  }

  def eitherT[A, B](fa: Future[Either[A, B]]): EitherT[Future, A, B] =
    EitherT(fa)

  def either[A, B](d: Either[A, B])(implicit ev: Applicative[Future]): EitherT[Future, A, B] =
    EitherT.fromEither[Future](d)
}
