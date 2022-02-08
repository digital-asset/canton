// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import cats.syntax.either._
import cats.{Applicative, Functor}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.metrics.MetricHandle.TimerM
import com.digitalasset.canton.util.Thereafter.syntax._
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Utility functions for the `cats ` [[cats.data.EitherT]] monad transformer.
  * https://typelevel.org/cats/datatypes/eithert.html
  */
object EitherTUtil {

  /** Runs a `finally block` after `fn` has completed, regardless of whether `fn` has completed successfully or failed.
    */
  def finallyET[F[_], E, R](
      `finally`: () => Unit
  )(fn: => EitherT[F, E, R])(implicit F: Applicative[F]): EitherT[F, E, R] = {
    val result = fn

    result.transform(value => {
      `finally`()
      value
    })

    result
  }

  /** Similar to `finallyET` but will only call the provided handler if `fn` returns a left/error or fails. */
  def onErrorOrFailure[A, B](errorHandler: () => Unit)(
      fn: => EitherT[Future, A, B]
  )(implicit executionContext: ExecutionContext): EitherT[Future, A, B] =
    fn.thereafter {
      case Failure(_) =>
        errorHandler()
      case Success(Left(_)) =>
        errorHandler()
      case _ => ()
    }

  def onErrorOrFailureUnlessShutdown[A, B](errorHandler: () => Unit)(
      fn: => EitherT[FutureUnlessShutdown, A, B]
  )(implicit executionContext: ExecutionContext): EitherT[FutureUnlessShutdown, A, B] =
    fn.thereafter {
      case Failure(_) =>
        errorHandler()
      case Success(UnlessShutdown.Outcome(Left(_))) =>
        errorHandler()
      case _ => ()
    }

  /** Lifts an `if (cond) then ... else ()` into the `EitherT` applicative */
  def ifThenET[F[_], L](cond: Boolean)(`then`: => EitherT[F, L, _])(implicit
      F: Applicative[F]
  ): EitherT[F, L, Unit] =
    if (cond) Functor[EitherT[F, L, *]].void(`then`) else EitherT.pure[F, L](())

  def condUnitET[F[_]]: CondUnitEitherTPartiallyApplied[F] =
    new CondUnitEitherTPartiallyApplied[F]()
  private[util] final class CondUnitEitherTPartiallyApplied[F[_]](private val dummy: Boolean = true)
      extends AnyVal {
    def apply[L](condition: Boolean, fail: => L)(implicit F: Applicative[F]): EitherT[F, L, Unit] =
      EitherT.cond[F](condition, (), fail)
  }

  def leftSubflatMap[F[_], A, B, C, BB >: B](x: EitherT[F, A, B])(f: A => Either[C, BB])(implicit
      F: Functor[F]
  ): EitherT[F, C, BB] =
    EitherT(F.map(x.value)(_.leftFlatMap(f)))

  /** Construct an EitherT from a possibly failed future. */
  def fromFuture[E, A](fut: Future[A], errorHandler: Throwable => E)(implicit
      ec: ExecutionContext
  ): EitherT[Future, E, A] =
    liftFailedFuture(fut.map(Right(_)), errorHandler)

  /** Lift a failed future into a Left value. */
  def liftFailedFuture[E, A](fut: Future[Either[E, A]], errorHandler: Throwable => E)(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, E, A] =
    EitherT(fut.recover[Either[E, A]] { case NonFatal(x) =>
      errorHandler(x).asLeft[A]
    })

  /** Log `message` if `result` fails with an exception or results in a `Left` */
  def logOnError[E, R](result: EitherT[Future, E, R], message: String, level: Level = Level.ERROR)(
      implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[Future, E, R] = {

    def logError(v: Try[Either[E, R]]): Try[Either[E, R]] = {
      v match {
        case Success(Left(err)) => LoggerUtil.logAtLevel(level, message + " " + err.toString)
        case Failure(NonFatal(err)) => LoggerUtil.logThrowableAtLevel(level, message, err)
        case _ =>
      }
      v
    }
    EitherT(result.value.transform(logError))
  }

  /** Discard `eitherT` and log an error if it does not result in a `Right`.
    * This is useful to document that an `EitherT[Future,_,_]` is intentionally not being awaited upon.
    */
  def doNotAwait(
      eitherT: EitherT[Future, _, _],
      failureMessage: => String,
      level: Level = Level.ERROR,
  )(implicit executionContext: ExecutionContext, loggingContext: ErrorLoggingContext): Unit = {
    val _ = logOnError(eitherT, failureMessage, level = level)
  }

  /** Measure time of EitherT-based calls, inspired by upstream com.daml.metrics.Timed.future */
  def timed[E, R](timerMetric: TimerM)(
      code: => EitherT[Future, E, R]
  )(implicit executionContext: ExecutionContext): EitherT[Future, E, R] = {
    val timerContext = timerMetric.metric.time()
    code.thereafter { _ =>
      timerContext.stop().discard[Long]
    }
  }

  /** Transform an EitherT into a Future.failed on left
    *
    * Comes handy when having to return io.grpc.StatusRuntimeExceptions
    */
  def toFuture[L <: Throwable, R](x: EitherT[Future, L, R])(implicit
      executionContext: ExecutionContext
  ): Future[R] =
    x.foldF(Future.failed, Future.successful)

  def unit[A]: EitherT[Future, A, Unit] = EitherT(Future.successful(().asRight[A]))

}
