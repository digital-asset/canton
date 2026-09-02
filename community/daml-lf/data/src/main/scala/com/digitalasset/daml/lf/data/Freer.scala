// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import cats.{Monad, StackSafeMonad, ~>}

import scala.annotation.tailrec

/** A freer monad over effect signature `F` with error channel `E`.
  *
  * Build with [[Freer.pure]] / [[Freer.raise]] / [[Freer.lift]], compose with [[flatMap]] /
  * [[map]]. Run with [[start]] (one step) or [[consume]] (to completion).
  *
  * The [[start]]/[[Freer.Step]] model is single-use, tuned for stateful interpreters (their `F`
  * effects mutate external state), and lets callers drive with their own async framework;
  * [[consume]] is a synchronous helper, mostly for tests.
  */
sealed abstract class Freer[F[_], E, +A] {

  import Freer.*

  def flatMap[B](f: A => Freer[F, E, B]): Freer[F, E, B] =
    Freer.Bind(this, f)

  def map[B](f: A => B): Freer[F, E, B] =
    flatMap(a => Freer.Pure(f(a)))

  def void: Freer[F, E, Unit] = flatMap(_ => Freer.unit)

  /** Reduce to the next [[Freer.Step]]: a value, an error, or the first effect. Runs no `F` effect.
    *
    * Reduction is purely functional (it reassociates the immutable program), so `start` may be
    * called repeatedly to replay from the beginning.
    */
  def start: Step[F, E, A] = {
    // Non-tail re-entry point for `Impure.resume` (a lambda cannot host the
    // tail-recursive self-call directly).
    def loop_(freer: Freer[F, E, Any]): Step[F, E, A] = loop(freer)

    @tailrec
    def loop(freer: Freer[F, E, Any]): Step[F, E, A] =
      freer match {
        case Pure(v) =>
          Step.Pure(v.asInstanceOf[A])

        case Error(e) =>
          Step.Error(e)

        case Lift(fa) =>
          new Step.Impure[F, E, Any, A](fa, (x: Any) => loop_(Pure(x)))

        case Bind(sub, f) =>
          sub match {
            case Pure(x) =>
              loop(f(x))
            case Error(e) =>
              Step.Error(e)
            case Lift(fa) =>
              new Step.Impure[F, E, Any, A](fa, (x: Any) => loop_(f(x)))
            case Bind(sub1, g) =>
              loop(Bind(sub1, (x: Any) => Bind(g(x), f)))
          }
      }

    loop(this)
  }

  /** Run this program to completion, answering each effect with `handler`. Returns `Left` if the
    * program raises or the handler short-circuits.
    */
  def consume(handler: F ~> Either[E, *]): Either[E, A] = {
    @tailrec
    def go(step: Step[F, E, A]): Either[E, A] =
      step match {
        case Step.Pure(a) => Right(a)
        case Step.Error(e) => Left(e)
        case im: Step.Impure[F, E, x, A] =>
          handler(im.fx) match {
            case Right(x) => go(im.resume(x))
            case Left(err) => Left(err)
          }
      }

    go(this.start)
  }
}

object Freer {

  final case class Pure[F[_], E, +A](value: A) extends Freer[F, E, A]

  final case class Error[F[_], E](error: E) extends Freer[F, E, Nothing]

  final case class Lift[F[_], E, A](fa: F[A]) extends Freer[F, E, A]

  private final case class Bind[F[_], E, X, +A](
      sub: Freer[F, E, X],
      f: X => Freer[F, E, A],
  ) extends Freer[F, E, A]

  def pure[F[_], E, A](a: A): Freer[F, E, A] = Pure(a)

  def raise[F[_], E](e: E): Freer[F, E, Nothing] = Error(e)

  def lift[F[_], E, A](fa: F[A]): Freer[F, E, A] = Lift(fa)

  def from[F[_], E, A](either: Either[E, A]): Freer[F, E, A] =
    either match {
      case Right(value) => pure(value)
      case Left(value) => raise(value)
    }

  implicit def monadInstance[F[_], E]: Monad[Freer[F, E, *]] =
    new StackSafeMonad[Freer[F, E, *]] {
      override def pure[A](a: A): Freer[F, E, A] = Freer.pure(a)
      override def flatMap[A, B](fa: Freer[F, E, A])(f: A => Freer[F, E, B]): Freer[F, E, B] =
        fa.flatMap(f)
      override def map[A, B](fa: Freer[F, E, A])(f: A => B): Freer[F, E, B] = fa.map(f)
    }

  private[this] val PureUnit: Freer[Nothing, Nothing, Unit] = pure(())
  def unit[F[_], E]: Freer[F, E, Unit] = PureUnit.asInstanceOf[Freer[F, E, Unit]]

  private val PureNone: Freer[Nothing, Nothing, None.type] = pure(None)
  def none[F[_], E]: Freer[F, E, scala.None.type] =
    PureNone.asInstanceOf[Freer[F, E, scala.None.type]]

  /** The result of reducing a [[Freer]] to its next observable point.
    *
    * A `Step` is the interpreter's cursor, not a program: it has no `flatMap` / `map`. Observe it
    * and, for [[Step.Impure]], run the effect and call `resume`.
    */
  sealed abstract class Step[F[_], E, +A]

  object Step {

    /** Program reduced to a final value. */
    final case class Pure[F[_], E, +A](value: A) extends Step[F, E, A]

    /** Program reduced to an error (remaining continuations are discarded). */
    final case class Error[F[_], E](error: E) extends Step[F, E, Nothing]

    /** Suspended on effect `fx`; run it and feed the result to `resume`.
      */
    final case class Impure[F[_], E, X, +A](fx: F[X], resume: X => Step[F, E, A])
        extends Step[F, E, A]
  }

  /** Provides a base trait for creating companion objects with fixed F[_] effect and E error types
    */
  abstract class Companion {
    type F[_]
    type E
    type T[A] = Freer[F, E, A]

    /** The synchronous answer to a single effect: either the error channel `E` or a value. */
    type ErrOr[A] = Either[E, A]

    /** A handler that answers each effect `F[X]` with either an error `E` or a value `X`, for use
      * with [[data.Freer#consume]].
      */
    type Handler = F ~> ErrOr

    type Step[A] = Freer.Step[F, E, A]
    object Step {
      type Pure[A] = Freer.Step.Pure[F, E, A]
      val Pure: Freer.Step.Pure.type = Freer.Step.Pure
      type Error = Freer.Step.Error[F, E]
      val Error: Freer.Step.Error.type = Freer.Step.Error
      type Impure[X, A] = Freer.Step.Impure[F, E, X, A]
      val Impure: Freer.Step.Impure.type = Freer.Step.Impure
    }
    def pure[A](a: A): T[A] = Freer.pure(a)
    def raise(e: E): T[Nothing] = Freer.raise(e)
    def lift[A](fa: F[A]): T[A] = Freer.lift(fa)
    val unit: T[Unit] = Freer.unit
    val none: T[None.type] = Freer.none
    def from[A](either: ErrOr[A]): T[A] = Freer.from(either)
    def assert(cond: Boolean)(e: => E): T[Unit] = if (cond) unit else raise(e)
  }

}
