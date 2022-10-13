// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.foldable.*
import cats.syntax.traverse.*
import cats.{Monad, Monoid}

import scala.annotation.tailrec

object MonadUtil {

  /** The caller must ensure that the underlying data structure of the iterator is immutable */
  def foldLeftM[M[_], S, A](initialState: S, iter: Iterator[A])(
      step: (S, A) => M[S]
  )(implicit monad: Monad[M]): M[S] =
    monad.tailRecM[S, S](initialState) { state =>
      if (iter.hasNext) {
        monad.map(step(state, iter.next()))(newState => Left(newState))
      } else monad.pure(Right(state))
    }

  /** The caller must ensure that the Iterable is immutable */
  def foldLeftM[M[_], S, A](initialState: S, xs: Iterable[A])(step: (S, A) => M[S])(implicit
      monad: Monad[M]
  ): M[S] =
    foldLeftM(initialState, xs.iterator)(step)

  /** The implementation of `traverse` in `cats` is parallel, so this provides a sequential alternative.
    * The caller must ensure that the Iterable is immutable
    */
  def sequentialTraverse_[M[_], A](xs: Iterable[A])(step: A => M[_])(implicit
      monad: Monad[M]
  ): M[Unit] =
    sequentialTraverse_(xs.iterator)(step)

  /** The caller must ensure that the underlying data structure of the iterator is immutable */
  def sequentialTraverse_[M[_], A](xs: Iterator[A])(step: A => M[_])(implicit
      monad: Monad[M]
  ): M[Unit] =
    foldLeftM((), xs)((_, x) => monad.void(step(x)))

  /** Repeatedly apply the same function to a monadic value `m`. This can be used to retry until the
    * limit `counter` is reached or the monad `m` aborts.
    */
  @tailrec
  def repeatFlatmap[M[_], A](m: M[A], f: A => M[A], counter: Int)(implicit
      monad: Monad[M]
  ): M[A] = {
    counter match {
      case 0 => m
      case n =>
        require(n > 0, s"Trying to repeat with negative counter: $n")
        val next = monad.flatMap(m)(f)
        repeatFlatmap(next, f, counter - 1)
    }
  }

  def sequentialTraverse[X, M[_], S](
      xs: Seq[X]
  )(f: X => M[S])(implicit monad: Monad[M]): M[Seq[S]] = {
    val result = foldLeftM(Seq.empty: Seq[S], xs)((ys, x) => monad.map(f(x))(y => y +: ys))
    monad.map(result)(seq => seq.reverse)
  }

  /** Batched version of sequential traverse
    *
    * Can be used to avoid overloading the database queue. Use e.g. maxDbConnections * 2
    * as parameter for parallelism to not overload the database queue but to make sufficient use
    * of the existing resources.
    */
  def batchedSequentialTraverse[X, M[_], S](parallelism: Int, chunkSize: Int)(
      xs: Seq[X]
  )(processChunk: Seq[X] => M[Seq[S]])(implicit
      monad: Monad[M]
  ): M[Seq[S]] =
    monad.map(
      sequentialTraverse(xs.grouped(chunkSize).grouped(parallelism).toSeq)(
        _.flatTraverse(processChunk)
      )
    )(
      _.flatten
    )

  def batchedSequentialTraverse_[X, M[_], S](parallelism: Int, chunkSize: Int)(
      xs: Seq[X]
  )(processChunk: Seq[X] => M[Unit])(implicit
      monad: Monad[M]
  ): M[Unit] = {
    sequentialTraverse_(xs.grouped(chunkSize).grouped(parallelism))(chunk =>
      chunk.toSeq.traverse_(processChunk)
    )
  }

  /** Conceptually equivalent to `sequentialTraverse(xs)(step).map(monoid.combineAll)`.
    * The caller must ensure that the Iterable is immutable.
    */
  def sequentialTraverseMonoid[M[_], A, B](
      xs: Iterable[A]
  )(step: A => M[B])(implicit monad: Monad[M], monoid: Monoid[B]): M[B] =
    foldLeftM[M, B, A](monoid.empty, xs) { (acc, x) =>
      monad.map(step(x))(monoid.combine(acc, _))
    }
}
