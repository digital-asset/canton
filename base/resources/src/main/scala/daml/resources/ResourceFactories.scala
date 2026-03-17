// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import com.daml.resources.HasExecutionContext.executionContext

import scala.concurrent.Future
import scala.util.Try

final class ResourceFactories[Context: HasExecutionContext] {

  private type R[+T] = Resource[Context, T]

  /** Builds a [[com.daml.resources.Resource]] from a [[scala.concurrent.Future]] and some release
    * logic.
    */
  def apply[T](
      future: Future[T]
  )(releaseResource: T => Future[Unit])(implicit context: Context): R[T] =
    ReleasableResource(future)(releaseResource)

  /** Wraps a simple [[scala.concurrent.Future]] in a [[com.daml.resources.Resource]] that doesn't
    * need to be released.
    */
  def fromFuture[T](future: Future[T]): R[T] =
    PureResource(future)

  /** Wraps a simple [[scala.util.Try]] in a [[com.daml.resources.Resource]] that doesn't need to be
    * released.
    */
  def fromTry[T](result: Try[T]): R[T] =
    PureResource(Future.fromTry(result))

  /** Produces a [[com.daml.resources.Resource]] that has already succeeded with the [[scala.Unit]]
    * value.
    */
  def unit: R[Unit] =
    PureResource(Future.unit)

  /** Produces a [[com.daml.resources.Resource]] that has already succeeded with a given value.
    */
  def successful[T](value: T): R[T] =
    PureResource(Future.successful(value))

  /** Produces a [[com.daml.resources.Resource]] that has already failed with a given exception.
    */
  def failed[T](exception: Throwable): R[T] =
    PureResource(Future.failed(exception))

  /** Sequences a [[scala.collection.Iterable]] of [[com.daml.resources.Resource]]s into a
    * [[com.daml.resources.Resource]] of the [[scala.collection.Iterable]] of their values.
    *
    * @param seq
    *   The [[scala.collection.Iterable]] of [[com.daml.resources.Resource]]s.
    * @param bf
    *   The projection from a [[scala.collection.Iterable]] of resources into one of their values.
    * @param context
    *   The asynchronous task execution engine.
    * @tparam T
    *   The value type.
    * @tparam C
    *   The [[scala.collection.Iterable]] actual type.
    * @tparam U
    *   The return type.
    * @return
    *   A [[com.daml.resources.Resource]] with a sequence of the values of the sequenced
    *   [[com.daml.resources.Resource]]s as its underlying value.
    */
  def sequence[T, C[X] <: Iterable[X], U](seq: C[R[T]])(implicit
      bf: collection.Factory[T, U],
      context: Context,
  ): R[U] = new R[U] {
    private val resource = seq
      .foldLeft(successful(bf.newBuilder))((builderResource, elementResource) =>
        for {
          builder <- builderResource // Consider the builder in the accumulator resource
          element <- elementResource // Consider the value in the actual resource element
        } yield builder += element
      ) // Append the element to the builder
      .map(_.result()) // Yield a resource of collection resulting from the builder

    override def asFuture: Future[U] =
      resource.asFuture

    override def release(): Future[Unit] =
      Future.sequence(seq.map(_.release())).map(_ => ())
  }

  /** Sequences a [[scala.collection.Iterable]] of [[com.daml.resources.Resource]]s into a
    * [[com.daml.resources.Resource]] with no underlying value.
    *
    * @param seq
    *   The [[scala.collection.Iterable]] of [[com.daml.resources.Resource]]s.
    * @param context
    *   The asynchronous task execution engine.
    * @tparam T
    *   The value type.
    * @tparam C
    *   The [[scala.collection.Iterable]] actual type.
    * @return
    *   A [[com.daml.resources.Resource]] sequencing the [[com.daml.resources.Resource]]s and no
    *   underlying value.
    */
  def sequenceIgnoringValues[T, C[X] <: Iterable[X]](seq: C[R[T]])(implicit
      context: Context
  ): R[Unit] =
    sequence(seq)(new UnitCanBuildFrom[T, Nothing], context)

}
