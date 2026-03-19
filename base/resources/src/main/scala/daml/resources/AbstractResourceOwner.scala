// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** A ResourceOwner of type `A` is can acquire a [[com.daml.resources.Resource]] of the same type
  * and its operations are applied to the [[com.daml.resources.Resource]] after it has been
  * acquired.
  *
  * @tparam A
  *   The [[com.daml.resources.Resource]] value type.
  */
abstract class AbstractResourceOwner[Context: HasExecutionContext, +A] {
  self =>

  private type R[+T] = AbstractResourceOwner[Context, T]

  protected implicit def executionContext(implicit context: Context): ExecutionContext =
    HasExecutionContext.executionContext

  /** Acquires the [[com.daml.resources.Resource]].
    *
    * @param context
    *   The acquisition context, including the asynchronous task execution engine.
    * @return
    *   The acquired [[com.daml.resources.Resource]].
    */
  def acquire()(implicit context: Context): Resource[Context, A]

  /** @see [[com.daml.resources.Resource.map]] */
  def map[B](f: A => B): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().map(f)
  }

  /** @see [[com.daml.resources.Resource.flatMap]] */
  def flatMap[B](f: A => R[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().flatMap(value => f(value).acquire())
  }

  /** @see [[com.daml.resources.Resource.withFilter]] */
  def withFilter(p: A => Boolean): R[A] = new R[A] {
    override def acquire()(implicit context: Context): Resource[Context, A] =
      self.acquire().withFilter(p)
  }

  /** @see [[com.daml.resources.Resource.transform]] */
  def transform[B](f: Try[A] => Try[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().transform(f)
  }

  /** @see [[com.daml.resources.Resource.transformWith]] */
  def transformWith[B](f: Try[A] => R[B]): R[B] = new R[B] {
    override def acquire()(implicit context: Context): Resource[Context, B] =
      self.acquire().transformWith(result => f(result).acquire())
  }

  /** Acquire the [[com.daml.resources.Resource]]'s value, use it asynchronously, and release it
    * afterwards.
    *
    * @param behavior
    *   The asynchronous computation on the value.
    * @param context
    *   The acquisition context, including the asynchronous task execution engine.
    * @tparam T
    *   The asynchronous computation's value type.
    * @return
    *   The asynchronous computation's [[scala.concurrent.Future]].
    */
  def use[T](behavior: A => Future[T])(implicit context: Context): Future[T] = {
    val resource = acquire()
    resource.asFuture
      .flatMap(behavior)
      .transformWith { // Release the resource whether the computation succeeds or not
        case Success(value) => resource.release().map(_ => value)
        case Failure(exception) => resource.release().flatMap(_ => Future.failed(exception))
      }
  }
}
