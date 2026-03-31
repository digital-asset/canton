// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.test

import io.grpc.Context
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

final class StreamConsumer[A](attach: StreamObserver[A] => Unit) {

  private def attachWithCancelContext(f: CancellableContext => StreamObserver[A]): Unit = {
    val context = Context.ROOT.withCancellation()
    context.run(() => attach(f(context)))
  }

  /** THIS WILL NEVER COMPLETE IF FED AN UNBOUND STREAM!!!
    */
  def all(): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attach(observer)
    observer.result
  }

  /** Filters the items coming via the observer and takes the first N.
    */
  def filterTake(predicate: A => Boolean)(sizeCap: Int): Future[Vector[A]] = {
    val finiteObserver = new FiniteStreamObserver[A]
    attachWithCancelContext(context =>
      new ObserverFilter(predicate)(
        new SizeBoundObserver(sizeCap, finiteObserver, context)
      )
    )
    finiteObserver.result
  }

  def take(sizeCap: Int): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attachWithCancelContext(new SizeBoundObserver(sizeCap, observer, _))
    observer.result
  }

  def find(predicate: A => Boolean)(implicit ec: ExecutionContext): Future[A] =
    filterTake(predicate)(sizeCap = 1).map(_.head)

  def first()(implicit ec: ExecutionContext): Future[Option[A]] =
    take(1).map(_.headOption)

  def within(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attachWithCancelContext(new TimeBoundObserver(duration, observer, _))
    observer.result
  }

  def firstWithin(duration: FiniteDuration)(implicit ec: ExecutionContext): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attachWithCancelContext(context =>
      new TimeBoundObserver(
        duration,
        new SizeBoundObserver(sizeCap = 1, observer, context),
        context,
      )
    )
    observer.result
  }

  def firstNWithin(duration: FiniteDuration, n: Int)(implicit
      ec: ExecutionContext
  ): Future[Vector[A]] = {
    val observer = new FiniteStreamObserver[A]
    attachWithCancelContext(context =>
      new TimeBoundObserver(
        duration,
        new SizeBoundObserver(sizeCap = n, observer, context),
        context,
      )
    )
    observer.result
  }

}
