// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.executors.executors

import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}
import scala.concurrent.ExecutionContextExecutorService

class NamedExecutionContextExecutorService(
    protected val delegate: ExecutorService,
    val name: String,
    reporter: Throwable => Unit,
) extends ExecutionContextExecutorService
    with NamedExecutor {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def reportFailure(cause: Throwable): Unit = reporter(cause)

  override def shutdown(): Unit = {
    logger.info(s"Shutting down executor service: $name")
    delegate.shutdown()
  }
  override def shutdownNow(): util.List[Runnable] = {
    logger.info(s"Shutting down executor service now: $name")
    delegate.shutdownNow()
  }
  override def isShutdown: Boolean = delegate.isShutdown
  override def isTerminated: Boolean = delegate.isTerminated
  override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean =
    delegate.awaitTermination(l, timeUnit)

  override def submit[T](callable: Callable[T]): Future[T] =
    delegate.submit(callable)

  override def submit[T](runnable: Runnable, t: T): Future[T] =
    delegate.submit(runnable, t)

  override def submit(runnable: Runnable): Future[?] =
    delegate.submit(runnable)

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]]
  ): util.List[Future[T]] =
    delegate.invokeAll(collection)

  override def invokeAll[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): util.List[Future[T]] =
    delegate.invokeAll(collection, l, timeUnit)

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]]
  ): T =
    delegate.invokeAny(collection)

  override def invokeAny[T](
      collection: util.Collection[? <: Callable[T]],
      l: Long,
      timeUnit: TimeUnit,
  ): T =
    delegate.invokeAny(collection, l, timeUnit)

  override def execute(runnable: Runnable): Unit =
    if (!isShutdown) {
      delegate.execute(runnable)
    } else {
      // Avoid RejectedExecutionException on shutdown
      logger.debug(
        s"Executor service $name is shutdown [terminated=${delegate.isTerminated}], discarding task: $runnable"
      )
    }
}
