// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.lifecycle.SyncCloseable
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Success

/** Provides a single flush [[scala.concurrent.Future]] that runs asynchronously. Tasks can be chained onto the flush
  *  future, although they will not run sequentially.
  */
trait HasFlushFuture
    extends
    // This trait must come after the NamedLogging trait in the class linearization to avoid initialization issue
    // with NamedLogging.logger. We therefore explicitly extend NamedLogging and do not declare it as a self type.
    NamedLogging {

  /** Adds the task `future` to the flush future so that [[doFlush]] completes only after `future` has completed */
  protected def addToFlush(name: String)(future: Future[_]): Unit =
    if (future.isCompleted) ()
    else {
      val promise = Promise[Unit]()
      val newTask = new HasFlushFuture.NamedTask(name, promise.future)
      tasks.put(newTask, ())
      // Make sure to remove the task again when the future is done.
      // This runs via a direct execution context as part of the task's execution context
      // so that we don't have to worry about execution contexts being closed here.
      val removeF = future.transform { _ =>
        tasks.remove(newTask)
        Success(())
      }(directExecutionContext)
      promise.completeWith(removeF)
    }

  /** Returns a future that completes after all added futures have completed. The returned future never fails. */
  protected def doFlush(): Future[Unit] = {
    val snapshot = tasks.readOnlySnapshot().keys
    flushFutureForSnapshot(snapshot)
  }

  // Invariant: The contained futures never fail with an exception
  private val tasks: TrieMap[HasFlushFuture.NamedTask, Unit] =
    TrieMap.empty[HasFlushFuture.NamedTask, Unit]

  private val directExecutionContext: ExecutionContext = DirectExecutionContext(logger)

  protected def flushCloseable(name: String, timeout: TimeoutDuration): SyncCloseable = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val snapshot = tasks.readOnlySnapshot().keys
    // It suffices to build the flush future only once,
    // but for pretty-printing we want to build the description for each log message
    // so that we can filter out the already completed tasks.
    val future = flushFutureForSnapshot(snapshot)
    def mkDescription(): String = {
      s"$name with tasks ${snapshot.filter(!_.future.isCompleted).mkString(", ")}"
    }
    SyncCloseable(name, timeout.await_(mkDescription())(future))
  }

  private def flushFutureForSnapshot(snapshot: Iterable[HasFlushFuture.NamedTask]): Future[Unit] = {
    snapshot.foldLeft(Future.unit) { (acc, task) =>
      val future = task.future
      if (future.isCompleted) acc
      else {
        acc.zipWith(future)((_, _) => ())(directExecutionContext)
      }
    }
  }
}

object HasFlushFuture {
  // Not a case class so that we get by-reference equality
  class NamedTask(val name: String, val future: Future[_]) extends PrettyPrinting {
    override def pretty: Pretty[NamedTask] =
      prettyOfString(x => if (x.future.isCompleted) x.name + " (completed)" else x.name)
  }
}
