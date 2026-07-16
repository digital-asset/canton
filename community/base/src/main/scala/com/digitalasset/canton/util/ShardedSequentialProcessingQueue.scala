// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, Nested}
import cats.syntax.functor.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** A processing queue that allows scheduling work associated with a particular identifier.
  *
  * It comes in two flavours:
  *   - [[GarbageCollectedShardedSequentialProcessingQueue]] when the number of identifiers is big
  *     and each identifier is usually short-lived.
  *   - [[NonGarbageCollectedShardedSequentialProcessingQueue]] when the number of identifiers is
  *     small and each identifier is potentially long-lived.
  *
  * @tparam Ident
  *   The type of the identifiers
  */
sealed trait ShardedSequentialProcessingQueue[Ident] {
  def executeUS[A](id: Ident)(action: => FutureUnlessShutdown[A], taskName: String)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[A]

  def executeEUS[A, B](id: Ident)(action: => EitherT[FutureUnlessShutdown, A, B], taskName: String)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, A, B] = EitherT(executeUS(id)(action.value, taskName))

  @VisibleForTesting
  def isQueueEmpty(id: Ident): Boolean

  @VisibleForTesting
  def areQueuesEmpty: Boolean
}

/** A processing queue that runs units of work for a particular identifier in a sequential manner,
  * but allows parallel processing of work for different identifiers. If a unit of work fails or
  * throws an exception, subsequent units of work for the same identifier are not executed.
  *
  * Use this queue when the number of identifiers is big and each identifier is usually short-lived.
  *
  * @tparam Ident
  *   The type of the identifiers
  */
class GarbageCollectedShardedSequentialProcessingQueue[Ident](implicit ec: ExecutionContext)
    extends ShardedSequentialProcessingQueue[Ident] {

  @VisibleForTesting
  val processingQueuePerId = new TrieMap[Ident, FutureUnlessShutdown[Unit]]()

  override def isQueueEmpty(id: Ident): Boolean = !processingQueuePerId.isDefinedAt(id)
  override def areQueuesEmpty: Boolean = processingQueuePerId.isEmpty

  override def executeUS[A](
      id: Ident
  )(action: => FutureUnlessShutdown[A], taskName: String)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[A] = {
    val processingPromise: PromiseUnlessShutdown[Unit] = PromiseUnlessShutdown.unsupervised()
    val processingFuture = processingPromise.futureUS

    val previousProcessingFuture: FutureUnlessShutdown[Unit] = processingQueuePerId
      .put(id, processingFuture)
      .getOrElse(FutureUnlessShutdown.unit)

    previousProcessingFuture.flatMap(_ => action).thereafter { result =>
      processingPromise.complete(Nested(result).void.value)
      // cleanup the processing queue
      processingQueuePerId
        .updateWith(id) {
          case Some(`processingFuture`) =>
            // if the "processing queue" still contains the same future that we put in, we can remove the entry from the map
            None
          case Some(other) =>
            // some other future was put into the map, retain it
            Some(other)
          case None =>
            // the entry was already removed, nothing to do
            None
        }
        .discard

    }
  }
}

/** A processing queue that runs units of work for a particular identifier in a sequential manner,
  * but allows parallel processing of work for different identifiers. For an identifier, the
  * behavior of the queue when one task fails or throws an exception depends on the failureMode.
  *
  * If a unit of work fails or throws an exception, subsequent units of work for the same identifier
  * are not executed.
  *
  * IMPORTANT NOTE: The underlying execution queues for the identifiers are not cleaned up. If you
  * have many short-lived identifier, then [[GarbageCollectedShardedSequentialProcessingQueue]]
  * should be preferred.
  *
  * Use this queue when the number of identifiers is small and each identifier is potentially
  * long-lived.
  *
  * @param name
  *   For logging purposes
  * @param logTaskTiming
  *   If true logs wait and run time for each of the tasks
  * @param failureMode
  *   How the queue handles the execution of tasks after a previous task had failed
  * @tparam Ident
  *   The type of the identifiers
  */
class NonGarbageCollectedShardedSequentialProcessingQueue[Ident: Pretty](
    private val name: String,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    private val logTaskTiming: Boolean,
    failureMode: FailureMode,
) extends ShardedSequentialProcessingQueue[Ident]
    with PrettyPrinting
    with NamedLogging
    with FlagCloseable {

  private val processingQueues = new TrieMap[Ident, SimpleExecutionQueue]()

  override def isQueueEmpty(id: Ident): Boolean = processingQueues.get(id).fold(true)(_.isEmpty)
  override def areQueuesEmpty: Boolean = processingQueues.forall { case (_, queue) =>
    queue.isEmpty
  }

  override def executeUS[A](
      id: Ident
  )(action: => FutureUnlessShutdown[A], taskName: String)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[A] = {

    val queue = processingQueues.getOrElseUpdate(
      id,
      new SimpleExecutionQueue(
        name = s"$name-$id",
        futureSupervisor = futureSupervisor,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
        logTaskTiming = logTaskTiming,
        failureMode = failureMode,
      ),
    )

    queue.executeUS(action, taskName)
  }

  override protected def pretty
      : Pretty[NonGarbageCollectedShardedSequentialProcessingQueue[Ident]] =
    prettyOfClass(
      param("tasks", _.processingQueues)
    )

  override protected def onClosed(): Unit =
    // close() (not onClosed()) so each shard queue flushes and runs its close hooks
    LifeCycle.close(processingQueues.readOnlySnapshot().values.toSeq*)(logger)
}
