// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.AsyncOrSyncCloseable
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SimpleExecutionQueue.TaskCell
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Functions executed with this class will only run when all previous calls have completed executing.
  * This can be used when async code should not be run concurrently.
  */
class SimpleExecutionQueue(logTaskTiming: Boolean = false)(implicit ec: ExecutionContext)
    extends PrettyPrinting {

  /** Will execute the given function after all previous executions have completed and return the
    * future with the result of this execution.
    */
  def execute[A](execution: => Future[A], description: String)(implicit
      loggingContext: ErrorLoggingContext
  ): Future[A] =
    genExecute(true, execution, description)

  def executeE[A, B](
      execution: => EitherT[Future, A, B],
      description: String,
  )(implicit loggingContext: ErrorLoggingContext): EitherT[Future, A, B] =
    EitherT(execute(execution.value, description))

  /** Executes the given function after all previous executions have completed successfully
    *
    * @return A future with the result of this execution or the last failure of an earlier execution
    */
  def executeUnlessFailed[A](execution: => Future[A], description: String)(implicit
      loggingContext: ErrorLoggingContext
  ): Future[A] =
    genExecute(false, execution, description)

  private def genExecute[A](
      runIfFailed: Boolean,
      execution: => Future[A],
      description: String,
  )(implicit loggingContext: ErrorLoggingContext): Future[A] = {
    val next = new TaskCell(description, logTaskTiming)
    val oldHead = queueHead.getAndSet(next) // linearization point
    next.chain(oldHead, runIfFailed, execution)
  }

  /** Returns a future that completes when all scheduled tasks up to now have completed. Never fails. */
  def flush(): Future[Unit] = queueHead.get().future.transform(_ => Success(()))

  private val queueHead: AtomicReference[TaskCell] =
    new AtomicReference[TaskCell](TaskCell.sentinel)

  /** slow and in-efficient queue size, to be used for inspection */
  def queueSize: Int = {
    @tailrec
    def go(cell: TaskCell, count: Int): Int = cell.predecessor match {
      case None => count
      case Some(predCell) => go(predCell, count + 1)
    }
    go(queueHead.get(), 0)
  }

  /** Returns a sequence of tasks' descriptions in this execution queue.
    * The first entry refers to the last known completed task,
    * the others are running or queued.
    */
  def queued: Seq[String] = {
    @tailrec
    def go(cell: TaskCell, descriptions: List[String]): List[String] = {
      cell.predecessor match {
        case None => s"${cell.description} (completed)" :: descriptions
        case Some(predCell) => go(predCell, cell.description :: descriptions)
      }
    }
    go(queueHead.get(), List.empty[String])
  }

  override def pretty: Pretty[SimpleExecutionQueue] = prettyOfClass(
    param("queued tasks", _.queued.map(_.unquoted))
  )

  /** Await all tasks to complete.
    * Does not await tasks that are added to the queue after this method has been called.
    *
    * @throws scala.concurrent.TimeoutException if the tasks did not complete within the `timeout`.
    */
  def asCloseable(name: String, timeout: Duration)(implicit
      loggingContext: ErrorLoggingContext
  ): AsyncOrSyncCloseable =
    new AsyncOrSyncCloseable {
      override def close(): Unit = FutureUtil.noisyAwaitResult(
        flush(),
        show"simple execution queue ${name.unquoted} with tasks ${queued.map(_.unquoted)}",
        timeout,
      )

      override def toString: String = s"SimpleExecutionQueueCloseable(name=${name})"
    }
}

object SimpleExecutionQueue {

  /** Implements the chaining of tasks and their descriptions. */
  private class TaskCell(val description: String, logTaskTiming: Boolean) {

    /** Completes after all earlier tasks and this task have completed.
      * Fails with the exception of the first task that failed, if any.
      */
    private val completionPromise: Promise[Unit] = Promise[Unit]()

    /** `null` if no predecessor has been chained.
      * [[scala.Some$]]`(cell)` if the predecessor task is `cell` and this task is queued or running.
      * [[scala.None$]] if this task has been completed.
      */
    private val predecessorCell: AtomicReference[Option[TaskCell]] =
      new AtomicReference[Option[TaskCell]]()

    private val taskCreationTime: Long = if (logTaskTiming) System.nanoTime() else 0L

    /** Chains this task cell after its predecessor `pred`. */
    /* The linearization point in the caller `genExecute` has already determined the sequencing of tasks
     * if they are enqueued concurrently. So it now suffices to make sure that this task's future executes after
     * `pred` (unless the previous task's future failed and `runIfFailed` is false) and that
     * we cut the chain to the predecessor thereafter.
     */
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    def chain[A](
        pred: TaskCell,
        runIfFailed: Boolean,
        execution: => Future[A],
    )(implicit
        ec: ExecutionContext,
        loggingContext: ErrorLoggingContext,
    ): Future[A] = {
      val succeed = predecessorCell.compareAndSet(null, Some(pred))
      ErrorUtil.requireState(succeed, s"Attempt to chain task $description several times.")(
        loggingContext
      )

      def runTask(propagatedException: Option[Throwable]): Future[(Option[Throwable], A)] = {
        if (logTaskTiming && loggingContext.logger.underlying.isDebugEnabled) {
          implicit val traceContext: TraceContext = loggingContext.traceContext
          val startTime = System.nanoTime()
          val waitingDelay = Duration.fromNanos(startTime - taskCreationTime)
          loggingContext.logger.debug(
            show"Running task ${description.singleQuoted} after waiting for $waitingDelay"
          )
          execution.transform { result =>
            val finishTime = System.nanoTime()
            val runningDuration = Duration.fromNanos(finishTime - startTime)
            loggingContext.logger.debug(
              show"Task ${description.singleQuoted} finished after $runningDuration running time and $waitingDelay waiting time"
            )
            result.map(a => (propagatedException, a))
          }
        } else {
          execution.map(a => (propagatedException, a))
        }
      }

      val chained = pred.future.transformWith {
        case Success(_) => runTask(None)
        case Failure(ex) =>
          // Propagate the exception `ex` from an earlier task
          if (runIfFailed) runTask(Some(ex))
          else {
            if (logTaskTiming && loggingContext.logger.underlying.isDebugEnabled) {
              val startTime = System.nanoTime()
              val waitingDelay = Duration.fromNanos(startTime - taskCreationTime)
              loggingContext.logger.debug(
                s"Not running task ${description.singleQuoted} due to exception after waiting for $waitingDelay"
              )(loggingContext.traceContext)
            }
            Future.failed(ex)
          }
      }
      val completed = chained.thereafter { _ =>
        // Cut the predecessor as we're now done.
        predecessorCell.set(None)
      }
      val propagatedException = completed.flatMap { case (earlierExceptionO, _) =>
        earlierExceptionO.fold(Future.unit)(Future.failed)
      }
      completionPromise.completeWith(propagatedException)
      completed.map(_._2)
    }

    /** The returned future completes after this task has completed.
      * If the task is not supposed to run if an earlier task has failed,
      * then this task completes when all earlier tasks have completed without being actually run.
      */
    def future: Future[Unit] = completionPromise.future

    /** Returns the predecessor task's cell or [[scala.None$]] if this task has already been completed. */
    def predecessor: Option[TaskCell] = {
      // Wait until the predecessor cell has been set.
      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      @tailrec def go(): Option[TaskCell] = {
        val pred = predecessorCell.get()
        if (pred eq null) go() else pred
      }
      go()
    }

  }

  private object TaskCell {

    /** Sentinel task cell that is already completed. */
    val sentinel: TaskCell = {
      val cell = new TaskCell("sentinel", false)
      cell.predecessorCell.set(None)
      cell.completionPromise.success(())
      cell
    }
  }
}
