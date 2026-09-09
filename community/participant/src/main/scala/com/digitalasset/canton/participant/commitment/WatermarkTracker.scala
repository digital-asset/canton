// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, Mutex, Thereafter}
import com.google.common.annotations.VisibleForTesting

import java.util
import scala.concurrent.{Future, Promise}

/** Keeps track of a boundary [[WatermarkTracker.highWatermark]] that increases monotonically.
  * Clients can do one of the following:
  *
  *   - Use the [[WatermarkTracker]] to execute a task associated with a given mark `mark` >=
  *     [[WatermarkTracker.highWatermark]]. If `mark` < [[WatermarkTracker.highWatermark]], the task
  *     will executed at `highWatermark` instead.
  *   - Use the tracker to increase the [[WatermarkTracker.highWatermark]]. Notify the caller as
  *     soon as all tasks with `mark` < [[WatermarkTracker.highWatermark]] have finished.
  *
  * The [[WatermarkTracker]] is effectively used to enforce mutual exclusion between two types of
  * tasks.
  *   - Tasks of type 1 act on data with `mark` >= [[WatermarkTracker.highWatermark]].
  *   - Tasks of type 2 act on data with `mark` < [[WatermarkTracker.highWatermark]].
  *
  * @param initialWatermark
  *   The initial value for [[WatermarkTracker.highWatermark]]
  */
class WatermarkTracker[Mark: Pretty](
    initialWatermark: Mark,
    protected override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit private val ordering: Ordering[Mark])
    extends NamedLogging {

  /** The high-watermark boundary, increases monotonically.
    *
    * Access must be guarded by [[lock]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var highWatermarkV: Mark = initialWatermark

  /** Associates marks to a promise. The promise completes after there are no [[runningTasks]] below
    * this mark.
    *
    * All keys are below [[highWatermark]].
    *
    * Access must be guarded by [[lock]].
    */
  private val waitForTasksFinishing: util.NavigableMap[Mark, Promise[Unit]] =
    new util.TreeMap[Mark, Promise[Unit]]()

  /** Counts the number of running tasks for a given mark. New tasks are added only if their mark is
    * at or above [[highWatermark]].
    *
    * Invariant: all values are positive integers.
    *
    * Access must be guarded by [[lock]].
    */
  private val runningTasks: util.NavigableMap[Mark, Int] = new util.TreeMap[Mark, Int]()

  /** Used to synchronize access to the mutable fields */
  // This data structure could probably be implemented without locks,
  // but it doesn't seem worth the effort for now.
  private val lock: Mutex = new Mutex

  /** Returns the current value of the watermark. */
  def highWatermark: Mark = lock.exclusive(highWatermarkV)

  /** Run a task `task`. The task gets `mark` if `mark` > [[highWatermark]] and [[highWatermark]]
    * otherwise.
    *
    * @return
    *   the result of running `task`
    * @throws java.lang.IllegalStateException
    *   if there are already `Int.MaxValue` tasks running for `mark`
    */
  def runWithMark[F[_], A](mark: Mark, register: Mark => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): F[A] = {
    val registeredMark = registerBegin(mark)
    register(registeredMark).thereafter { _ =>
      checked(registerEnd(registeredMark))
    }
  }

  /** Record that a task with mark `mark` wants to start. Registers the task under `mark` if `mark`
    * >= [[highWatermark]] and under [[highWatermark]] otherwise.
    *
    * @return
    *   the mark under which the task was registered
    * @throws java.lang.IllegalStateException
    *   if there are already `Int.MaxValue` tasks with the bumped mark running.
    */
  @VisibleForTesting
  private[commitment] def registerBegin(mark: Mark)(implicit traceContext: TraceContext): Mark =
    lock.exclusive {
      val bumpedMark = ordering.max(highWatermarkV, mark)
      Option(runningTasks.get(bumpedMark)) match {
        case None => runningTasks.put(bumpedMark, 1).discard
        case Some(count) =>
          ErrorUtil.requireState(
            count < Int.MaxValue,
            show"Overflow: already ${Int.MaxValue} running tasks for $bumpedMark.",
          )
          runningTasks.put(bumpedMark, count + 1).discard
      }
      bumpedMark
    }

  /** Record that a task registered under mark `mark` has finished.
    *
    * @throws java.lang.IllegalStateException
    *   if there is no running task for `mark`
    */
  @VisibleForTesting
  private[commitment] def registerEnd(mark: Mark)(implicit traceContext: TraceContext): Unit =
    lock.exclusive {
      val count = Option(runningTasks.get(mark)).getOrElse(
        ErrorUtil.internalError(new IllegalStateException(s"No running tasks for $mark"))
      )
      if (count == 1) runningTasks.remove(mark).discard
      else runningTasks.put(mark, count - 1).discard
      drainAndNotify()
    }

  /** Increases the [[highWatermark]] to `mark` unless it was higher previously.
    *
    * @return
    *   The future completes after there are no running tasks with marks up to `mark` exclusive.
    */
  def increaseWatermark(mark: Mark)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val promise = lock.exclusive {
      highWatermarkV = ordering.max(highWatermarkV, mark)
      val promise = new SupervisedPromise[Unit]("increase-watermark", futureSupervisor)
      val previousO = Option(waitForTasksFinishing.put(mark, promise))
      previousO.foreach(_.completeWith(promise.future))
      drainAndNotify()
      promise
    }
    promise.future
  }

  /** Remove all entries from [[waitForTasksFinishing]] and complete the associated promise up to
    * the first [[runningTasks]] timestamp inclusive (if it exists) or the [[highWatermarkV]]
    * inclusive, whatever is lower.
    *
    * The caller must have acquired [[lock]].
    */
  private def drainAndNotify(): Unit = {
    import scala.jdk.CollectionConverters.*
    val upto =
      // If there are no tasks or the first task's mark is at least `highWatermark`,
      // we can complete all promises because all marks in `waitForTasksFinishing`
      // are below `highWatermark` by the invariant.
      if (runningTasks.isEmpty) waitForTasksFinishing
      else {
        val firstTaskMark = checked(runningTasks.firstKey)
        if (ordering.gteq(firstTaskMark, highWatermarkV)) waitForTasksFinishing
        else waitForTasksFinishing.headMap(firstTaskMark, true)
      }

    for (promise <- upto.values.asScala) { promise.success(()) }
    upto.clear()
  }
}
