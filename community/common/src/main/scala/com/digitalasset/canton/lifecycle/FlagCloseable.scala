// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable.forceShutdownStr
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax._
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.immutable.MultiSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Trait that can be registered with a [FlagCloseable] to run on shutdown */
trait RunOnShutdown {

  /** the name, used for logging during shutdown */
  def name: String

  /** true if the task has already run (maybe elsewhere) */
  def done: Boolean

  /** invoked by [FlagCloseable] during shutdown */
  def run(): Unit
}

/** Provides a way to synchronize closing with other running tasks in the class, such that new tasks aren't scheduled
  * while closing, and such that closing waits for the scheduled tasks.
  *
  * The component's custom shutdown behaviour should override the `onClosed` method.
  */
trait FlagCloseable extends AutoCloseable {

  protected def timeouts: ProcessingTimeout

  protected def closingTimeout: FiniteDuration = timeouts.closing.asFiniteApproximation

  private val closingFlag = new AtomicBoolean(false)

  case class ReaderState(count: Int, readers: MultiSet[String])
  object ReaderState {
    def empty: ReaderState = ReaderState(0, MultiSet.empty)
  }

  // Poor man's read-write lock; stores the number of tasks holding the read lock. If a write lock is held, this
  // goes to -1. Not using Java's ReadWriteLocks since they are about thread synchronization, and since we can't
  // count on acquires and releases happening on the same thread, since we support the synchronization of futures.
  private val readerState = new AtomicReference(ReaderState.empty)

  private val onShutdownTasks =
    new AtomicReference[List[RunOnShutdown]](List())

  protected def logger: TracedLogger

  // How often to poll to check that all tasks have completed.
  protected def maxSleepMillis: Long = 500

  @VisibleForTesting
  protected def runStateChanged(waitingState: Boolean = false): Unit = {} // used for unit testing

  /** Register a task that will run onClose after all "performUnlessShutdown" tasks have finished */
  def runOnShutdown[T](
      task: RunOnShutdown
  )(implicit traceContext: TraceContext): Unit = {
    onShutdownTasks.updateAndGet { seq => seq.filterNot(_.done) :+ task }.discard

    if (isClosing) runOnShutdownTasks()
  }

  private def runOnShutdownTasks()(implicit traceContext: TraceContext): Unit = {
    val tasks = onShutdownTasks.getAndSet(List())
    tasks.foreach { task =>
      if (!task.done) {
        Try { task.run() }.recover { t =>
          logger.warn(s"Task ${task.name} failed on shutdown!", t)
        }
      }
    }
  }

  /** Check whether we're closing.
    * Susceptible to race conditions; unless you're using using this as a flag to the retry lib or you really know
    * what you're doing, prefer [[performUnlessClosing]] and friends.
    */
  def isClosing: Boolean = closingFlag.get()

  /** Performs the task given by `f` unless a shutdown has been initiated.
    * The shutdown will only begin after `f` completes, but other tasks may execute concurrently with `f`, if started using this
    * function, or one of the other variants ([[performUnlessClosingF]] and [[performUnlessClosingEitherT]]).
    * The tasks are assumed to take less than [[closingTimeout]] to complete.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock.
    *
    * @param f The task to perform
    * @return [[scala.None$]] if a shutdown has been initiated. Otherwise the result of the task.
    */
  def performUnlessClosing[A](
      name: String
  )(f: => A)(implicit traceContext: TraceContext): UnlessShutdown[A] = {
    if (isClosing || !addReader(name)) {
      logger.debug(s"Won't schedule the task '$name' as this object is closing")
      UnlessShutdown.AbortedDueToShutdown
    } else
      try {
        UnlessShutdown.Outcome(f)
      } finally {
        removeReader(name)
      }
  }

  /** Performs the Future given by `f` unless a shutdown has been initiated. The future is lazy and not evaluated during shutdown.
    * The shutdown will only begin after `f` completes, but other tasks may execute concurrently with `f`, if started using this
    * function, or one of the other variants ([[performUnlessClosing]] and [[performUnlessClosingEitherT]]).
    * The tasks are assumed to take less than [[closingTimeout]] to complete.
    *
    * DO NOT CALL `this.close` as part of `f`, because it will result in a deadlock.
    *
    * @param f The task to perform
    * @return The future completes with [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if
    *         a shutdown has been initiated.
    *         Otherwise the result of the task wrapped in [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]].
    */
  def performUnlessClosingF[A](name: String)(
      f: => Future[A]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): FutureUnlessShutdown[A] =
    FutureUnlessShutdown(internalPerformUnlessClosingF(name)(f).sequence)

  protected def internalPerformUnlessClosingF[A](name: String)(
      f: => Future[A]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): UnlessShutdown[Future[A]] = {
    if (isClosing || !addReader(name)) {
      logger.debug(s"Won't schedule the future '$name' as this object is closing")
      UnlessShutdown.AbortedDueToShutdown
    } else {
      val fut = Try(f).fold(Future.failed, x => x).thereafter { _ =>
        removeReader(name)
      }
      trackFuture(fut)
      UnlessShutdown.Outcome(fut)
    }
  }

  /** Performs the EitherT[Future] given by `etf` unless a shutdown has been initiated, in which case the provided error is returned instead.
    * Both `etf` and the error are lazy; `etf` is only evaluated if there is no shutdown, the error only if we're shutting down.
    * The shutdown will only begin after `etf` completes, but other tasks may execute concurrently with `etf`, if started using this
    * function, or one of the other variants ([[performUnlessClosing]] and [[performUnlessClosingF]]).
    * The tasks are assumed to take less than [[closingTimeout]] to complete.
    *
    * DO NOT CALL `this.close` as part of `etf`, because it will result in a deadlock.
    *
    * @param etf The task to perform
    */
  def performUnlessClosingEitherT[E, R](name: String, onClosing: => E)(
      etf: => EitherT[Future, E, R]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, E, R] = {
    EitherT(performUnlessClosingF(name)(etf.value).unwrap.map(_.onShutdown(Left(onClosing))))
  }

  def performUnlessClosingEitherU[E, R](name: String)(
      etf: => EitherT[Future, E, R]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, E, R] = {
    EitherT(performUnlessClosingF(name)(etf.value))
  }

  def performUnlessClosingEitherTF[E, R](name: String, onClosing: => E)(
      etf: => EitherT[Future, E, Future[R]]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): EitherT[Future, E, Future[R]] = {
    if (isClosing || !addReader(name)) {
      logger.debug(s"Won't schedule the future '$name' as this object is closing")
      EitherT.leftT(onClosing)
    } else {
      val res = Try(etf.value).fold(Future.failed, x => x)
      trackFuture(res)
      val _ = res
        .flatMap {
          case Left(_) => Future.unit
          case Right(value) => value.map(_ => ())
        }
        .thereafter { _ =>
          removeReader(name)
        }
      EitherT(res)
    }
  }

  private case class RunningFuture(fut: Future[Any], location: Exception)

  /** track running futures on shutdown
    *
    * set to true to get detailed information about all futures that did not complete during
    * shutdown. if set to false, we don't do anything.
    */
  protected def keepTrackOfOpenFutures: Boolean = false

  private val scheduled = new AtomicReference[Seq[RunningFuture]](Seq())
  private def trackFuture(fut: Future[Any])(implicit executionContext: ExecutionContext): Unit =
    if (keepTrackOfOpenFutures) {
      val ex = new Exception("location")
      Future {
        scheduled
          .updateAndGet(x => x.filterNot(_.fut.isCompleted) :+ RunningFuture(fut, ex))
      }.discard
    }
  private def dumpRunning()(implicit traceContext: TraceContext): Unit = {
    scheduled.updateAndGet(x => x.filterNot(_.fut.isCompleted)).foreach { cur =>
      logger.debug("Future created from here is still running", cur.location)
    }
  }

  protected def onClosed(): Unit = ()

  /** Blocks until all earlier tasks have completed and then prevents further tasks from being run.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Var"))
  final override def close(): Unit = {
    import TraceContext.Implicits.Empty._

    /* Setting closingFlag to true first ensures that we can shut down cleanly, unless one of the
       readers takes longer to complete than the closing timeout. After the flag is set to true, the readerCount
       can only decrease (since it only increases in performUnlessClosingF, and since the || there short-circuits).
     */
    val firstCallToClose = closingFlag.compareAndSet(false, true)
    runStateChanged()
    if (firstCallToClose) {
      // First run onShutdown tasks.
      // Important to run them in the beginning as they may be used to cancel long-running tasks.
      runOnShutdownTasks()

      // Poll for tasks to finish. Inefficient, but we're only doing this during shutdown.
      val deadline = closingTimeout.fromNow
      var sleepMillis = 1L
      while (
        (readerState.getAndUpdate { current =>
          if (current == ReaderState.empty) {
            current.copy(count = -1)
          } else current
        }.count != 0) && deadline.hasTimeLeft()
      ) {
        val readers = readerState.get()
        logger.debug(
          s"${readers.count} active tasks (${readers.readers.mkString(",")}) preventing closing; sleeping for ${sleepMillis}ms"
        )
        runStateChanged(true)
        Threading.sleep(sleepMillis)
        sleepMillis = (sleepMillis * 2) min maxSleepMillis min deadline.timeLeft.toMillis
      }
      if (readerState.get.count >= 0) {
        logger.warn(
          s"Timeout ${closingTimeout} expired, but tasks still running. ${forceShutdownStr}"
        )
        dumpRunning()
      }
      if (keepTrackOfOpenFutures) {
        logger.warn("Tracking of open futures is enabled, but this is only meant for debugging!")
      }
      onClosed()
    } else {
      // TODO(i8594): Ensure we call close only once
    }
  }

  private def addReader(reader: String): Boolean =
    (readerState.updateAndGet { case state @ ReaderState(cnt, readers) =>
      if (cnt == Int.MaxValue)
        throw new IllegalStateException("Overflow on active reader locks")
      if (cnt >= 0) {
        ReaderState(cnt + 1, readers + reader)
      } else state
    }).count > 0

  private def removeReader(reader: String): Unit = {
    val _ = readerState.updateAndGet { case ReaderState(cnt, readers) =>
      if (cnt <= 0)
        throw new IllegalStateException("No active readers, but still trying to deactivate one")
      ReaderState(cnt - 1, readers - reader)
    }
  }
}

object FlagCloseable {
  def apply(tracedLogger: TracedLogger, timeoutsArgs: ProcessingTimeout): FlagCloseable =
    new FlagCloseable {
      override protected def logger: TracedLogger = tracedLogger
      override protected def timeouts: ProcessingTimeout = timeoutsArgs
    }

  /** Logged upon forced shutdown. Pulled out a string here so that test log checking can refer to it. */
  val forceShutdownStr = "Shutting down forcibly"
}

/** Context to capture and pass through a caller's closing state.
  *
  * This allows us for example to stop operations down the call graph if either the caller or the current component
  * executing an operation is closed.
  */
final case class CloseContext(flagCloseable: FlagCloseable)

/** Mix-in to obtain a [[CloseContext]] implicit based on the class's [[FlagCloseable]] */
trait HasCloseContext { self: FlagCloseable =>
  implicit val closeContext: CloseContext = CloseContext(self)
}
