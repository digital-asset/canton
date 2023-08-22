// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, RunnableGraph, Source}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{
  ActorAttributes,
  Attributes,
  FlowShape,
  Inlet,
  KillSwitch,
  KillSwitches,
  Materializer,
  Outlet,
  QueueCompletionResult,
  QueueOfferResult,
  Supervision,
  UniqueKillSwitch,
}
import akka.{Done, NotUsed}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{DirectExecutionContext, Threading}
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.typesafe.config.ConfigFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object AkkaUtil extends HasLoggerName {

  /** Utility function to run the graph supervised and stop on an unhandled exception.
    *
    * By default, an Akka flow will discard exceptions. Use this method to avoid discarding exceptions.
    */
  def runSupervised[T](
      reporter: Throwable => Unit,
      graph: RunnableGraph[T],
      debugLogging: Boolean = false,
  )(implicit
      mat: Materializer
  ): T = {
    val tmp = graph
      .addAttributes(ActorAttributes.withSupervisionStrategy { ex =>
        reporter(ex)
        Supervision.Stop
      })
    (if (debugLogging)
       tmp.addAttributes(ActorAttributes.debugLogging(true))
     else tmp)
      .run()
  }

  /** Create an Actor system using the existing execution context `ec`
    */
  def createActorSystem(namePrefix: String)(implicit ec: ExecutionContext): ActorSystem =
    ActorSystem(
      namePrefix + "-actor-system",
      defaultExecutionContext = Some(ec),
      config = Some(ConfigFactory.load),
    )

  /** Create a new execution sequencer factory (mainly used to create a ledger client) with the existing actor system `actorSystem`
    */
  def createExecutionSequencerFactory(namePrefix: String, logger: TracedLogger)(implicit
      actorSystem: ActorSystem,
      traceContext: TraceContext,
  ): ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(
      namePrefix + "-execution-sequencer",
      actorCount = Threading.detectNumberOfThreads(logger),
    )

  /** A version of [[akka.stream.scaladsl.FlowOps.mapAsync]] that additionally allows to pass state of type `S` between
    * every subsequent element. Unlike [[akka.stream.scaladsl.FlowOps.statefulMapConcat]], the state is passed explicitly.
    * Must not be run with supervision strategies [[akka.stream.Supervision.Restart]] nor [[akka.stream.Supervision.Resume]]
    */
  def statefulMapAsync[Out, Mat, S, T](source: Source[Out, Mat], initial: S)(
      f: (S, Out) => Future[(S, T)]
  )(implicit loggingContext: NamedLoggingContext): Source[T, Mat] = {
    val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
    source
      .scanAsync((initial, Option.empty[T])) { case ((state, _), next) =>
        f(state, next)
          .map { case (newState, out) => (newState, Some(out)) }(directExecutionContext)
      }
      .drop(1) // The first element is `(initial, empty)`, which we want to drop
      .map(
        _._2.getOrElse(
          ErrorUtil.internalError(new NoSuchElementException("scanAsync did not return an element"))
        )
      )
  }

  /** Version of [[akka.stream.scaladsl.FlowOps.mapAsync]] for a [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]].
    * If `f` returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] on one element of
    * `source`, then the returned source returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]
    * for all subsequent elements as well.
    *
    * If `parallelism` is one, ensures that `f` is called sequentially for each element of `source`
    * and that `f` is not invoked on later stream elements if `f` returns
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] for an earlier element.
    * If `parellelism` is greater than one, `f` may be invoked on later stream elements
    * even though an earlier invocation results in `f` returning
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]].
    *
    * '''Emits when''' the Future returned by the provided function finishes for the next element in sequence
    *
    * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream
    * backpressures or the first future is not completed
    *
    * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted,
    * including those for which the future did not run due to earlier [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s.
    *
    * '''Cancels when''' downstream cancels
    *
    * @param parallelism The parallelism level. Must be at least 1.
    * @throws java.lang.IllegalArgumentException if `parallelism` is not positive.
    */
  def mapAsyncUS[A, Mat, B](source: Source[A, Mat], parallelism: Int)(
      f: A => FutureUnlessShutdown[B]
  )(implicit loggingContext: NamedLoggingContext): Source[UnlessShutdown[B], Mat] = {
    require(parallelism > 0, "Parallelism must be positive")
    // If parallelism is 1, then the caller expects that the futures run in sequential order,
    // so if one of them aborts due to shutdown we must not run the subsequent ones.
    // For parallelism > 1, we do not have to stop immediately, as there is always a possible execution
    // where the future may have been started before the first one aborted.
    // So we just need to throw away the results of the futures and convert them into aborts.
    if (parallelism == 1) {
      val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
      statefulMapAsync(source, initial = false) { (aborted, next) =>
        if (aborted) Future.successful(true -> AbortedDueToShutdown)
        else f(next).unwrap.map(us => !us.isOutcome -> us)(directExecutionContext)
      }
    } else {
      val discardedInitial: UnlessShutdown[B] = AbortedDueToShutdown
      // Mutable reference to short-circuit one we've observed the first aborted due to shutdown.
      val abortedFlag = new AtomicBoolean(false)
      source
        .mapAsync(parallelism)(elem =>
          if (abortedFlag.get()) Future.successful(AbortedDueToShutdown)
          else f(elem).unwrap
        )
        .scan((false, discardedInitial)) { case ((aborted, _), next) =>
          if (aborted) (true, AbortedDueToShutdown)
          else {
            val abort = !next.isOutcome
            if (abort) abortedFlag.set(true)
            (abort, next)
          }
        }
        .drop(1) // The first element is `(false, discardedInitial)`, which we want to drop
        .map(_._2)
    }
  }

  /** Version of [[mapAsyncUS]] that discards the [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s.
    *
    * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted.
    */
  def mapAsyncAndDrainUS[A, Mat, B](source: Source[A, Mat], parallelism: Int)(
      f: A => FutureUnlessShutdown[B]
  )(implicit loggingContext: NamedLoggingContext): Source[B, Mat] =
    mapAsyncUS(source, parallelism)(f)
      // Important to use `collect` instead of `takeWhile` here
      // so that the return source completes only after all `source`'s elements have been consumed.
      .collect { case Outcome(x) => x }

  /** Combines two kill switches into one */
  class CombinedKillSwitch(private val killSwitch1: KillSwitch, private val killSwitch2: KillSwitch)
      extends KillSwitch {
    override def shutdown(): Unit = {
      killSwitch1.shutdown()
      killSwitch2.shutdown()
    }

    override def abort(ex: Throwable): Unit = {
      killSwitch1.abort(ex)
      killSwitch2.abort(ex)
    }
  }

  /** Defines the policy when [[restartSource]] should restart the source, and the state from which the source should be restarted from. */
  trait RetrySourcePolicy[S, A, M] {

    /** Determines whether the source should be restarted, and if so (([[scala.Some$]])),
      * the backoff duration and the new state to restart from.
      * Called after the current source has terminated normally or with an error.
      *
      * @param lastState The state that was used to create the current source
      * @param mat The materialized value returned by the current source
      * @param lastEmittedElement The last element emitted by the current source and passed downstream.
      *                           Downstream obviously need not yet have fully processed the element though.
      *                           [[scala.None$]] if the current source did not emit anything,
      *                           even if previous sources have emitted elements.
      * @param lastFailure The error the current source failed with, if any.
      */
    def shouldRetry(
        lastState: S,
        mat: M,
        lastEmittedElement: Option[A],
        lastFailure: Option[Throwable],
    ): Option[(FiniteDuration, S)]
  }

  /** Creates a source from `mkSource` from the `initial` state.
    * Whenever this source terminates, `policy` determines whether another source shall be constructed (after a given delay) from a possibly new state.
    * The returned source concatenates the output of all the constructed sources in order.
    * At most one constructed source is active at any given point in time.
    *
    * Failures in the constructed sources are passed to the `policy`, but do not make it downstream.
    * The `policy` is responsible for properly logging these errors if necessary.
    *
    * @return The concatenation of all constructed sources.
    *         This source is NOT a blueprint and MUST therefore be materialized at most once.
    *         Its materialized value provides a kill switch to stop retrying.
    *         Only the [[akka.stream.KillSwitch.shutdown]] method should be used;
    *         The switch does not short-circuit the already constructed sources though.
    *         synchronization may not work correctly with [[akka.stream.KillSwitch.abort]].
    *         Downstream should not cancel; use the kill switch instead.
    *
    *         The materialized [[scala.concurrent.Future]] can be used to synchronize on the computations for restarts:
    *         if the source is stopped with the kill switch, the future completes after the computations have finished.
    */
  def restartSource[S: Pretty, A, M](
      name: String,
      initial: S,
      mkSource: S => Source[A, M],
      policy: RetrySourcePolicy[S, A, M],
  )(implicit
      loggingContext: NamedLoggingContext,
      materializer: Materializer,
  ): Source[A, (KillSwitch, Future[Done])] = {
    val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)

    // Use immediate acknowledgements and buffer size 1 to minimize the risk that
    // several materializations of the returned source concurrently restart stuff.
    val (boundedSourceQueue, source) = Source.queue[S](bufferSize = 1).preMaterialize()
    val flushFuture = new FlushFuture(s"RestartSource $name", loggingContext.loggerFactory)

    def idempotentComplete(): Unit =
      try {
        boundedSourceQueue.complete()
      } catch {
        case _: IllegalStateException =>
      }

    class KillSwitchForRestartSource extends KillSwitch {
      private val isClosing = new AtomicBoolean(false)

      private val completeOnClosing: scala.collection.concurrent.Map[Any, () => Unit] =
        TrieMap.empty[Any, () => Unit]

      private def onClose(): Unit = {
        isClosing.set(true)
        completeOnClosing.foreach { case (_, f) => f() }
      }

      def runOnClose(f: () => Unit): Any = {
        val key = new Object()
        completeOnClosing.put(key, f).discard[Option[() => Unit]]
        if (isClosing.get()) f()
        key
      }

      def removeKey(key: Any): Unit =
        completeOnClosing.remove(key).discard[Option[() => Unit]]

      override def shutdown(): Unit = {
        onClose()
        idempotentComplete()
      }

      override def abort(ex: Throwable): Unit = {
        onClose()
        try {
          boundedSourceQueue.fail(ex)
        } catch {
          case _: IllegalStateException =>
        }
      }
    }
    val killSwitchForSourceQueue = new KillSwitchForRestartSource

    def restartFrom(nextState: S): Unit = {
      loggingContext.debug(show"(Re)Starting the source $name from state $nextState")
      boundedSourceQueue.offer(nextState) match {
        case QueueOfferResult.Enqueued =>
          loggingContext.debug(s"Restarted the source $name with state $nextState")
        case QueueOfferResult.Dropped =>
          // This should not happen
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Could not restart the source $name because the state queue is full. Has the returned source been materialized multiple times?"
            )
          )
        case _: QueueCompletionResult =>
          loggingContext.debug(
            s"Not restarting $name because the restart source has already been completed"
          )
      }
    }

    // Kick it off with the initial state
    restartFrom(initial)

    source
      .flatMapConcat { state =>
        val lastObservedElem: AtomicReference[Option[A]] = new AtomicReference[Option[A]](None)
        val lastObservedError: AtomicReference[Option[Throwable]] =
          new AtomicReference[Option[Throwable]](None)

        def observeSuccess(elem: Try[A]): Try[A] = {
          elem.foreach(x => lastObservedElem.set(Some(x)))
          elem
        }
        val observeError: Throwable PartialFunction Try[A] = { case NonFatal(ex) =>
          lastObservedError.set(Some(ex))
          Failure(ex)
        }

        // flatMapConcat swallows the materialized value of the inner sources
        // So we make them accessible to the retry directly.
        def uponTermination(mat: M, doneF: Future[Done]): NotUsed = {
          val afterTerminationF = doneF
            .thereafter { outcome =>
              ErrorUtil.requireArgument(
                outcome.isSuccess,
                s"RestartSource $name: recover did not catch the error $outcome",
              )
              policy.shouldRetry(state, mat, lastObservedElem.get, lastObservedError.get) match {
                case Some((backoff, nextState)) =>
                  implicit val ec: ExecutionContext = directExecutionContext

                  val delayedPromise = Promise[UnlessShutdown[Unit]]()
                  val key = killSwitchForSourceQueue.runOnClose { () =>
                    delayedPromise.trySuccess(AbortedDueToShutdown).discard[Boolean]
                  }
                  val delayedF = DelayUtil.delay(backoff).thereafter { _ =>
                    killSwitchForSourceQueue.removeKey(key)
                    delayedPromise.trySuccess(Outcome(())).discard[Boolean]
                  }
                  FutureUtil.doNotAwait(
                    delayedF,
                    s"DelayUtil.delay for RestartSource $name failed",
                  )

                  val restartF = delayedPromise.future.map {
                    case AbortedDueToShutdown =>
                      loggingContext.debug(s"Not restarting $name due to shutdown")
                    case Outcome(()) => restartFrom(nextState)
                  }
                  FutureUtil.doNotAwait(
                    restartF,
                    s"Restart future for RestartSource $name failed",
                  )
                case None =>
                  loggingContext.debug(s"Not retrying $name any more. Completing the source.")
                  idempotentComplete()
              }
            }(materializer.executionContext)
            .thereafter(_.failed.foreach { ex =>
              loggingContext.error(
                s"The retry policy for RestartSource $name failed with an error. Stop retrying.",
                ex,
              )
              idempotentComplete()
            })(materializer.executionContext)
          flushFuture.addToFlushAndLogError(show"RestartSource ${name.unquoted} at state $state")(
            afterTerminationF
          )
          NotUsed
        }

        mkSource(state)
          .map(Success.apply)
          // Grab any upstream errors of the current source
          // before they escape to the concatenated source and bypass the restart logic
          .recover(observeError)
          // Observe elements only after recovering from errors so that the error cannot jump over the map.
          .map(observeSuccess)
          .watchTermination()(uponTermination)
      }
      // Filter out the exceptions from the recover
      .mapConcat(_.toOption)
      .watchTermination() { case (NotUsed, doneF) =>
        val everythingTerminatedF =
          doneF.thereafterF { _ =>
            // Complete the queue of elements again, to make sure that
            // downstream cancellations do not race with a restart.
            idempotentComplete()
            flushFuture.flush()
          }(
            // The direct execution context ensures that this runs as soon as the future's promise is completed,
            // i.e., a downstream cancellation signal cannot propagate upstream while this is running.
            directExecutionContext
          )
        killSwitchForSourceQueue -> everythingTerminatedF
      }
  }

  /** Adds a [[akka.stream.KillSwitches.single]] into the stream after the given source
    * and injects the created kill switch into the stream
    */
  def withUniqueKillSwitch[A, Mat, Mat2](
      source: Source[A, Mat]
  )(mat: (Mat, UniqueKillSwitch) => Mat2): Source[(A, KillSwitch), Mat2] = {
    import syntax.*
    source
      .withMaterializedValueMat(new AtomicReference[UniqueKillSwitch])(Keep.both)
      .viaMat(KillSwitches.single) { case ((m, ref), killSwitch) =>
        ref.set(killSwitch)
        mat(m, killSwitch)
      }
      .map { case (a, ref) => (a, ref.get()) }
  }

  private[util] def withMaterializedValueMat[M, A, Mat, Mat2](create: => M)(source: Source[A, Mat])(
      combine: (Mat, M) => Mat2
  ): Source[(A, M), Mat2] =
    source.viaMat(new WithMaterializedValue[M, A](() => create))(combine)

  /** Creates a value upon materialization that is added to every element of the stream.
    *
    * WARNING: This flow breaks the synchronization abstraction of Akka streams,
    * as the created value is accessible from within the stream and from the outside through the materialized value.
    * Users of this flow must make sure that accessing the value is thread-safe!
    */
  private class WithMaterializedValue[M, A](create: () => M)
      extends GraphStageWithMaterializedValue[FlowShape[A, (A, M)], M] {
    private val in: Inlet[A] = Inlet[A]("withMaterializedValue.in")
    private val out: Outlet[(A, M)] = Outlet[(A, M)]("withMaterializedValue.out")
    override val shape: FlowShape[A, (A, M)] = FlowShape(in, out)

    override def initialAttributes: Attributes = Attributes.name("withMaterializedValue")

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes
    ): (GraphStageLogic, M) = {
      val m: M = create()
      val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onPush(): Unit = push(out, grab(in) -> m)

        override def onPull(): Unit = pull(in)

        setHandlers(in, out, this)
      }
      (logic, m)
    }
  }

  object syntax {

    /** Defines extension methods for [[akka.stream.scaladsl.Source]] that map to the methods defined in this class */
    implicit class AkkaUtilSyntaxForSource[A, Mat](private val source: Source[A, Mat])
        extends AnyVal {
      def statefulMapAsync[S, T](initial: S)(
          f: (S, A) => Future[(S, T)]
      )(implicit loggingContext: NamedLoggingContext): Source[T, Mat] =
        AkkaUtil.statefulMapAsync(source, initial)(f)

      def mapAsyncUS[B](parallelism: Int)(f: A => FutureUnlessShutdown[B])(implicit
          loggingContext: NamedLoggingContext
      ): Source[UnlessShutdown[B], Mat] =
        AkkaUtil.mapAsyncUS(source, parallelism)(f)

      def mapAsyncAndDrainUS[B](parallelism: Int)(
          f: A => FutureUnlessShutdown[B]
      )(implicit loggingContext: NamedLoggingContext): Source[B, Mat] =
        AkkaUtil.mapAsyncAndDrainUS(source, parallelism)(f)

      private[util] def withMaterializedValueMat[M, Mat2](create: => M)(
          mat: (Mat, M) => Mat2
      ): Source[(A, M), Mat2] =
        AkkaUtil.withMaterializedValueMat(create)(source)(mat)

      def withUniqueKillSwitchMat[Mat2](
      )(mat: (Mat, UniqueKillSwitch) => Mat2): Source[(A, KillSwitch), Mat2] =
        AkkaUtil.withUniqueKillSwitch(source)(mat)
    }
  }
}
