// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.{RunnableGraph, Source}
import akka.stream.{ActorAttributes, KillSwitch, Materializer, Supervision}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.canton.concurrent.{DirectExecutionContext, Threading}
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.ConfigFactory

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

object AkkaUtil extends HasLoggerName {

  /** Utility function to run the graph supervised and stop on an unhandled exception.
    *
    * By default, an Akka flow will discard exceptions. Use this method to avoid discarding exceptions.
    */
  def runSupervised[T](reporter: Throwable => Unit, graph: RunnableGraph[T])(implicit
      mat: Materializer
  ): T = {
    graph
      .withAttributes(ActorAttributes.withSupervisionStrategy { ex =>
        reporter(ex)
        Supervision.Stop
      })
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
}
