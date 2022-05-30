// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.{RunnableGraph, Source}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.canton.concurrent.{DirectExecutionContext, Threading}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ExecutionContext, Future}

object AkkaUtil {

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
  )(implicit loggingContext: ErrorLoggingContext): Source[T, Mat] = {
    val directExecutionContext = DirectExecutionContext(loggingContext.logger)
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
}
