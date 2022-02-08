// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{ActorAttributes, Materializer, Supervision}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext

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
}
