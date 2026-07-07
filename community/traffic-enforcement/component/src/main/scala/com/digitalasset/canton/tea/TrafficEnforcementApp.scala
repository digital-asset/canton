// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tea.projection.{EventSource, TeaProjection}
import com.digitalasset.canton.time.Clock
import io.grpc.inprocess.InProcessServerBuilder
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.ClassicActorSystemOps

import scala.concurrent.ExecutionContext

/** Top level TEA class. Creates an in-process gRPC server that exposes the
  * [[com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService]] via
  * [[TrafficEnforcementServiceGrpc]].
  */
class TrafficEnforcementApp(
    config: TrafficEnforcementServerConfig.Internal,
    service: TrafficEnforcementService,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit system: ActorSystem[?])
    extends NamedLogging
    with FlagCloseable {

  import system.executionContext

  private val server = InProcessServerBuilder
    .forName(config.inProcessTeaServerName)
    .addService(new TrafficEnforcementServiceGrpc(service, loggerFactory))
    .build()

  // Start the in process gRPC server
  server.start().discard

  override def onClosed(): Unit = {
    val toClose = List(
      LifeCycle.toCloseableServer(server, logger, config.inProcessTeaServerName),
      LifeCycle.toCloseableActorSystem(system.classicSystem, logger, timeouts),
    )
    LifeCycle.close(toClose*)(logger)
  }
}

object TrafficEnforcementApp {
  type TeaAppBuilder = () => TrafficEnforcementApp

  def apply(
      storage: Storage,
      config: TrafficEnforcementServerConfig.Internal,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      clock: Clock,
  )(implicit
      ec: ExecutionContext
  ): TrafficEnforcementApp = {
    // Pekko config to configure the TEA's actor system
    val pekkoConfig = config.pekkoConfig(storage)
    implicit val system: ActorSystem[Nothing] =
      org.apache.pekko.actor
        .ActorSystem("TrafficEnforcementAppSystem", pekkoConfig)
        .toTyped

    val (projectionFactory, store) =
      TeaProjection.create(
        storage,
        EventSource.LedgerAPI,
        config.projection,
        loggerFactory,
        timeouts,
      )
    val service = new TrafficEnforcementService(store, clock, loggerFactory)
    new TrafficEnforcementApp(config, service, loggerFactory, timeouts)
  }
}
