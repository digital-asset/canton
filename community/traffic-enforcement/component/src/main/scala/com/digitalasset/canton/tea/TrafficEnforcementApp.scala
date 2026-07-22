// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.InProcessGrpcName
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tea.ingestion.{TeaDebitIngestionService, TeaLedgerResources}
import com.digitalasset.canton.tea.projection.{
  CloseableProjection,
  EventSource,
  TeaProjectionFactory,
}
import com.digitalasset.canton.time.Clock
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.{ClassicActorSystemOps, TypedActorSystemOps}
import org.apache.pekko.projection.ProjectionId

import java.util.UUID
import scala.concurrent.ExecutionContext

/** Top level TEA class. Creates an in-process gRPC server that exposes the
  * [[com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService]] via
  * [[TrafficEnforcementServiceGrpc]].
  */
class TrafficEnforcementApp(
    config: TrafficEnforcementServerConfig.Internal,
    instanceName: InstanceName,
    ledgerApiPort: Port,
    service: TrafficEnforcementService,
    ledgerResources: TeaLedgerResources,
    debitProjection: TeaProjectionFactory,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit system: ActorSystem[?])
    extends NamedLogging
    with FlagCloseable {

  import system.executionContext

  // Distinguishes this instance's top-level actors from those of a previous (closing) instance.
  private val instanceId: String = UUID.randomUUID().toString

  // Each TEA in the same JVM needs its own in-process gRPC name, otherwise two of them would try to
  // register under the same name and collide on start. In CI several suites share one JVM and reuse
  // instance names like "participant1", so the instance name alone is not unique. The ledger API port
  // is handed out from a machine wide unique counter.
  private val serverName: String =
    config.processServerNameForInstance(instanceName, ledgerApiPort)

  private val server = InProcessServerBuilder
    .forName(serverName)
    .addService(new TrafficEnforcementServiceGrpc(service, loggerFactory))
    .build()

  private val debitIngestionService =
    new TeaDebitIngestionService(
      ledgerResources.client.completionService,
      ledgerResources.client.stateService,
      config.projection,
      loggerFactory,
    )

  private val debitProjectionId = ProjectionId("debit-ingestion", "grpc-stream")

  // Projection for debits
  private val debitIngestion = new CloseableProjection(
    debitProjectionId,
    system.toClassic.spawn(
      debitProjection.projection(
        debitProjectionId,
        debitIngestionService.grpcSource,
      ),
      s"${debitProjectionId.name}-$instanceId",
    ),
    loggerFactory,
    timeouts,
  )

  // Start the in process gRPC server
  server.start().discard

  override def onClosed(): Unit =
    LifeCycle.close(
      debitIngestion,
      ledgerResources,
      LifeCycle.toCloseableActorSystem(system.classicSystem, logger, timeouts),
      LifeCycle.toCloseableServer(server, logger, serverName),
    )(logger)
}

object TrafficEnforcementApp {
  def apply(
      storage: Storage,
      instanceName: InstanceName,
      ledgerApiPort: Port,
      config: TrafficEnforcementServerConfig.Internal,
      token: () => Option[String],
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      clock: Clock,
  )(implicit
      ec: ExecutionContext
  ): TrafficEnforcementApp = {
    val logger = loggerFactory.getTracedLogger(getClass)

    // Pekko config to configure the TEA's actor system
    val pekkoConfig = config.pekkoConfig(storage)
    implicit val system: ActorSystem[Nothing] =
      org.apache.pekko.actor
        .ActorSystem("TrafficEnforcementAppSystem", pekkoConfig)
        .toTyped

    val ledgerApiChannel = LifeCycle.toCloseableChannel(
      // In-process channel to the participant's own Ledger API gRPC server
      InProcessChannelBuilder
        .forName(InProcessGrpcName.forPort(ledgerApiPort))
        .build(),
      logger,
      "Traffic Enforcement App",
    )

    val ledgerResources = TeaLedgerResources.fromChannel(
      channel = ledgerApiChannel,
      token = token,
      forNode = instanceName,
      loggerFactory = loggerFactory,
      timeouts = timeouts,
    )(ec, system.classicSystem)

    val (projectionFactory, store) =
      TeaProjectionFactory.create(
        storage,
        EventSource.LedgerAPI,
        config.projection,
        loggerFactory,
        timeouts,
      )

    val service = new TrafficEnforcementService(store, clock, loggerFactory)
    new TrafficEnforcementApp(
      config,
      instanceName,
      ledgerApiPort,
      service,
      ledgerResources,
      projectionFactory,
      loggerFactory,
      timeouts,
    )
  }
}
