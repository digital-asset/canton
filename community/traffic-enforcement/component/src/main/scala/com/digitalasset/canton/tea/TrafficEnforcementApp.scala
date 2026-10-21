// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.InProcessGrpcName
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tea.ingestion.{
  TeaCreditIngestionService,
  TeaDebitIngestionService,
  TeaLedgerResources,
}
import com.digitalasset.canton.tea.projection.{
  CloseableProjection,
  EventSource,
  ProjectionEvent,
  TeaProjectionFactory,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContextGrpc, Traced}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.ActorSystem as TypedSystem
import org.apache.pekko.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import org.apache.pekko.projection.ProjectionId
import org.apache.pekko.stream.scaladsl.Source

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
    projectionFactory: TeaProjectionFactory,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit untypedSystem: ActorSystem, typedSystem: TypedSystem[?], tracer: Tracer)
    extends NamedLogging
    with FlagCloseable {

  import typedSystem.executionContext

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
    .intercept(TraceContextGrpc.reportingServerInterceptor(tracer))
    .addService(new TrafficEnforcementServiceGrpc(service, loggerFactory))
    .build()

  private def createProjection(
      projectionId: ProjectionId,
      eventSource: EventSource,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): CloseableProjection =
    new CloseableProjection(
      projectionId,
      untypedSystem.spawn(
        projectionFactory.projection(projectionId, eventSource, grpcSourceFactory),
        s"${projectionId.name}-$instanceId",
      ),
      loggerFactory,
      timeouts,
    )

  // Projection for debits
  private val debitIngestion = createProjection(
    ProjectionId("debit-ingestion", "grpc-stream"),
    EventSource.LedgerAPICompletions,
    new TeaDebitIngestionService(
      ledgerResources.client.completionService,
      ledgerResources.client.stateService,
      config.projection,
      loggerFactory,
    ).grpcSource,
  )

  // Projection for credits, only started when paymasters are configured: without them no top-up can
  // be trusted, and the top-up package might not even be uploaded on this participant.
  private val creditIngestion: Option[CloseableProjection] =
    if (config.projection.topUpPaymasters.isEmpty) {
      noTracingLogger.info("No top-up paymasters configured, top-up ingestion is disabled")
      None
    } else
      Some(
        createProjection(
          ProjectionId("credit-ingestion", "grpc-stream"),
          EventSource.DamlTopUp,
          new TeaCreditIngestionService(
            ledgerResources.client.updateService,
            ledgerResources.client.stateService,
            config.projection,
            loggerFactory,
          ).grpcSource,
        )
      )

  // Start the in process gRPC server
  server.start().discard

  override def onClosed(): Unit =
    LifeCycle.close(
      (creditIngestion.toList ++ Seq(
        debitIngestion,
        ledgerResources,
        LifeCycle.toCloseableServer(server, logger, serverName),
      ))*
    )(logger)
}

object TrafficEnforcementApp {
  def apply(
      storage: Storage,
      instanceName: InstanceName,
      ledgerApiPort: Port,
      config: TrafficEnforcementServerConfig.Internal,
      minimumPruningRetention: NonNegativeFiniteDuration,
      token: () => Option[String],
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      clock: Clock,
      metrics: TrafficEnforcementMetrics,
      onEventCommitted: () => Unit = () => (),
  )(implicit
      ec: ExecutionContext,
      tracer: Tracer,
      system: ActorSystem,
  ): TrafficEnforcementApp = {
    val logger = loggerFactory.getTracedLogger(getClass)

    implicit val typedSystem: TypedSystem[Nothing] = system.toTyped

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
        config.projection,
        loggerFactory,
        timeouts,
        config.databaseQueryTimeout,
        metrics,
        onEventCommitted,
      )

    val service = new TrafficEnforcementService(
      store,
      clock,
      minimumPruningRetention,
      metrics,
      loggerFactory,
    )
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
