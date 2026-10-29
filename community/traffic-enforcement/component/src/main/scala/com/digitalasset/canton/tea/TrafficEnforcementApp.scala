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
import com.digitalasset.canton.platform.config.{TopUpConfig, TrafficEnforcementServerConfig}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tea.ingestion.{
  TeaCreditIngestionService,
  TeaDebitIngestionService,
  TeaLedgerResources,
}
import com.digitalasset.canton.tea.projection.{
  CloseableProjection,
  CompletionAccountingPolicy,
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
  * [[TrafficEnforcementServiceGrpc]] and the participant-internal
  * [[com.digitalasset.canton.tea.v1.SubmissionAccountingServiceGrpc.SubmissionAccountingService]]
  * via [[TeaSubmissionAccountingServiceGrpc]].
  *
  * The app may be started before the participant's Ledger API server: the ingestion streams retry
  * until the Ledger API is reachable. Debit ingestion starts from `ledgerEndAtStart` while it has
  * no persisted offset, so completions recorded during the retries are not missed.
  */
class TrafficEnforcementApp(
    config: TrafficEnforcementServerConfig.Internal,
    instanceName: InstanceName,
    ledgerApiPort: Port,
    ledgerEndAtStart: Long,
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
    .addService(new TeaSubmissionAccountingServiceGrpc(service, loggerFactory))
    .build()

  private def createProjection(
      projectionId: ProjectionId,
      eventSource: EventSource,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): CloseableProjection =
    new CloseableProjection(
      projectionId,
      untypedSystem.spawn(
        projectionFactory.projection(
          projectionId,
          eventSource,
          // Reconnect right away when the stream is retried after a failed connection attempt.
          // Pairs with the retry of the source in TeaProjectionFactory.createSourceProvider: without
          // the reset, the channel's own reconnect backoff (up to 2 minutes) would delay ingestion
          // after the Ledger API comes up. The reset lives here because the app owns the channel.
          offset => {
            ledgerResources.resetConnectBackoff()
            grpcSourceFactory(offset)
          },
        ),
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
      ledgerEndAtStart,
      config.projection,
      loggerFactory,
    ).grpcSource,
  )

  // Projection for credits by daml top-ups
  private val creditIngestion: Option[CloseableProjection] = config.topUp match {
    case TopUpConfig.Disabled =>
      noTracingLogger.info("Top-up ingestion is disabled")
      None
    case damlCredit: TopUpConfig.DamlCredit =>
      Some(
        createProjection(
          ProjectionId("credit-ingestion", "grpc-stream"),
          EventSource.DamlTopUp,
          new TeaCreditIngestionService(
            ledgerResources.client.updateService,
            ledgerResources.client.stateService,
            damlCredit,
            loggerFactory,
          ).grpcSource,
        )
      )
  }

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
      ledgerEndAtStart: Long,
      config: TrafficEnforcementServerConfig.Internal,
      minimumPruningRetention: NonNegativeFiniteDuration,
      token: () => Option[String],
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      clock: Clock,
      metrics: TrafficEnforcementMetrics,
      completionAccountingPolicy: CompletionAccountingPolicy,
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
        config.accountLookupTimeout,
        metrics,
        completionAccountingPolicy,
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
      ledgerEndAtStart,
      service,
      ledgerResources,
      projectionFactory,
      loggerFactory,
      timeouts,
    )
  }
}
