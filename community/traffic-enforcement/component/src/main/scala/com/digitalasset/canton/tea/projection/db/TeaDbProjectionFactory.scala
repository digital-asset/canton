// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.resource.{DbStorage, DbStorageMulti, DbStorageSingle}
import com.digitalasset.canton.tea.projection.TeaProjectionFactory.ApplyDeltaSpanName
import com.digitalasset.canton.tea.projection.{
  EventId,
  EventSource,
  ProjectionEvent,
  TeaProjectionFactory,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.typesafe.config.{Config, ConfigFactory}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.projection.slick.{SlickHandler, SlickProjection}
import org.apache.pekko.projection.{HandlerRecoveryStrategy, ProjectionBehavior, ProjectionId}
import org.apache.pekko.stream.scaladsl.Source
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Pekko projection backed by a JDBC DB.
  * @param dbStorage
  *   dbStorage coming from the participant node.
  */
private[projection] class TeaDbProjectionFactory(
    dbStorage: DbStorage,
    override val loggerFactory: NamedLoggerFactory,
    store: TeaDbTrafficStore,
    eventSource: EventSource,
    config: ProjectionConfig,
    override protected val metrics: TrafficEnforcementMetrics,
    override val onEventCommitted: () => Unit = () => (),
)(implicit system: ActorSystem[?], tracer: Tracer)
    extends TeaProjectionFactory
    with NamedLogging {
  implicit val ec: ExecutionContext = system.executionContext

  private val jdbcProfile: JdbcProfile = dbStorage.profile.jdbc

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private val slickProjectionDbConfig: DatabaseConfig[JdbcProfile] =
    new DatabaseConfig[JdbcProfile] {
      override val profile: JdbcProfile = jdbcProfile
      override def db: profile.backend.Database =
        (dbStorage match {
          case multi: DbStorageMulti => multi.writeDb
          case single: DbStorageSingle => single.db
          case _ => throw new IllegalArgumentException("unsupported db storage")
        })
          // asInstanceOf is ugly but safe,
          // the type checker can't verify because the db type here is a dependent type of the profile
          // but DbStorage does return a JdbcBackend.Database
          .asInstanceOf[profile.backend.Database]
      override def config: Config = ConfigFactory.empty()
      override def profileName: String = jdbcProfile.getClass.getName
      override def profileIsObject: Boolean = true
    }

  def projection(
      projectionId: ProjectionId,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): Behavior[ProjectionBehavior.Command] = ProjectionBehavior {
    SlickProjection
      .exactlyOnce(
        projectionId = projectionId,
        sourceProvider = createSourceProvider(logger, projectionId, grpcSourceFactory),
        databaseConfig = slickProjectionDbConfig,
        handler = () => eventHandler(projectionId),
      )
      .withStatusObserver(loggingObserver)
      .withRestartBackoff(
        config.minProjectionRestartBackoff.asFiniteApproximation,
        config.maxProjectionRestartBackoff.asFiniteApproximation,
        config.projectionRestartRandomFactor,
        config.projectionMaxRestarts,
      )
      .withRecoveryStrategy(
        HandlerRecoveryStrategy
          .retryAndFail(
            config.maxHandlerRetries.value,
            config.handlerRetryDelay.asFiniteApproximation,
          )
      )
  }

  // Event handler for projection events
  // Runs sequentially for each event
  // Returns a DBIO that is run in the same transaction as the offset watermark bump,
  // giving exactly once semantics and crash recovery
  private def eventHandler(projectionId: ProjectionId) = new SlickHandler[Traced[ProjectionEvent]] {
    override def process(envelope: Traced[ProjectionEvent]): DBIO[Done] = {
      val account = envelope.value.account
      val event = envelope.value.event
      implicit val traceContext: TraceContext = envelope.traceContext

      logger.debug(s"Persisting event ${envelope.value}")

      withSpanDBIO(ApplyDeltaSpanName) { implicit traceContext => span =>
        setApplyDeltaSpanAttributes(span, envelope.value, eventSource)

        // Classifies the exception so it can be reported under TRAFFIC_UPDATE_OUT_OF_BOUND.
        val reject = TeaDbTrafficStore.rejection(account, event.deltaEvent.delta)

        // We don't add 'transactionally' on purpose here, as this DBIO is picked up by the pekko projection
        // which will add the offset persistence to it and wrap the whole thing into a transaction
        // to provide exactlyOnce semantics
        store
          .persistDeltaDBIO(
            accountId = account,
            eventId = EventId.tryCreate(s"${projectionId.id}-${envelope.value.event.offset}"),
            trafficDelta = event.deltaEvent.delta,
            timestamp = event.deltaEvent.timestamp,
            eventSource = eventSource,
          )
          .asTry
          .flatMap {
            case Success(_) => DBIO.successful(Done)
            // Both branches fail the action, the difference is only that a rejected delta is reported
            // under its own error code rather than as a raw JDBC failure.
            // TODO(#34424): decide the failure policy for a rejected delta. Failing stalls the
            //  projection, skipping the event drops the delta and needs a different transaction shape,
            //  since the event row inserted above has to roll back while the offset bump commits.
            case Failure(ex) if reject.isDefinedAt(ex) => DBIO.failed(reject(ex).asGrpcError)
            case Failure(ex) => DBIO.failed(ex)
          }
      }
    }
  }
}
