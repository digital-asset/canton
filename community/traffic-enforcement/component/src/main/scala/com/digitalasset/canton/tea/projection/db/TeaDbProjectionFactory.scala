// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.resource.{DbStorage, DbStorageMulti, DbStorageSingle}
import com.digitalasset.canton.tea.projection.TeaProjectionFactory.{
  ApplyCompletionSpanName,
  ApplyDeltaSpanName,
}
import com.digitalasset.canton.tea.projection.{
  CompletionAccountingPolicy,
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
    config: ProjectionConfig,
    override protected val metrics: TrafficEnforcementMetrics,
    completionPolicy: CompletionAccountingPolicy,
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
      eventSource: EventSource,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): Behavior[ProjectionBehavior.Command] = ProjectionBehavior {
    SlickProjection
      .exactlyOnce(
        projectionId = projectionId,
        sourceProvider = createSourceProvider(logger, projectionId, grpcSourceFactory),
        databaseConfig = slickProjectionDbConfig,
        handler = () => eventHandler(projectionId, eventSource),
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
  private def eventHandler(projectionId: ProjectionId, eventSource: EventSource) =
    new SlickHandler[Traced[ProjectionEvent]] {
      override def process(envelope: Traced[ProjectionEvent]): DBIO[Done] = {
        implicit val traceContext: TraceContext = envelope.traceContext

        logger.debug(s"Persisting event ${envelope.value}")

        envelope.value match {
          case topUps: ProjectionEvent.TopUps => applyDeltas(projectionId, eventSource, topUps)
          case completion: ProjectionEvent.Completion =>
            applyCompletion(projectionId, eventSource, completion)
          case _: ProjectionEvent.Checkpoint => DBIO.successful(Done)
        }
      }
    }

  private def applyCompletion(
      projectionId: ProjectionId,
      eventSource: EventSource,
      completion: ProjectionEvent.Completion,
  )(implicit traceContext: TraceContext): DBIO[Done] =
    withSpanDBIO(ApplyCompletionSpanName) { implicit traceContext => span =>
      setApplyCompletionSpanAttributes(span, completion, eventSource)
      store
        .applyCompletionDBIO(
          completion,
          projectionId.id,
          eventSource,
          completionPolicy,
        )
        .asTry
        .flatMap {
          case Success(decision) =>
            // None means a replayed completion, which we already reported.
            decision.foreach(recordCompletion(span, _))
            DBIO.successful(Done)
          case Failure(ex) =>
            recordCompletionFailure(span)
            DBIO.failed(ex)
        }
    }

  private def applyDeltas(
      projectionId: ProjectionId,
      eventSource: EventSource,
      topUps: ProjectionEvent.TopUps,
  )(implicit traceContext: TraceContext): DBIO[Done] =
    DBIO
      .seq(topUps.deltasWithEventIds(projectionId.id).map { case (accountDelta, eventId) =>
        withSpanDBIO(ApplyDeltaSpanName) { implicit traceContext => span =>
          setApplyDeltaSpanAttributes(span, topUps, accountDelta, eventSource)
          store.persistProjectedDeltaDBIO(accountDelta, eventId, eventSource, topUps.timestamp)
        }
      }*)
      .map(_ => Done)
}
