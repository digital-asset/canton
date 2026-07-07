// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tea.projection.db.{TeaDbProjectionFactory, TeaDbTrafficStore}
import com.digitalasset.canton.tea.projection.memory.{
  TeaMemoryProjectionFactory,
  TeaMemoryTrafficStore,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.projection.scaladsl.SourceProvider
import org.apache.pekko.projection.{
  HandlerRecoveryStrategy,
  ProjectionBehavior,
  ProjectionId,
  StatusObserver,
}
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

/** Shared storage backing all TEA ingestion projections.
  *
  * Single function that builds a projection from a stream source and handler The trait abstracts
  * over the in-memory and DB implementations
  */
trait TeaProjectionFactory { this: NamedLogging =>

  /** Build a projection behavior for a single ingestion stream, reusing the shared storage. */
  def projection(
      projectionId: ProjectionId,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): Behavior[ProjectionBehavior.Command]

  // Logging observer to get insights into the lifecycle of the projection
  protected val loggingObserver: StatusObserver[Traced[ProjectionEvent]] =
    new StatusObserver[Traced[ProjectionEvent]] {
      override def started(projectionId: ProjectionId): Unit =
        logger.info(s"Starting projection for projectionId $projectionId")(TraceContext.empty)
      override def failed(projectionId: ProjectionId, cause: Throwable): Unit =
        logger.info(
          s"Failed projection for projectionId $projectionId. It will be restarted.",
          cause,
        )(TraceContext.empty)
      override def stopped(projectionId: ProjectionId): Unit =
        logger.info(s"Stopped projection for projectionId $projectionId")(TraceContext.empty)
      override def beforeProcess(
          projectionId: ProjectionId,
          envelope: Traced[ProjectionEvent],
      ): Unit =
        logger.trace(s"Ready to process event ${envelope.value} for projectionId $projectionId")(
          envelope.traceContext
        )
      override def afterProcess(
          projectionId: ProjectionId,
          envelope: Traced[ProjectionEvent],
      ): Unit =
        logger.trace(s"Processed event ${envelope.value} for projectionId $projectionId")(
          envelope.traceContext
        )
      override def offsetProgress(projectionId: ProjectionId, env: Traced[ProjectionEvent]): Unit =
        logger.info(s"Stored offset ${env.value.event.offset} for projectionId $projectionId")(
          env.traceContext
        )
      override def error(
          projectionId: ProjectionId,
          env: Traced[ProjectionEvent],
          cause: Throwable,
          recoveryStrategy: HandlerRecoveryStrategy,
      ): Unit =
        logger.warn(
          s"Error during envelope processing of ${env.value} for projectionId $projectionId",
          cause,
        )(env.traceContext)
    }

  /** Create a projection source provider from a source of ProjectionEvent
    * @param grpcSourceFactory
    *   the grpcSourceFactory: takes an optional offset (Long) and returns a source pulling events
    *   from this offset. The offset provided will be the last one stored (so processed). This
    *   matches with the LAPI "beginExclusive" semantics: the stream will start at the following
    *   offset.
    */
  protected def createSourceProvider(
      logger: TracedLogger,
      projectionId: ProjectionId,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  )(implicit ec: ExecutionContext): SourceProvider[Long, Traced[ProjectionEvent]] =
    new SourceProvider[Long, Traced[ProjectionEvent]] {
      override def source(
          offsetProvider: () => Future[Option[Long]]
      ): Future[Source[Traced[ProjectionEvent], NotUsed]] =
        offsetProvider().map { maybeOffset =>
          logger.info(s"Starting ingestion stream for $projectionId with offset $maybeOffset")(
            TraceContext.empty
          )
          grpcSourceFactory(maybeOffset).mapMaterializedValue(_ => NotUsed)
        }
      override def extractOffset(record: Traced[ProjectionEvent]): Long = record.value.event.offset
      override def extractCreationTime(record: Traced[ProjectionEvent]): Long =
        record.value.event.deltaEvent.timestamp.toEpochMilli
    }
}

object TeaProjectionFactory {

  /** Open the shared storage once, based on the configured storage backend. */
  def create(
      storage: Storage,
      eventSource: EventSource,
      config: ProjectionConfig,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit system: ActorSystem[?]): (TeaProjectionFactory, TeaTrafficStore) = {
    import system.executionContext

    storage match {
      case db: DbStorage =>
        val store = new TeaDbTrafficStore(db, loggerFactory, timeouts)
        val projection: TeaProjectionFactory =
          new TeaDbProjectionFactory(db, loggerFactory, store, eventSource, config)
        (projection, store)
      case _: MemoryStorage =>
        val store = new TeaMemoryTrafficStore()
        val projection: TeaProjectionFactory = new TeaMemoryProjectionFactory(loggerFactory, store)
        (projection, store)
    }
  }
}
