// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.tea.projection.TeaProjectionFactory.ApplyDeltaSpanName
import com.digitalasset.canton.tea.projection.{EventSource, ProjectionEvent, TeaProjectionFactory}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.projection.scaladsl.Handler
import org.apache.pekko.projection.testkit.scaladsl.TestProjection
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

/** In memory only projection. Offsets are not persisted.
  */
private[projection] class TeaMemoryProjectionFactory(
    override val loggerFactory: NamedLoggerFactory,
    store: TeaMemoryTrafficStore,
    override protected val metrics: TrafficEnforcementMetrics,
    override val onEventCommitted: () => Unit = () => (),
)(implicit
    ec: ExecutionContext,
    tracer: Tracer,
) extends TeaProjectionFactory
    with NamedLogging {
  override def projection(
      projectionId: ProjectionId,
      eventSource: EventSource,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): Behavior[ProjectionBehavior.Command] = {
    // The in memory projection comes from pekko test-kit that's why it's called TestProjection
    val inMemoryProjection = TestProjection(
      projectionId = projectionId,
      sourceProvider = createSourceProvider(logger, projectionId, grpcSourceFactory),
      handler = () => inMemoryHandler(projectionId, eventSource),
    )
      .withStatusObserver(loggingObserver)
    ProjectionBehavior(inMemoryProjection)
  }

  private def inMemoryHandler(projectionId: ProjectionId, eventSource: EventSource) =
    new Handler[Traced[ProjectionEvent]] {
      override def process(envelope: Traced[ProjectionEvent]): Future[Done] = {
        implicit val tc: TraceContext = envelope.traceContext
        val event = envelope.value

        event.deltasWithEventIds(projectionId.id).foldLeft(Future.successful(Done)) {
          case (previous, (accountDelta, eventId)) =>
            previous.flatMap { _ =>
              withSpan(ApplyDeltaSpanName) { implicit tc => span =>
                setApplyDeltaSpanAttributes(span, event, accountDelta, eventSource)
                val reject =
                  TeaMemoryTrafficStore.rejection(accountDelta.account, accountDelta.delta)
                store
                  .persistDeltaInternal(
                    account = accountDelta.account,
                    eventId = eventId,
                    eventSource = eventSource,
                    trafficDelta = accountDelta.delta,
                    timestamp = event.timestamp,
                  )
                  .map(_ => Done)
                  .recoverWith(reject.andThen(err => Future.failed(err.asGrpcError)))
              }
            }
        }
      }
    }
}
