// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
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
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.Done
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.projection.scaladsl.Handler
import org.apache.pekko.projection.testkit.scaladsl.TestProjection
import org.apache.pekko.projection.{ProjectionBehavior, ProjectionId}
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** In memory only projection. Offsets are not persisted.
  */
private[projection] class TeaMemoryProjectionFactory(
    override val loggerFactory: NamedLoggerFactory,
    store: TeaMemoryTrafficStore,
    override protected val metrics: TrafficEnforcementMetrics,
    completionPolicy: CompletionAccountingPolicy,
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
        envelope.value match {
          case topUps: ProjectionEvent.TopUps => applyDeltas(projectionId, eventSource, topUps)
          case completion: ProjectionEvent.Completion =>
            applyCompletion(projectionId, eventSource, completion)
          case _: ProjectionEvent.Checkpoint => Future.successful(Done)
        }
      }
    }

  private def applyCompletion(
      projectionId: ProjectionId,
      eventSource: EventSource,
      completion: ProjectionEvent.Completion,
  )(implicit tc: TraceContext): Future[Done] =
    withSpan(ApplyCompletionSpanName) { implicit tc => span =>
      setApplyCompletionSpanAttributes(span, completion, eventSource)
      store
        .applyCompletion(
          completion,
          projectionId.id,
          eventSource,
          completionPolicy,
        )
        .transform {
          case Success(decision) =>
            // None means a replayed completion, which we already reported.
            decision.foreach(recordCompletion(span, _))
            Success(Done)
          case Failure(ex) =>
            recordCompletionFailure(span)
            Failure(ex)
        }
    }

  private def applyDeltas(
      projectionId: ProjectionId,
      eventSource: EventSource,
      topUps: ProjectionEvent.TopUps,
  )(implicit tc: TraceContext): Future[Done] =
    topUps.deltasWithEventIds(projectionId.id).foldLeft(Future.successful(Done)) {
      case (previous, (accountDelta, eventId)) =>
        previous.flatMap { _ =>
          withSpan(ApplyDeltaSpanName) { implicit tc => span =>
            setApplyDeltaSpanAttributes(span, topUps, accountDelta, eventSource)
            val reject = TeaMemoryTrafficStore.rejection(accountDelta.account, accountDelta.delta)
            store
              .persistDeltaInternal(
                account = accountDelta.account,
                eventId = eventId,
                eventSource = eventSource,
                trafficDelta = accountDelta.delta,
                timestamp = topUps.timestamp,
              )
              .map(_ => Done)
              .recoverWith(reject.andThen(err => Future.failed(err.asGrpcError)))
          }
        }
    }
}
