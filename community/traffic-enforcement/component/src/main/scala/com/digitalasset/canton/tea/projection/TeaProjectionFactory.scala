// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementOutcome
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tea.projection.db.{TeaDbProjectionFactory, TeaDbTrafficStore}
import com.digitalasset.canton.tea.projection.memory.{
  TeaMemoryProjectionFactory,
  TeaMemoryTrafficStore,
}
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.RecoveryStrategy
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
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

import java.sql.{SQLNonTransientConnectionException, SQLRecoverableException, SQLTransientException}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Shared storage backing all TEA ingestion projections.
  *
  * Single function that builds a projection from a stream source and handler The trait abstracts
  * over the in-memory and DB implementations
  */
trait TeaProjectionFactory extends Spanning { this: NamedLogging =>

  import TeaProjectionFactory.*

  protected val metrics: TrafficEnforcementMetrics

  protected def setApplyDeltaSpanAttributes(
      span: SpanWrapper,
      topUps: ProjectionEvent.TopUps,
      accountDelta: AccountDelta,
      eventSource: EventSource,
  ): Unit = {
    span.setAttribute(AccountIdAttribute, accountDelta.account.unwrap)
    span.setAttribute(DeltaAttribute, accountDelta.delta.toString)
    span.setAttribute(EventSourceAttribute, eventSource.toString)
    span.setAttribute(OffsetAttribute, topUps.offset.toString)
  }

  protected def setApplyCompletionSpanAttributes(
      span: SpanWrapper,
      completion: ProjectionEvent.Completion,
      eventSource: EventSource,
  ): Unit = {
    span.setAttribute(EventSourceAttribute, eventSource.toString)
    span.setAttribute(OffsetAttribute, completion.offset.toString)
  }

  protected def recordCompletion(
      span: SpanWrapper,
      decision: CompletionDecision,
  )(implicit traceContext: TraceContext): Unit = {
    val (outcome, reason) = decision match {
      case CompletionDecision.Debit(delta) =>
        span.setAttribute(AccountIdAttribute, delta.account.unwrap)
        (TrafficEnforcementOutcome.Charged, None)
      case CompletionDecision.RecordUncharged(completion) =>
        metrics.markUnchargedCompletion(
          completion.reason.label,
          completion.actAsCategory.label,
          completion.paidTrafficCost.value,
        )
        if (completion.reason == UnchargedReason.MissingCorrelation)
          logger.warn(
            s"No correlation found for completion at offset ${completion.offset}, so it isn't charged: " +
              s"changeIdHash=${completion.changeIdHash.toHexString}, submissionId=${completion.submissionId}, " +
              s"paidTrafficCost=${completion.paidTrafficCost}, actAsCategory=${completion.actAsCategory.label}"
          )
        (TrafficEnforcementOutcome.Skipped, Some(completion.reason.label))
      case CompletionDecision.NoEvent =>
        (TrafficEnforcementOutcome.Skipped, Some(TrafficEnforcementOutcome.ZeroCost))
    }
    span.setAttribute(TrafficEnforcementOutcome.OutcomeAttribute, outcome)
    reason.foreach(span.setAttribute(TrafficEnforcementOutcome.ReasonAttribute, _))
    metrics.markCompletion(outcome, reason)
  }

  protected def recordCompletionFailure(span: SpanWrapper): Unit = {
    span.setAttribute(
      TrafficEnforcementOutcome.OutcomeAttribute,
      TrafficEnforcementOutcome.Failed,
    )
    metrics.markCompletion(TrafficEnforcementOutcome.Failed, None)
  }

  /** Build a projection behavior for a single ingestion stream, reusing the shared storage. */
  def projection(
      projectionId: ProjectionId,
      eventSource: EventSource,
      grpcSourceFactory: Option[Long] => Source[Traced[ProjectionEvent], ?],
  ): Behavior[ProjectionBehavior.Command]

  /** Test-only hook called after each event right after the projection has committed it. Default
    * no-op so production behavior is unaffected.
    */
  protected val onEventCommitted: () => Unit = () => ()

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
      ): Unit = {
        onEventCommitted()
        metrics.projectionTimestamp.updateValue(
          envelope.value.timestamp.toEpochMilli
        )
        logger.trace(s"Processed event ${envelope.value} for projectionId $projectionId")(
          envelope.traceContext
        )
      }
      override def offsetProgress(
          projectionId: ProjectionId,
          env: Traced[ProjectionEvent],
      ): Unit = {
        metrics.projectionOffset.updateValue(env.value.offset)
        // DEBUG because this runs once per completion.
        logger.debug(s"Stored offset ${env.value.offset} for projectionId $projectionId")(
          env.traceContext
        )
      }

      override def error(
          projectionId: ProjectionId,
          env: Traced[ProjectionEvent],
          cause: Throwable,
          recoveryStrategy: HandlerRecoveryStrategy,
      ): Unit = {
        val message =
          s"Error during envelope processing of ${env.value} for projectionId $projectionId"
        cause match {
          // DbExceptionRetryPolicy treats these as transient, so they’re retried and usually succeed,
          // so WARN would mostly be noise.
          case _: SQLTransientException | _: SQLRecoverableException |
              _: SQLNonTransientConnectionException =>
            logger.info(message, cause)(env.traceContext)
          case _ => logger.warn(message, cause)(env.traceContext)
        }
      }
    }

  /** Create a projection source provider from a source of ProjectionEvent
    * @param grpcSourceFactory
    *   the grpcSourceFactory: takes an optional offset (Long) and returns a source pulling events
    *   from this offset. The offset provided will be the last one stored (so processed). This
    *   matches with the LAPI "beginExclusive" semantics: the stream will start at the following
    *   offset.
    *
    * Retryable failures of the source (e.g. the Ledger API not being up yet) are retried forever
    * with exponential backoff, resuming after the last emitted event, so that the projection itself
    * does not fail.
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
          implicit val traceContext: TraceContext = TraceContext.empty
          logger.info(s"Starting ingestion stream for $projectionId with offset $maybeOffset")
          PekkoUtil.recoveringSource(
            init = maybeOffset,
            recoveryStrategy = RecoveryStrategy.exponentialBackoff(
              initialDelay = 100.millis,
              maxDelay = 5.seconds,
              streamName = s"TEA ingestion ${projectionId.id}",
              warnLoggingAttemptThreshold = 65, // ~5 minutes of retrying with exponential backoff
            ) {
              case sre: StatusRuntimeException =>
                LedgerClientUtils.defaultRetryRulesEx(sre).isDefined
              case _ => false
            },
          )(grpcSourceFactory(_).mapMaterializedValue(_ => NotUsed))(event =>
            Some(event.value.offset)
          )
        }
      override def extractOffset(record: Traced[ProjectionEvent]): Long = record.value.offset
      override def extractCreationTime(record: Traced[ProjectionEvent]): Long =
        record.value.timestamp.toEpochMilli
    }
}

object TeaProjectionFactory {

  private[projection] val ApplyDeltaSpanName = "TeaProjectionHandler.applyDelta"
  private[projection] val ApplyCompletionSpanName = "TeaProjectionHandler.applyCompletion"
  private[projection] val AccountIdAttribute = "account_id"
  private[projection] val DeltaAttribute = "delta"
  private[projection] val EventSourceAttribute = "event_source"
  private[projection] val OffsetAttribute = "offset"

  /** Open the shared storage once, based on the configured storage backend. */
  def create(
      storage: Storage,
      config: ProjectionConfig,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      databaseQueryTimeout: PositiveFiniteDuration,
      accountLookupTimeout: PositiveFiniteDuration,
      metrics: TrafficEnforcementMetrics,
      completionPolicy: CompletionAccountingPolicy,
      onEventCommitted: () => Unit = () => (),
  )(implicit system: ActorSystem[?], tracer: Tracer): (TeaProjectionFactory, TeaTrafficStore) = {
    import system.executionContext

    storage match {
      case db: DbStorage =>
        val store = new TeaDbTrafficStore(
          db,
          loggerFactory,
          timeouts,
          databaseQueryTimeout,
          accountLookupTimeout,
        )
        val projection: TeaProjectionFactory =
          new TeaDbProjectionFactory(
            db,
            loggerFactory,
            store,
            config,
            metrics,
            completionPolicy,
            onEventCommitted,
          )
        (projection, store)
      case _: MemoryStorage =>
        val store = new TeaMemoryTrafficStore(loggerFactory)
        val projection: TeaProjectionFactory =
          new TeaMemoryProjectionFactory(
            loggerFactory,
            store,
            metrics,
            completionPolicy,
            onEventCommitted,
          )
        (projection, store)
    }
  }
}
