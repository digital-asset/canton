// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import cats.syntax.either.*
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.completions.CompletionServiceClient
import com.digitalasset.canton.ledger.client.services.state.StateServiceClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.projection.{
  AccountId,
  DeltaEvent,
  EventSource,
  EventType,
  OffsetDeltaEvent,
  ProjectionEvent,
}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Builds the debit delta stream from the LAPI completions.
  *
  * Restart and offset-resume are handled by the Pekko projection that consumes [[grpcSource]]: the
  * projection passes the last persisted offset and restarts the source on failure.
  */
final class TeaDebitIngestionService(
    completionServiceClient: CompletionServiceClient,
    stateServiceClient: StateServiceClient,
    config: ProjectionConfig,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Offset-resume aware source factory: builds the debit delta source starting (exclusively) from
    * the offset persisted by the projection, or from the configured ledger end.
    */
  def grpcSource(beginOffset: Option[Long]): Source[Traced[ProjectionEvent], NotUsed] =
    TraceContext.withNewTraceContext("tea-debit-ingestion") { implicit tc =>
      val offsetF = beginOffset
        .map(Future.successful)
        .orElse(config.initialCompletionOffsetBeginExclusive.map(Future.successful))
        .getOrElse {
          // Without a known offset or specific config, we start from the current ledger end
          stateServiceClient.getLedgerEndOffset()
        }

      Source
        .future(offsetF)
        .flatMap { offset =>
          logger.info(s"(Re)connecting completion stream at offset $offset")
          completionServiceClient
            .getCompletionsSource(
              begin = offset,
              parties = Seq.empty,
            )
            .mapConcat(decodeRecord)
        }
    }

  private def decodeRecord(
      response: CompletionStreamResponse
  ): Option[Traced[ProjectionEvent]] =
    response.completionResponse match {
      case CompletionStreamResponse.CompletionResponse.Completion(c) =>
        // Deserialize only if the trace context is non empty, otherwise it logs a warning
        implicit val tc: TraceContext = c.traceContext
          .map(tc => fromDamlProtoSafeOpt(noTracingLogger)(Some(tc)).traceContext)
          .getOrElse(TraceContext.empty)

        logger.debug(
          s"Received a completion: offset=${c.offset}, actAs=${c.actAs}, paidTrafficCost=${c.paidTrafficCost}"
        )

        val synchronizerTime = c.synchronizerTime.getOrElse(
          throw new IllegalStateException(s"Empty synchronizer time on completion $c")
        )
        val recordTime = synchronizerTime.recordTime.getOrElse(
          throw new IllegalStateException(s"Empty recordTime time on completion $c")
        )
        val cantonTimestampRecordTime = CantonTimestamp
          .fromProtoTimestamp(recordTime)
          .valueOr(err => throw new IllegalStateException(s"Invalid recordTime $err"))

        val deltaEvent = DeltaEvent(
          // delta is negative: it's a debit
          delta = -c.paidTrafficCost,
          timestamp = cantonTimestampRecordTime,
          eventType = EventType.Usage,
          eventSource = EventSource.LedgerAPI,
        )

        val offsetDeltaEvent = OffsetDeltaEvent(deltaEvent, c.offset)

        c.actAs.toList match {
          case one :: Nil =>
            AccountId.create(one) match {
              case Left(err) =>
                logger.warn(
                  s"actAs $one is not a valid AccountId. Completion will be skipped: $err"
                )
                None
              case Right(accountId) =>
                Some(Traced(ProjectionEvent(accountId, offsetDeltaEvent)))
            }
          case Nil =>
            logger.error(
              s"No actAs for completion $c. This shouldn't happen. Cost won't be deducted."
            )
            None
          case _ =>
            // Should only be possible for local parties for now as we don't support multi party submission for external parties
            logger.info(s"More than one actAs parties for completion :$c. Cost won't be deducted.")
            None
        }
      case _ => None
    }
}
