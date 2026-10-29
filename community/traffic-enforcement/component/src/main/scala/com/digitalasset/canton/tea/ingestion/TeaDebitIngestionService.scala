// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.client.services.completions.CompletionServiceClient
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.tea.projection.{CompletionRecord, ProjectionEvent}
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.Status
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

/** Builds the stream of completions to charge from the LAPI completions.
  *
  * Restart and offset-resume are handled by the Pekko projection that consumes [[grpcSource]]: the
  * projection passes the last persisted offset and restarts the source on failure.
  *
  * @param ledgerEndAtStart
  *   the participant's ledger end when TEA was started, before the Ledger API accepted submissions.
  *   Ingestion starts from it while no offset is persisted or configured. Using a fixed offset
  *   rather than querying the ledger end on each (re)connection means that completions recorded
  *   while the stream is retrying are not skipped. Offset checkpoints and completions with invalid
  *   identifiers are emitted as checkpoint events, so the projection moves past this offset even if
  *   the stream carries no valid completion, before pruning can remove it.
  */
final class TeaDebitIngestionService(
    completionServiceClient: CompletionServiceClient,
    ledgerEndAtStart: Long,
    config: ProjectionConfig,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Offset-resume aware source factory: builds the completion source starting (exclusively) from
    * the offset persisted by the projection, else the configured offset, else the ledger end when
    * TEA was started.
    */
  def grpcSource(beginOffset: Option[Long]): Source[Traced[ProjectionEvent], NotUsed] =
    TraceContext.withNewTraceContext("tea-debit-ingestion") { implicit tc =>
      val offset = beginOffset
        .orElse(config.initialCompletionOffsetBeginExclusive)
        .getOrElse(ledgerEndAtStart)
      logger.info(s"(Re)connecting completion stream at offset $offset")
      completionServiceClient
        .getCompletionsSource(
          begin = offset,
          parties = Seq.empty,
        )
        .mapConcat(decodeRecord)
    }

  private def decodeRecord(
      response: CompletionStreamResponse
  ): Option[Traced[ProjectionEvent]] =
    response.completionResponse match {
      case CompletionStreamResponse.CompletionResponse.Completion(c) =>
        implicit val tc: TraceContext =
          fromDamlProtoSafeOpt(noTracingLogger)(c.traceContext).traceContext

        logger.debug(
          s"Received a completion: offset=${c.offset}, actAs=${c.actAs}, paidTrafficCost=${c.paidTrafficCost}"
        )

        changeId(c) match {
          case Right(changeId) => Some(doDecode(c, changeId))
          case Left(err) =>
            logger.error(s"Invalid identifiers on completion $c: $err. Cost won't be deducted.")
            Some(Traced(ProjectionEvent.Checkpoint(c.offset, recordTime(c))))
        }
      case CompletionStreamResponse.CompletionResponse.OffsetCheckpoint(checkpoint) =>
        implicit val tc: TraceContext = TraceContext.empty
        logger.debug(s"Received a checkpoint at offset ${checkpoint.offset}")
        OffsetCheckpoints.toProjectionEvent(checkpoint)
      case CompletionStreamResponse.CompletionResponse.Empty => None
    }

  private def recordTime(completion: Completion): CantonTimestamp = {
    val synchronizerTime = completion.synchronizerTime.getOrElse(
      throw new IllegalStateException(s"Empty synchronizer time on completion $completion")
    )
    val recordTime = synchronizerTime.recordTime.getOrElse(
      throw new IllegalStateException(s"Empty recordTime time on completion $completion")
    )
    CantonTimestamp
      .fromProtoTimestamp(recordTime)
      .valueOr(err => throw new IllegalStateException(s"Invalid recordTime $err"))
  }

  private def doDecode(completion: Completion, changeId: ChangeId)(implicit tc: TraceContext) = {
    val paidTrafficCost = NonNegativeLong
      .create(completion.paidTrafficCost)
      .getOrElse(
        throw new IllegalStateException(
          s"Negative paid traffic cost received on completion: ${completion.paidTrafficCost}"
        )
      )

    val record = CompletionRecord(
      changeId = changeId,
      submissionId = completion.submissionId,
      paidTrafficCost = paidTrafficCost,
      accepted = completion.status.forall(_.code == Status.Code.OK.value),
    )

    Traced(ProjectionEvent.Completion(completion.offset, recordTime(completion), record))
  }

  private def changeId(completion: Completion): Either[String, ChangeId] =
    for {
      userId <- Ref.UserId.fromString(completion.userId)
      commandId <- Ref.CommandId.fromString(completion.commandId)
      actAs <- completion.actAs.traverse(Ref.Party.fromString)
    } yield ChangeId(userId, commandId, actAs.toSet)
}
