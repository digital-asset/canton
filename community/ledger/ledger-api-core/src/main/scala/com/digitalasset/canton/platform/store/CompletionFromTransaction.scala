// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse.CompletionResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.completion.Completion.DeduplicationPeriod.Empty
import com.daml.ledger.api.v2.offset_checkpoint.SynchronizerTime
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.duration.Duration
import com.google.rpc.status.Status as StatusProto
import io.grpc.Status

// Turn a stream of transactions into a stream of completions for a given user and set of parties
object CompletionFromTransaction {
  val OkStatus = StatusProto.of(Status.Code.OK.value(), "", Seq.empty)
  private val RejectionUpdateId = ""

  def acceptedCompletion(
      submitters: Set[String],
      recordTime: Timestamp,
      offset: Offset,
      commandId: String,
      updateId: String,
      userId: String,
      synchronizerId: String,
      traceContext: TraceContext,
      optSubmissionId: Option[String] = None,
      optDeduplicationOffset: Option[Long] = None,
      optDeduplicationDurationSeconds: Option[Long] = None,
      optDeduplicationDurationNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      completionResponse = CompletionResponse.Completion(
        toApiCompletion(
          submitters = submitters,
          commandId = commandId,
          updateId = updateId,
          userId = userId,
          traceContext = traceContext,
          optStatus = Some(OkStatus),
          optSubmissionId = optSubmissionId,
          optDeduplicationOffset = optDeduplicationOffset,
          optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
          optDeduplicationDurationNanos = optDeduplicationDurationNanos,
          offset = offset.unwrap,
          synchronizerTime = Some(toApiSynchronizerTime(synchronizerId, recordTime)),
        )
      )
    )

  def rejectedCompletion(
      submitters: Set[String],
      recordTime: Timestamp,
      offset: Offset,
      commandId: String,
      status: StatusProto,
      userId: String,
      synchronizerId: String,
      traceContext: TraceContext,
      optSubmissionId: Option[String] = None,
      optDeduplicationOffset: Option[Long] = None,
      optDeduplicationDurationSeconds: Option[Long] = None,
      optDeduplicationDurationNanos: Option[Int] = None,
  ): CompletionStreamResponse =
    CompletionStreamResponse.of(
      completionResponse = CompletionResponse.Completion(
        toApiCompletion(
          submitters = submitters,
          commandId = commandId,
          updateId = RejectionUpdateId,
          userId = userId,
          traceContext = traceContext,
          optStatus = Some(status),
          optSubmissionId = optSubmissionId,
          optDeduplicationOffset = optDeduplicationOffset,
          optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
          optDeduplicationDurationNanos = optDeduplicationDurationNanos,
          offset = offset.unwrap,
          synchronizerTime = Some(toApiSynchronizerTime(synchronizerId, recordTime)),
        )
      )
    )

  private def toApiSynchronizerTime(
      synchronizerId: String,
      recordTime: Timestamp,
  ): SynchronizerTime =
    SynchronizerTime.of(
      synchronizerId = synchronizerId,
      recordTime = Some(fromInstant(recordTime.toInstant)),
    )

  def toApiCompletion(
      submitters: Set[String],
      commandId: String,
      updateId: String,
      userId: String,
      traceContext: TraceContext,
      optStatus: Option[StatusProto],
      optSubmissionId: Option[String],
      optDeduplicationOffset: Option[Long],
      optDeduplicationDurationSeconds: Option[Long],
      optDeduplicationDurationNanos: Option[Int],
      offset: Long,
      synchronizerTime: Option[SynchronizerTime],
  ): Completion = {
    val completionWithMandatoryFields = Completion(
      commandId = commandId,
      status = optStatus,
      updateId = updateId,
      userId = userId,
      actAs = submitters.toSeq,
      submissionId = "", // will be adapted later
      deduplicationPeriod = Empty, // will be adapted later
      traceContext = SerializableTraceContext(traceContext).toDamlProtoOpt,
      offset = offset,
      synchronizerTime = synchronizerTime,
    )
    val optDeduplicationPeriod = toApiDeduplicationPeriod(
      optDeduplicationOffset = optDeduplicationOffset,
      optDeduplicationDurationSeconds = optDeduplicationDurationSeconds,
      optDeduplicationDurationNanos = optDeduplicationDurationNanos,
    )
    (optSubmissionId, optDeduplicationPeriod) match {
      case (Some(submissionId), Some(deduplicationPeriod)) =>
        completionWithMandatoryFields.copy(
          submissionId = submissionId,
          deduplicationPeriod = deduplicationPeriod,
        )
      case (Some(submissionId), None) =>
        completionWithMandatoryFields.copy(
          submissionId = submissionId
        )
      case (None, Some(deduplicationPeriod)) =>
        completionWithMandatoryFields.copy(
          deduplicationPeriod = deduplicationPeriod
        )
      case _ =>
        completionWithMandatoryFields
    }
  }

  private def toApiDeduplicationPeriod(
      optDeduplicationOffset: Option[Long],
      optDeduplicationDurationSeconds: Option[Long],
      optDeduplicationDurationNanos: Option[Int],
  ): Option[Completion.DeduplicationPeriod] =
    // The only invariant that should hold, considering legacy data, is that either
    // the deduplication duration seconds and nanos are both populated, or neither is.
    (
      optDeduplicationOffset,
      (optDeduplicationDurationSeconds, optDeduplicationDurationNanos),
    ) match {
      case (None, (None, None)) => None
      case (Some(offset), _) =>
        Some(
          Completion.DeduplicationPeriod.DeduplicationOffset(offset)
        )
      case (_, (Some(deduplicationDurationSeconds), Some(deduplicationDurationNanos))) =>
        Some(
          Completion.DeduplicationPeriod.DeduplicationDuration(
            new Duration(
              seconds = deduplicationDurationSeconds,
              nanos = deduplicationDurationNanos,
            )
          )
        )
      case _ =>
        throw new IllegalArgumentException(
          "One of deduplication duration seconds and nanos has been provided " +
            "but they must be either both provided or both absent"
        )
    }
}
