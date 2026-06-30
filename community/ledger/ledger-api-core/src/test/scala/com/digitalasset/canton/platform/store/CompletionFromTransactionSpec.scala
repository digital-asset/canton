// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.api.v2.completion.Completion.DeduplicationPeriod
import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.CompletionFromTransaction.CommonCompletionProperties
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.daml.lf.data.Time
import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import com.google.rpc.status.Status
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class CompletionFromTransactionSpec
    extends AnyWordSpec
    with Matchers
    with OptionValues
    with TestEssentials
    with TableDrivenPropertyChecks {

  "CompletionFromTransaction" should {
    "create an accepted completion" in {
      val testCases = Table(
        (
          "submissionId",
          "deduplicationOffset",
          "deduplicationDurationSeconds",
          "deduplicationDurationNanos",
          "expectedSubmissionId",
          "expectedDeduplicationPeriod",
        ),
        (Some("submissionId"), None, None, None, "submissionId", DeduplicationPeriod.Empty),
        (None, None, None, None, "", DeduplicationPeriod.Empty),
        (
          None,
          Some(12345678L),
          None,
          None,
          "",
          DeduplicationPeriod.DeduplicationOffset(12345678L),
        ),
        (
          None,
          None,
          Some(1L),
          Some(2),
          "",
          DeduplicationPeriod
            .DeduplicationDuration(new Duration(1, 2))
            .asInstanceOf[ // otherwise the compilation fails due to an inference warning
              DeduplicationPeriod
            ],
        ),
      )

      forEvery(testCases) {
        (
            submissionId,
            deduplicationOffset,
            deduplicationDurationSeconds,
            deduplicationDurationNanos,
            expectedSubmissionId,
            expectedDeduplicationPeriod,
        ) =>
          val completionStream = CompletionFromTransaction.acceptedCompletion(
            mkProps(
              submitters = Set("party1", "party2"),
              submissionId = submissionId,
              deduplicationOffset = deduplicationOffset,
              deduplicationDurationSeconds = deduplicationDurationSeconds,
              deduplicationDurationNanos = deduplicationDurationNanos,
              trafficCost = 4324L,
            ),
            TestUpdateId("updateId"),
          )

          val completion = completionStream.completionResponse.completion.value
          completion.synchronizerTime.value.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
          completion.offset shouldBe 1L

          completion.commandId shouldBe "commandId"
          completion.updateId shouldBe TestUpdateId("updateId").toHexString
          completion.userId shouldBe "userId"
          completion.submissionId shouldBe expectedSubmissionId
          completion.deduplicationPeriod shouldBe expectedDeduplicationPeriod
          completion.actAs.toSet shouldBe Set("party1", "party2")
          completion.paidTrafficCost shouldBe 4324L
      }
    }

    "fail on an invalid deduplication duration" in {
      val testCases = Table(
        ("deduplicationDurationSeconds", "deduplicationDurationNanos"),
        (Some(1L), None),
        (None, Some(1)),
      )

      forEvery(testCases) { (deduplicationDurationSeconds, deduplicationDurationNanos) =>
        an[IllegalArgumentException] shouldBe thrownBy(
          CompletionFromTransaction.acceptedCompletion(
            mkProps(
              submitters = Set.empty,
              deduplicationDurationSeconds = deduplicationDurationSeconds,
              deduplicationDurationNanos = deduplicationDurationNanos,
              trafficCost = 4234L,
            ),
            updateId = TestUpdateId("updateId"),
          )
        )
      }
    }

    "create a rejected completion" in {
      val status = Status.of(io.grpc.Status.Code.INTERNAL.value(), "message", Seq.empty)
      val completionStream = CompletionFromTransaction.rejectedCompletion(
        commonCompletionProperties =
          mkProps(completionOffset = Offset.tryFromLong(2L), trafficCost = 4324L),
        status = status,
      )

      val completion = completionStream.completionResponse.completion.value
      completion.synchronizerTime.value.recordTime shouldBe Some(Timestamp(Instant.EPOCH))
      completion.offset shouldBe 2L

      completion.commandId shouldBe "commandId"
      completion.userId shouldBe "userId"
      completion.submissionId shouldBe "submissionId"
      completion.status shouldBe Some(status)
      completion.actAs shouldBe Seq("party")
      completion.paidTrafficCost shouldBe 4324
    }

    "include transaction hash on accepted completion when provided" in {
      val hash = ByteString.copyFromUtf8("tx-hash-1")
      val completionStream = CompletionFromTransaction.acceptedCompletion(
        mkProps(transactionHash = Some(hash)),
        TestUpdateId("updateId"),
      )

      val completion = completionStream.completionResponse.completion.value
      completion.transactionHash shouldBe Some(hash)
    }

    "include transaction hash on rejected completion when provided" in {
      val hash = ByteString.copyFromUtf8("tx-hash-2")
      val status = Status.of(io.grpc.Status.Code.INTERNAL.value(), "message", Seq.empty)
      val completionStream = CompletionFromTransaction.rejectedCompletion(
        commonCompletionProperties = mkProps(transactionHash = Some(hash)),
        status = status,
      )

      val completion = completionStream.completionResponse.completion.value
      completion.transactionHash shouldBe Some(hash)
    }

    "not include transaction hash when not provided" in {
      val completionStream = CompletionFromTransaction.acceptedCompletion(
        mkProps(),
        TestUpdateId("updateId"),
      )

      val completion = completionStream.completionResponse.completion.value
      completion.transactionHash shouldBe None
    }

  }

  private def mkProps(
      submitters: Set[String] = Set("party"),
      completionOffset: Offset = Offset.firstOffset,
      submissionId: Option[String] = Some("submissionId"),
      deduplicationOffset: Option[Long] = None,
      deduplicationDurationSeconds: Option[Long] = None,
      deduplicationDurationNanos: Option[Int] = None,
      trafficCost: Long = 0L,
      transactionHash: Option[ByteString] = None,
  ): CommonCompletionProperties =
    CommonCompletionProperties.createFromRecordTimeAndSynchronizerId(
      submitters = submitters,
      recordTime = Time.Timestamp.Epoch,
      completionOffset = completionOffset,
      commandId = "commandId",
      userId = "userId",
      submissionId = submissionId,
      synchronizerId = "synchronizer id",
      traceContext = SerializableTraceContext(traceContext).toDamlProto,
      deduplicationOffset = deduplicationOffset,
      deduplicationDurationSeconds = deduplicationDurationSeconds,
      deduplicationDurationNanos = deduplicationDurationNanos,
      trafficCost = trafficCost,
      transactionHash = transactionHash,
    )

}
