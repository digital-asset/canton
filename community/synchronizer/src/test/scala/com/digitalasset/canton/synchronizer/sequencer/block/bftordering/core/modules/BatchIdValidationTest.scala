// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules

import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.sequencing.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.tracing.{Traced, W3CTraceContext}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class BatchIdValidationTest extends AnyWordSpec with BftSequencerBaseTest {

  val TraceParentHeaderName = "traceparent"
  val TraceStateHeaderName = "tracestate"
  val traceparent = "00-6a61d0a2000000002ea86a24cff7cfcd-7358c6deaed4868f-01"
  val tracestate = "dd=s:1;p:7358c6deaed4868f;t.dm:-1;t.tid:6a61d0a200000000;t.ksr:1"

  "A batch" when {
    "it includes an ordering request that has a trace contexts with state" should {
      "validate after serialization and deserialization" in {
        Table(
          (TraceParentHeaderName, TraceStateHeaderName),
          (traceparent, tracestate),
          ("", tracestate),
          (traceparent, ""),
          ("", ""),
        ).forEvery { (tp, ts) =>
          val traceContext =
            BftBlockOrderer
              .adaptOrderingRequestTraceContextForBatchValidation(logger, "aMessageId")(
                W3CTraceContext
                  .fromHeaders(
                    Map(
                      TraceParentHeaderName -> tp,
                      TraceStateHeaderName -> ts,
                    )
                  )
                  .value
                  .toTraceContext
              )
          val batch =
            OrderingRequestBatch.create(
              requests = Seq(
                Traced(
                  OrderingRequest(
                    tag = BlockFormat.SendTag,
                    messageId = "aMessageId",
                    payload = ByteString.copyFromUtf8("payload1"),
                    orderingStartInstant = Some(Instant.now()),
                  )
                )(traceContext)
              ),
              epochNumber = EpochNumber.First,
            )
          val batchIdBeforeSerialization = BatchId.from(batch)
          val serializedBatch = batch.toProtoV30
          val deserializedBatch =
            OrderingRequestBatch
              .fromProtoV30(serializedBatch)
              .getOrElse(fail("Failed to deserialize batch"))
          val batchIdAfterDeserialization = BatchId.from(deserializedBatch)

          batchIdBeforeSerialization shouldBe batchIdAfterDeserialization
        }
      }
    }
  }
}
