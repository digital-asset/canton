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
import com.digitalasset.canton.tracing.{TraceContext, Traced, W3CTraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class BatchIdValidationTest extends AnyWordSpec with BftSequencerBaseTest {

  val traceparent = "00-6a61d0a2000000002ea86a24cff7cfcd-7358c6deaed4868f-01"
  val tracestate = "dd=s:1;p:7358c6deaed4868f;t.dm:-1;t.tid:6a61d0a200000000;t.ksr:1"

  "A batch" when {
    "it includes an ordering request that has a trace contexts with state" should {
      "validate after serialization and deserialization" in {
        Table(
          (W3CTraceContext.TraceparentHeader.value, W3CTraceContext.TracestateHeader.value),
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
                      W3CTraceContext.TraceparentHeader -> tp,
                      W3CTraceContext.TracestateHeader -> ts,
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
          val batchIdBeforeSerialization =
            BatchId.from(batch, logger)(implicitly, TraceContext.empty)
          val serializedBatch = batch.toProtoV30
          val deserializedBatch =
            OrderingRequestBatch
              .fromProtoV30(serializedBatch)
              .getOrElse(fail("Failed to deserialize batch"))
          val batchIdAfterDeserialization =
            BatchId.from(deserializedBatch, logger)(implicitly, TraceContext.empty)

          batchIdBeforeSerialization shouldBe batchIdAfterDeserialization
        }
      }
    }
  }

  "BatchId.from" should {
    "produce a stable id for a fixed batch" in {
      val batch =
        OrderingRequestBatch.create(
          requests = Seq(
            Traced(
              OrderingRequest(
                tag = BlockFormat.SendTag,
                messageId = "aMessageId",
                payload = ByteString.copyFromUtf8("payload1"),
                orderingStartInstant = Some(Instant.parse("2026-01-01T00:00:00Z")),
              )
            )(TraceContext.empty)
          ),
          epochNumber = EpochNumber(7L),
        )

      ProtocolVersion.supported.foreach { protocolVersion =>
        BatchId.from(batch, logger)(protocolVersion, traceContext).hash.toHexString shouldBe
          (protocolVersion match {
            case pv if pv <= ProtocolVersion.v36 =>
              "1220adaf0c41ab56b822d643fc006ba8aae59efc9cc79d5c1604600d5b560bc80cd2"
            case pv =>
              "122081e19cab056cf75722b0b8822e0b8feeec486fae8b7b4a0a83294736bd7b9684"
          })
      }
    }
  }
}
