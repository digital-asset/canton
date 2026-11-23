// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.{BftOrdererLimits, BftOrdererLimitsView}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class OrderingRequestBatchLimitsTest extends AnyWordSpec with BftSequencerBaseTest {

  private val bound = 3
  private val limits = BftOrdererLimitsView.default.copy(
    limits = BftOrdererLimits.default.copy(maxOrderingRequests = PositiveInt.three)
  )

  private val enforcing = ProtocolVersionValidation.PV(ProtocolVersion.dev)

  private def batchWith(requests: Int) =
    OrderingRequestBatch
      .create(
        Seq.fill(requests)(
          Traced(
            OrderingRequest(
              tag = BlockFormat.SendTag,
              messageId = "aMessageId",
              payload = ByteString.copyFromUtf8("payload"),
              orderingStartInstant = None,
            )
          )(TraceContext.empty)
        ),
        EpochNumber.First,
      )
      .toProtoV30

  private def parse(requests: Int, pvv: ProtocolVersionValidation = enforcing) =
    OrderingRequestBatch.fromProtoV30(pvv, limits, batchWith(requests))

  "OrderingRequestBatch" should {
    "accept exactly the bound and reject bound + 1 for ordering_requests" in {
      parse(bound) shouldBe a[Right[?, ?]]

      parse(bound + 1) should matchPattern {
        case Left(ProtoDeserializationError.InvariantViolation(Some("ordering_requests"), _)) =>
      }
    }

    "enforce the limit from the dev protocol version and with AlwaysValidation only" in {
      parse(bound + 1, ProtocolVersionValidation.AlwaysValidation) shouldBe a[Left[?, ?]]
      parse(bound + 1, enforcing) shouldBe a[Left[?, ?]]

      // Older (stable) protocol versions and NoValidation bypass the check
      parse(bound + 1, ProtocolVersionValidation.PV(ProtocolVersion.v37)) shouldBe a[Right[?, ?]]
      parse(bound + 1, ProtocolVersionValidation.PV(ProtocolVersion.v36)) shouldBe a[Right[?, ?]]
      parse(bound + 1, ProtocolVersionValidation.NoValidation) shouldBe a[Right[?, ?]]
    }
  }
}
