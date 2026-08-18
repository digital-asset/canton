// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ConsensusStatusTest extends AnyWordSpec with BftSequencerBaseTest {
  private val from = BftNodeId("node0")

  "EpochStatus" should {
    "reject a wire message with an empty segments list rather than let it through, " +
      "as it can otherwise lead to a divide-by-zero when processing it" in {
        val proto = v30.EpochStatus(epochNumber = 0L, segments = Seq.empty)

        ConsensusStatus.EpochStatus.fromProto(from, proto)(ByteString.EMPTY) shouldBe Left(
          ProtoDeserializationError.ValueConversionError("segments", "must not be empty")
        )
      }
  }
}
