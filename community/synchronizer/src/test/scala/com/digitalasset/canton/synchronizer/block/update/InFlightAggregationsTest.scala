// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{AggregationId, AggregationRule}
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.google.protobuf.ByteString

import scala.collection.immutable.SortedMap

class InFlightAggregationsTest extends BaseTestWordSpec {

  private def mkRule(senders: Seq[Member]): AggregationRule =
    AggregationRule(
      eligibleSenders = NonEmpty.from(senders).value,
      threshold = PositiveInt.one,
      protocolVersion = testedProtocolVersion,
    )

  private def mkAggId(str: String) =
    AggregationId(
      Hash.digest(HashPurpose.AggregationId, ByteString.copyFromUtf8(str), HashAlgorithm.Sha256)
    )

  "InFlightAggregations" should {
    "expire the right one" in {
      val ts1 = CantonTimestamp.Epoch
      val ts2 = ts1.plusSeconds(1)
      val agg1 = InFlightAggregation.tryCreate(
        aggregatedSenders = SortedMap.empty,
        maxSequencingTimestamp = ts2,
        rule = mkRule(Seq(DefaultTestIdentities.mediatorId)),
      )
      val agg1Id = mkAggId("aaaa")
      val agg2 = InFlightAggregation.tryCreate(
        aggregatedSenders = SortedMap.empty,
        maxSequencingTimestamp = ts1,
        rule = mkRule(Seq(DefaultTestIdentities.participant1)),
      )
      val agg2Id = mkAggId("bbbb")

      val step1 = InFlightAggregations.empty.updated(agg1Id, agg1)

      step1.contains(agg1Id) shouldBe true
      step1.contains(agg2Id) shouldBe false

      val step2 = step1.updated(agg2Id, agg2)

      step2.contains(agg2Id) shouldBe true

      // expire nothing
      val step3 = step2.cleanExpired(ts1.immediatePredecessor)
      step3.contains(agg1Id) shouldBe true
      step3.contains(agg2Id) shouldBe true

      // expire first
      val step4 = step3.cleanExpired(ts1)
      step4.contains(agg1Id) shouldBe true
      step4.contains(agg2Id) shouldBe false
      step4.byId.toSeq should have length (1)
      step4.expiryIndex.toSeq should have length (1)

      // expire second
      val step5 = step4.cleanExpired(ts2)
      step5.contains(agg1Id) shouldBe false
      step5.byId shouldBe empty
      step5.expiryIndex shouldBe empty

    }
  }

}
