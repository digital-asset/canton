// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

final class SequencerConnectionPenaltyConfigTest extends AnyWordSpec with BaseTest {
  private val penaltyConfig = SequencerConnectionPenaltyConfig(
    enabled = true,
    decayInterval = PositiveFiniteDuration.tryOfSeconds(3),
    halfLife = PositiveFiniteDuration.tryOfSeconds(30),
    zeroThreshold = PositiveDouble.tryCreate(0.01),
  )

  "SequencerConnectionPenaltyConfig" should {
    "reject invalid values when parsing" in {
      val zero = Some(NonNegativeFiniteDuration.Zero.toProtoPrimitive)

      forAll(
        Seq(
          penaltyConfig.toProtoV30.copy(decayInterval = None),
          penaltyConfig.toProtoV30.copy(halfLife = None),
          penaltyConfig.toProtoV30.copy(decayInterval = zero),
          penaltyConfig.toProtoV30.copy(halfLife = zero),
          penaltyConfig.toProtoV30.copy(zeroThreshold = 0.0),
        )
      ) { proto =>
        SequencerConnectionPenaltyConfig.fromProtoV30(proto).left.value shouldBe a[
          ProtoDeserializationError
        ]
      }
    }

    "derive the decay factor from the half-life" in {
      def factor(interval: PositiveFiniteDuration, halfLife: PositiveFiniteDuration): Double =
        SequencerConnectionPenaltyConfig(
          enabled = true,
          decayInterval = interval,
          halfLife = halfLife,
          zeroThreshold = PositiveDouble.tryCreate(0.01),
        ).decayFactor.value

      factor(
        interval = PositiveFiniteDuration.tryOfSeconds(10),
        halfLife = PositiveFiniteDuration.tryOfSeconds(10),
      ) shouldBe 0.5
      factor(
        interval = PositiveFiniteDuration.tryOfSeconds(20),
        halfLife = PositiveFiniteDuration.tryOfSeconds(10),
      ) shouldBe 0.25
      factor(
        interval = PositiveFiniteDuration.tryOfMillis(500),
        halfLife = PositiveFiniteDuration.tryOfSeconds(1),
      ) shouldBe math.sqrt(0.5) +- 1e-12
    }
  }

  "SequencerConnections" should {
    "use the default penalty mechanism config when it is absent from the proto" in {
      val connection = GrpcSequencerConnection.create("http://localhost:1234").value
      def connections(penaltyConfig: SequencerConnectionPenaltyConfig): SequencerConnections =
        SequencerConnections
          .many(
            NonEmpty(Seq, connection),
            PositiveInt.one,
            NonNegativeInt.zero,
            SubmissionRequestAmplification.NoAmplification,
            SequencerConnectionPoolDelays.default,
            SubscriptionLivenessLimits.default,
            penaltyConfig,
          )
          .value
      val proto =
        connections(penaltyConfig).toProtoV30.copy(sequencerConnectionPenaltyConfig = None)

      SequencerConnections.fromProtoV30(proto) shouldBe Right(
        connections(SequencerConnectionPenaltyConfig.default)
      )
    }
  }
}
