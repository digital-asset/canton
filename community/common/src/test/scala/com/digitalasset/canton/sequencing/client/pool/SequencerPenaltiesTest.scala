// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeDouble, PositiveDouble, PositiveInt}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology.{Namespace, SequencerId}
import org.scalatest.wordspec.AnyWordSpec

final class SequencerPenaltiesTest extends AnyWordSpec with BaseTest {
  private def sequencerId(index: Int): SequencerId =
    SequencerId.tryCreate(s"sequencer-$index", Namespace(Fingerprint.tryFromString("namespace")))

  private val seq1 = sequencerId(1)
  private val seq2 = sequencerId(2)
  private val seq3 = sequencerId(3)

  "SequencerPenalties" should {
    "start all penalties at 0" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.get(seq1) shouldBe NonNegativeDouble.zero
    }

    "accumulate increases" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.increase(seq1, PositiveInt.one, reason = "test")
      penalties.increase(seq1, PositiveInt.two, reason = "test")
      penalties.increase(seq2, PositiveInt.tryCreate(4), reason = "test")

      penalties.get(seq1) shouldBe NonNegativeDouble.tryCreate(3)
      penalties.get(seq2) shouldBe NonNegativeDouble.tryCreate(4)
      penalties.get(seq3) shouldBe NonNegativeDouble.zero
    }

    "decay all penalties" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.increase(seq1, PositiveInt.tryCreate(8), reason = "test")
      penalties.increase(seq2, PositiveInt.two, reason = "test")

      penalties.decay(
        factor = NonNegativeDouble.tryCreate(0.5),
        zeroThreshold = PositiveDouble.tryCreate(0.01),
      )

      penalties.get(seq1) shouldBe NonNegativeDouble.tryCreate(4.0)
      penalties.get(seq2) shouldBe NonNegativeDouble.one
      penalties.get(seq3) shouldBe NonNegativeDouble.zero
    }

    "reset decayed penalties below the threshold to 0" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.increase(seq1, PositiveInt.two, reason = "test")
      penalties.increase(seq2, PositiveInt.one, reason = "test")

      // 2/128 = 0.015625 >= 0.01 and 1/128 = 0.0078125 < 0.01
      penalties.decay(
        factor = NonNegativeDouble.tryCreate(1.0 / 128),
        zeroThreshold = PositiveDouble.tryCreate(0.01),
      )

      penalties.get(seq1) shouldBe NonNegativeDouble.tryCreate(2.0 / 128)
      penalties.get(seq2) shouldBe NonNegativeDouble.zero
    }

    "bring a penalty back to 0 after a bounded number of decays" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.increase(seq1, PositiveInt.one, reason = "test")

      // 0.9^43 ≈ 0.0108 >= 0.01 and 0.9^44 ≈ 0.0097 < 0.01
      (1 to 43).foreach(_ =>
        penalties.decay(
          factor = NonNegativeDouble.tryCreate(0.9),
          zeroThreshold = PositiveDouble.tryCreate(0.01),
        )
      )
      penalties.get(seq1) should be > NonNegativeDouble.zero

      penalties.decay(
        factor = NonNegativeDouble.tryCreate(0.9),
        zeroThreshold = PositiveDouble.tryCreate(0.01),
      )
      penalties.get(seq1) shouldBe NonNegativeDouble.zero
    }

    "start again from 0 after a penalty has been reset" in {
      val penalties = new SequencerPenalties(loggerFactory)
      penalties.increase(seq1, PositiveInt.one, reason = "test")
      penalties.decay(
        factor = NonNegativeDouble.tryCreate(1.0 / 128),
        zeroThreshold = PositiveDouble.tryCreate(0.01),
      )

      penalties.increase(seq1, PositiveInt.one, reason = "test")

      penalties.get(seq1) shouldBe NonNegativeDouble.one
    }
  }
}
