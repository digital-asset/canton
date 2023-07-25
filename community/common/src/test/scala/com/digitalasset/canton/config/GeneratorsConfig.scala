// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsConfig {
  // Refined Int
  val nonNegativeIntGen: Gen[NonNegativeInt] =
    Gen.choose(0, Int.MaxValue).map(NonNegativeInt.tryCreate)
  implicit val nonNegativeIntArb: Arbitrary[NonNegativeInt] = Arbitrary(nonNegativeIntGen)
  val positiveIntGen: Gen[PositiveInt] = Gen.choose(1, Int.MaxValue).map(PositiveInt.tryCreate)
  implicit val positiveIntArb: Arbitrary[PositiveInt] = Arbitrary(positiveIntGen)

  // Refined Long
  val nonNegativeLongGen: Gen[NonNegativeLong] =
    Gen.choose(0, Long.MaxValue).map(NonNegativeLong.tryCreate)
  implicit val nonNegativeLongArb: Arbitrary[NonNegativeLong] = Arbitrary(nonNegativeLongGen)

  val positiveLongGen: Gen[PositiveLong] = Gen.choose(1, Long.MaxValue).map(PositiveLong.tryCreate)
  implicit val positiveLongArb: Arbitrary[PositiveLong] = Arbitrary(positiveLongGen)
}
