// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues

import java.time.Duration

object GeneratorsTime extends EitherValues {
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val nonNegativeSecondsArb: Arbitrary[NonNegativeSeconds] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i => NonNegativeSeconds.tryOfSeconds(i.unwrap))
  )

  val nonNegativeFiniteDurationGen: Gen[NonNegativeFiniteDuration] =
    nonNegativeLongGen.map(i => NonNegativeFiniteDuration.create(Duration.ofNanos(i.unwrap)).value)
  implicit val nonNegativeFiniteDurationArb: Arbitrary[NonNegativeFiniteDuration] = Arbitrary(
    nonNegativeFiniteDurationGen
  )

  val positiveSecondsGen: Gen[PositiveSeconds] =
    positiveLongGen.map(i => PositiveSeconds.tryOfSeconds(i.unwrap))

  implicit val positiveSecondsArb: Arbitrary[PositiveSeconds] = Arbitrary(
    positiveLongArb.arbitrary.map(i => PositiveSeconds.tryOfSeconds(i.unwrap))
  )
}
