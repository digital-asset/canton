// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import org.scalacheck.Arbitrary
import org.scalatest.EitherValues

import java.time.Duration

object GeneratorsTime extends EitherValues {
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val nonNegativeSecondsArb: Arbitrary[NonNegativeSeconds] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i => NonNegativeSeconds.tryOfSeconds(i.unwrap))
  )

  implicit val nonNegativeFiniteDurationArb: Arbitrary[NonNegativeFiniteDuration] = Arbitrary(
    nonNegativeLongArb.arbitrary.map(i =>
      NonNegativeFiniteDuration.create(Duration.ofNanos(i.unwrap)).value
    )
  )

  implicit val positiveSecondsArb: Arbitrary[PositiveSeconds] = Arbitrary(
    positiveLongArb.arbitrary.map(i => PositiveSeconds.tryOfSeconds(i.unwrap))
  )
}
