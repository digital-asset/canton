// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.time.{GeneratorsTime, NonNegativeFiniteDuration}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class RandomDelayPropertyTest
    extends AnyPropSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with Inside {

  property("random delay within the expected limit") {
    forAll(GeneratorsTime.positiveSecondsArb.arbitrary) { reconciliationInterval =>
      val delay = AcsCommitmentSender.randomDelay(reconciliationInterval, 0.1, 0.5)

      delay should be >= NonNegativeFiniteDuration.tryOfMicros(
        reconciliationInterval.toFiniteDuration.toMicros / 10
      )
      delay should be <= NonNegativeFiniteDuration.tryOfMicros(
        reconciliationInterval.toFiniteDuration.toMicros / 2
      )
    }
  }
}
