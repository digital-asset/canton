// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.wordspec.AnyWordSpec

final class SequencerConnectionPoolDelaysTest extends AnyWordSpec with BaseTest {
  "SequencerConnectionPoolDelays" should {
    "use the default minimum stable duration if absent (data continuity)" in {
      val delays = SequencerConnectionPoolDelays.default.copy(
        minStableDuration = NonNegativeFiniteDuration.tryOfSeconds(5)
      )
      val proto = delays.toProtoV30.copy(minStableDuration = None)

      SequencerConnectionPoolDelays.fromProtoV30(proto).value shouldBe
        SequencerConnectionPoolDelays.default
    }
  }
}
