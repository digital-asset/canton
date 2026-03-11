// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Z3SynthesizerTest extends AnyWordSpec with Matchers {

  "Z3Synthesizer" should {

    "synthesize a value greater than a positive number" in {
      val bound = 42L
      val result = Z3Synthesizer.synthesizeGreaterThan(bound)
      result.fold(fail("expected Some")) { value =>
        value should be > bound
      }
    }
  }
}
