// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfInterpretationConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InterpretationConfigTest extends AnyWordSpec with Matchers {

  "InterpretationConfig.forProtocolVersion" should {

    "be defined for all supported protocol versions" in {
      ProtocolVersion.supported.foreach(pv =>
        noException should be thrownBy
          InterpretationConfig.forProtocolVersion(pv)
      )
    }

    "not map stable protocol versions to dev LF versions" in {
      ProtocolVersion.stable.foreach { pv =>
        val config = InterpretationConfig.forProtocolVersion(pv)
        config should not be LfInterpretationConfig.Dev
        config.allowedLanguageVersions.filter(_.isDevVersion) shouldBe empty
      }
    }

    // The interpretation config of a stable protocol version must never change.
    // If this test fails, do not update it: add a new constant for a new protocol version instead.
    "not change for stable protocol versions" in {
      InterpretationConfig
        .forProtocolVersion(ProtocolVersion.v36)
        .allowedLanguageVersions
        .map(_.pretty) shouldBe List("2.1", "2.2", "2.3", "2.4")
    }
  }
}
