// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EngineConfigTest extends AnyWordSpec with Matchers {

  private val allowedLanguageVersions = LanguageVersion.allLfVersionsRange

  "EngineConfig.getInterpreterCostModel" should {
    "keep external-call charging disabled when gas accounting is off" in {
      val config = EngineConfig(allowedLanguageVersions = allowedLanguageVersions)

      config.getInterpreterCostModel.BExternalCall.cost("fun") shouldBe 0L
    }

    "use the default external-call base cost when gas accounting is enabled" in {
      val config = EngineConfig(
        allowedLanguageVersions = allowedLanguageVersions,
        gasBudget = Some(1000L),
      )

      config.getInterpreterCostModel.BExternalCall.cost("fun") shouldBe 103L
    }

    "use the configured external-call base cost when gas accounting is enabled" in {
      val config = EngineConfig(
        allowedLanguageVersions = allowedLanguageVersions,
        gasBudget = Some(1000L),
        externalCallBaseCost = 250L,
      )

      config.getInterpreterCostModel.BExternalCall.cost("fun") shouldBe 253L
    }
  }
}
