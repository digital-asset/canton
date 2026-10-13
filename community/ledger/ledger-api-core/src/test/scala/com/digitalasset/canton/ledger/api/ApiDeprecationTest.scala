// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DeprecatedApiConfig
import com.digitalasset.canton.version.{ApiDeprecation, CantonMinorVersion}
import org.scalatest.wordspec.AnyWordSpec

class ApiDeprecationTest extends AnyWordSpec with BaseTest {

  private object RemovalUndecided
      extends ApiDeprecation(
        deprecatedIn = CantonMinorVersion(3, 8),
        disabledIn = CantonMinorVersion(3, 9),
        removedIn = None,
        featureFlag =
          "canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-38",
      ) {
    override def isEnabledBy(config: DeprecatedApiConfig): Boolean = false
  }

  "ApiDeprecation" should {
    "name the removing release when it is decided" in {
      ApiDeprecation.Canton35.Endpoints.disabledMessage("GET /v2/foo") shouldBe
        "GET /v2/foo was deprecated in Canton 3.5, is disabled by default since Canton 3.6 " +
        "and will be removed in Canton 3.7. " +
        "Set `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-35 = true` " +
        "to re-enable it temporarily."
      ApiDeprecation.Canton35.Endpoints.documentationNotice("POST /v2/bar") shouldBe
        "Deprecated since Canton 3.5: disabled by default since Canton 3.6 " +
        "(re-enable temporarily with " +
        "`canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-35 = true`) " +
        "and will be removed in Canton 3.7. Use POST /v2/bar instead."
    }

    "announce the removal in a future release otherwise" in {
      RemovalUndecided.disabledMessage("GET /v2/foo") shouldBe
        "GET /v2/foo was deprecated in Canton 3.8, is disabled by default since Canton 3.9 " +
        "and will be removed in a future Canton release. " +
        "Set `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-38 = true` " +
        "to re-enable it temporarily."
      RemovalUndecided.documentationNotice("POST /v2/bar") shouldBe
        "Deprecated since Canton 3.8: disabled by default since Canton 3.9 " +
        "(re-enable temporarily with " +
        "`canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-38 = true`) " +
        "and will be removed in a future Canton release. Use POST /v2/bar instead."
    }
  }

  "DeprecatedApiGate" should {
    "reject only the disabled deprecations" in {
      val gate = DeprecatedApiGate.enabling(ApiDeprecation.Canton35.Endpoints)

      gate.isEnabled(ApiDeprecation.Canton35.Endpoints) shouldBe true
      gate.rejection(ApiDeprecation.Canton35.Endpoints, "GET /v2/foo") shouldBe None

      gate.isEnabled(ApiDeprecation.Canton34.Parameters) shouldBe false
      gate.isEnabled(RemovalUndecided) shouldBe false
      gate.rejection(RemovalUndecided, "GET /v2/foo").map(_.cause) shouldBe Some(
        RemovalUndecided.disabledMessage("GET /v2/foo")
      )
    }
  }
}
