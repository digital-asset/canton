// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.canton.config.DeprecatedApiConfig
import com.digitalasset.canton.ledger.api.DeprecatedApiGate.GatedFutureOps
import com.digitalasset.canton.version.ApiDeprecation.Canton37
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class DeprecatedApiGateTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val api = "TestService.DeprecatedMethod"

  "gated" should {
    "run the body by default" in {
      Future
        .successful(42)
        .gated(DeprecatedApiGate(DeprecatedApiConfig()), Canton37.Endpoints, api)
        .futureValue shouldBe 42
    }

    "reject without running the body when the flag is off" in {
      val gate = DeprecatedApiGate(DeprecatedApiConfig(enableDeprecatedEndpoints37 = false))

      val error = (fail("must not run"): Future[Int])
        .gated(gate, Canton37.Endpoints, api)
        .failed
        .futureValue

      inside(error) { case e: StatusRuntimeException =>
        e.getStatus.getCode shouldBe Status.Code.FAILED_PRECONDITION
        val description = e.getStatus.getDescription
        description should include("DEPRECATED_API_DISABLED")
        description should include(api)
        description should include(Canton37.Endpoints.featureFlag)
      }
    }
  }
}
