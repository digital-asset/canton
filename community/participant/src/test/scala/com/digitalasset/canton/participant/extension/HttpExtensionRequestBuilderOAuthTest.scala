// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import org.scalatest.wordspec.AnyWordSpec

import java.net.URI
import java.time.Duration

class HttpExtensionRequestBuilderOAuthTest extends AnyWordSpec with BaseTest {

  private def makeConfig(
      requestIdHeader: String = "X-Request-Id",
      useTls: Boolean = false,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = "test-ext",
      host = "localhost",
      port = Port.tryCreate(8080),
      useTls = useTls,
      auth = ExtensionServiceAuthConfig.NoAuth,
      requestIdHeader = requestIdHeader,
    )

  "HttpExtensionRequestBuilder" should {

    "preserve the resource request protocol without Authorization for auth.type = none" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig())

      val request = builder.buildCallRequest(
        functionId = "echo",
        configHash = "cafebabe",
        input = "deadbeef",
        mode = "submission",
        timeout = Duration.ofSeconds(10),
        requestId = "req-1",
      )

      request.uri shouldBe URI.create("http://localhost:8080/api/v1/external-call")
      request.timeout shouldBe Duration.ofSeconds(10)
      request.headers should contain theSameElementsInOrderAs Seq(
        "Content-Type" -> "application/octet-stream",
        "X-Daml-External-Function-Id" -> "echo",
        "X-Daml-External-Config-Hash" -> "cafebabe",
        "X-Daml-External-Mode" -> "submission",
        "X-Request-Id" -> "req-1",
      )
      request.body shouldBe "deadbeef"
    }

    "attach Authorization when an OAuth bearer token is supplied" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig(useTls = true))

      val request = builder.buildCallRequest(
        functionId = "echo",
        configHash = "cafebabe",
        input = "deadbeef",
        mode = "submission",
        timeout = Duration.ofSeconds(10),
        requestId = "req-1",
        bearerToken = Some("access-token-1"),
      )

      request.uri shouldBe URI.create("https://localhost:8080/api/v1/external-call")
      request.headers should contain theSameElementsInOrderAs Seq(
        "Content-Type" -> "application/octet-stream",
        "X-Daml-External-Function-Id" -> "echo",
        "X-Daml-External-Config-Hash" -> "cafebabe",
        "X-Daml-External-Mode" -> "submission",
        "X-Request-Id" -> "req-1",
        "Authorization" -> "Bearer access-token-1",
      )
    }

    "forward validation mode unchanged on direct resource requests" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig())

      val request = builder.buildCallRequest(
        functionId = "echo",
        configHash = "cafebabe",
        input = "deadbeef",
        mode = "validation",
        timeout = Duration.ofSeconds(10),
        requestId = "req-1",
      )

      request.headers should contain("X-Daml-External-Mode" -> "validation")
    }

    "use the configured request id header name" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig(requestIdHeader = "X-Correlation-Id"))

      val request = builder.buildCallRequest(
        functionId = "echo",
        configHash = "cafebabe",
        input = "deadbeef",
        mode = "submission",
        timeout = Duration.ofSeconds(10),
        requestId = "req-1",
      )

      request.headers should contain("X-Correlation-Id" -> "req-1")
      request.headers.exists(_._1 == "X-Request-Id") shouldBe false
    }

    "build validation requests using the preserved protocol without Authorization" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig())

      val request = builder.buildValidationRequest(
        timeout = Duration.ofMillis(500),
        requestId = "req-1",
      )

      request.uri shouldBe URI.create("http://localhost:8080/api/v1/external-call")
      request.timeout shouldBe Duration.ofMillis(500)
      request.headers should contain theSameElementsInOrderAs Seq(
        "Content-Type" -> "application/octet-stream",
        "X-Daml-External-Function-Id" -> "_health",
        "X-Daml-External-Config-Hash" -> "",
        "X-Daml-External-Mode" -> "validation",
        "X-Request-Id" -> "req-1",
      )
      request.body shouldBe ""
    }

    "attach Authorization to validation requests when an OAuth bearer token is supplied" in {
      val builder = new HttpExtensionRequestBuilder(makeConfig(useTls = true))

      val request = builder.buildValidationRequest(
        timeout = Duration.ofMillis(500),
        requestId = "req-1",
        bearerToken = Some("access-token-1"),
      )

      request.uri shouldBe URI.create("https://localhost:8080/api/v1/external-call")
      request.headers should contain theSameElementsInOrderAs Seq(
        "Content-Type" -> "application/octet-stream",
        "X-Daml-External-Function-Id" -> "_health",
        "X-Daml-External-Config-Hash" -> "",
        "X-Daml-External-Mode" -> "validation",
        "X-Request-Id" -> "req-1",
        "Authorization" -> "Bearer access-token-1",
      )
      request.body shouldBe ""
    }
  }
}
