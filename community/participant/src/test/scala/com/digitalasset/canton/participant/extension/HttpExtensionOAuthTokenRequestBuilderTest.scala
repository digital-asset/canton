// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
  ExtensionServiceTokenEndpointConfig,
}
import org.scalatest.wordspec.AnyWordSpec

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.time.Duration

class HttpExtensionOAuthTokenRequestBuilderTest extends AnyWordSpec with BaseTest {

  private def makeConfig(
      requestIdHeader: String = "X-Request-Id",
      scope: Option[String] = Some("external.call.invoke profile"),
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = "test-ext",
      host = "resource.example.internal",
      port = Port.tryCreate(9443),
      useTls = true,
      requestIdHeader = requestIdHeader,
      auth = ExtensionServiceAuthConfig.OAuth(
        tokenEndpoint = ExtensionServiceTokenEndpointConfig(
          host = "issuer.example.internal",
          port = Port.tryCreate(443),
          path = "/oauth2/token",
        ),
        clientId = "participant1",
        privateKeyFile = Paths.get("/tmp/oauth-client-key.der"),
        scope = scope,
      ),
    )

  private def decodeForm(body: String): Map[String, String] =
    body
      .split("&")
      .toSeq
      .filter(_.nonEmpty)
      .map { pair =>
        val parts = pair.split("=", 2)
        val name = URLDecoder.decode(parts(0), StandardCharsets.UTF_8)
        val value = URLDecoder.decode(parts.lift(1).getOrElse(""), StandardCharsets.UTF_8)
        name -> value
      }
      .toMap

  "HttpExtensionOAuthTokenRequestBuilder" should {

    "target the configured token endpoint and include the required token request fields" in {
      val builder = new HttpExtensionOAuthTokenRequestBuilder(makeConfig())

      val request = builder.buildTokenRequest(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
        clientAssertion = "signed.jwt.value",
      )

      request.uri shouldBe URI.create("https://issuer.example.internal:443/oauth2/token")
      request.timeout shouldBe Duration.ofSeconds(3)
      request.headers should contain theSameElementsInOrderAs Seq(
        "Content-Type" -> "application/x-www-form-urlencoded",
        "X-Request-Id" -> "req-1",
      )

      decodeForm(request.body) shouldBe Map(
        "grant_type" -> "client_credentials",
        "client_assertion_type" -> "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion" -> "signed.jwt.value",
        "scope" -> "external.call.invoke profile",
      )
    }

    "use the configured request id header name on token requests" in {
      val builder =
        new HttpExtensionOAuthTokenRequestBuilder(makeConfig(requestIdHeader = "X-Correlation-Id"))

      val request = builder.buildTokenRequest(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
        clientAssertion = "signed.jwt.value",
      )

      request.headers should contain("X-Correlation-Id" -> "req-1")
      request.headers.exists(_._1 == "X-Request-Id") shouldBe false
    }

    "omit scope and audience when scope is not configured" in {
      val builder = new HttpExtensionOAuthTokenRequestBuilder(makeConfig(scope = None))

      val request = builder.buildTokenRequest(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
        clientAssertion = "signed.jwt.value",
      )

      val form = decodeForm(request.body)
      form shouldBe Map(
        "grant_type" -> "client_credentials",
        "client_assertion_type" -> "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion" -> "signed.jwt.value",
      )
      form.keySet should not contain "scope"
      form.keySet should not contain "audience"
    }
  }
}
