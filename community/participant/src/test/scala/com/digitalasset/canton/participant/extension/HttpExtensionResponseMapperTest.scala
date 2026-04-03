// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.extension.HttpExtensionResponseMapper.FailureProfile
import org.scalatest.wordspec.AnyWordSpec

class HttpExtensionResponseMapperTest extends AnyWordSpec with BaseTest {

  private val mapper = new HttpExtensionResponseMapper

  private def response(
      statusCode: Int,
      body: String,
      headers: Map[String, Seq[String]] = Map.empty,
  ): HttpExtensionClientResponse =
    HttpExtensionClientResponse(statusCode, body, headers)

  "HttpExtensionResponseMapper" should {

    "use resource-specific defaults for resource request failures" in {
      val forbidden =
        mapper.mapHttpFailure(response(403, "forbidden"), "req-1", FailureProfile.Resource)
      forbidden shouldBe ExtensionCallError(
        statusCode = 403,
        message = "Forbidden - insufficient permissions: forbidden",
        requestId = Some("req-1"),
      )

      val notFound = mapper.mapResponse(response(404, "missing"), "req-2").left.value
      notFound shouldBe ExtensionCallError(
        statusCode = 404,
        message = "Function not found: missing",
        requestId = Some("req-2"),
      )
    }

    "use token-endpoint-specific defaults for OAuth token failures" in {
      val forbidden = mapper.mapHttpFailure(
        response(403, "forbidden"),
        "req-1",
        FailureProfile.OAuthTokenEndpoint,
      )
      forbidden shouldBe ExtensionCallError(
        statusCode = 403,
        message = "Forbidden: forbidden",
        requestId = Some("req-1"),
      )

      val notFound = mapper.mapHttpFailure(
        response(404, "missing-token-endpoint"),
        "req-2",
        FailureProfile.OAuthTokenEndpoint,
      )
      notFound shouldBe ExtensionCallError(
        statusCode = 404,
        message = "Not found: missing-token-endpoint",
        requestId = Some("req-2"),
      )
    }

    "parse Retry-After case-insensitively for retryable failures" in {
      val failure = mapper.mapHttpFailure(
        response(503, "issuer-down", Map("retry-after" -> Seq("7"))),
        "req-1",
        FailureProfile.OAuthTokenEndpoint,
      )

      failure shouldBe ExtensionCallErrorWithRetry(
        statusCode = 503,
        message = "Service unavailable: issuer-down",
        requestId = Some("req-1"),
        retryAfterSeconds = Some(7),
      )
    }

    "suppress oversized response bodies in rendered messages and fallback text" in {
      val oversizedBody = "x" * 500

      val failure = mapper.mapHttpFailure(
        response(400, oversizedBody),
        "req-1",
        FailureProfile.OAuthTokenEndpoint,
      )
      failure shouldBe ExtensionCallError(
        statusCode = 400,
        message = "Bad Request",
        requestId = Some("req-1"),
      )

      mapper.responseBodyOrDefault(response(401, oversizedBody), "Unauthorized") shouldBe
        "Unauthorized"
    }
  }
}
