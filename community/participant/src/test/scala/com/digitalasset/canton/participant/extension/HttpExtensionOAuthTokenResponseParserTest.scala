// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import io.circe.Json
import org.scalatest.wordspec.AnyWordSpec

class HttpExtensionOAuthTokenResponseParserTest extends AnyWordSpec with BaseTest {

  private def response(body: String): HttpExtensionClientResponse =
    HttpExtensionClientResponse(statusCode = 200, body = body, headers = Map.empty)

  private def tokenResponse(
      accessToken: Json = Json.fromString("opaque.access.token"),
      tokenType: Json = Json.fromString("Bearer"),
      expiresIn: Json = Json.fromLong(120L),
  ): String =
    Json.obj(
      "access_token" -> accessToken,
      "token_type" -> tokenType,
      "expires_in" -> expiresIn,
    ).noSpaces

  "HttpExtensionOAuthTokenResponseParser" should {

    "accept a valid token response and compute local expiry from expires_in" in {
      val parser = new HttpExtensionOAuthTokenResponseParser

      val result = parser.parse(
        response(tokenResponse()),
        requestId = "req-1",
        nowMillis = 1000L,
      )

      result shouldBe Right(
        HttpExtensionOAuthAccessToken(
          value = "opaque.access.token",
          expiresAtMillis = 121000L,
        )
      )
    }

    "accept token_type = Bearer case-insensitively" in {
      val parser = new HttpExtensionOAuthTokenResponseParser

      val result = parser.parse(
        response(tokenResponse(tokenType = Json.fromString("bEaReR"))),
        requestId = "req-1",
        nowMillis = 1000L,
      )

      result.valueOrFail("parse valid mixed-case Bearer token response").value shouldBe "opaque.access.token"
    }

    "treat access tokens as opaque bearer tokens without local claim parsing or verification" in {
      val parser = new HttpExtensionOAuthTokenResponseParser
      val jwtLikeButUnverified = "header.payload.invalid-signature"

      val result = parser.parse(
        response(tokenResponse(accessToken = Json.fromString(jwtLikeButUnverified))),
        requestId = "req-1",
        nowMillis = 1000L,
      )

      result shouldBe Right(
        HttpExtensionOAuthAccessToken(
          value = jwtLikeButUnverified,
          expiresAtMillis = 121000L,
        )
      )
    }

    "reject token responses with missing or malformed required fields as malformed 502 errors" in {
      val parser = new HttpExtensionOAuthTokenResponseParser

      val invalidBodies = Seq(
        "missing access_token" -> Json.obj(
          "token_type" -> Json.fromString("Bearer"),
          "expires_in" -> Json.fromLong(120L),
        ).noSpaces,
        "missing token_type" -> Json.obj(
          "access_token" -> Json.fromString("opaque.access.token"),
          "expires_in" -> Json.fromLong(120L),
        ).noSpaces,
        "missing expires_in" -> Json.obj(
          "access_token" -> Json.fromString("opaque.access.token"),
          "token_type" -> Json.fromString("Bearer"),
        ).noSpaces,
        "malformed expires_in" -> tokenResponse(expiresIn = Json.fromString("120")),
        "non bearer token_type" -> tokenResponse(tokenType = Json.fromString("MAC")),
        "invalid json" -> """{"access_token": "opaque.access.token"""",
      )

      invalidBodies.foreach { case (label, body) =>
        withClue(label) {
          val result = parser.parse(response(body), requestId = "req-1", nowMillis = 1000L)
          result.isLeft shouldBe true
          val error = result.left.value
          error.statusCode shouldBe 502
          error.message should startWith("Malformed OAuth token response")
          error.requestId shouldBe Some("req-1")
        }
      }
    }
  }
}
