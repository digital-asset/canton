// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EndpointsTest extends AnyWordSpec with Matchers with Inside {

  "extractWsJwtToken" should {

    "extract the token from RFC 6455 comma-space separated subprotocols" in {
      Endpoints.extractWsJwtToken(Some("daml.ws.auth, jwt.token.abc123")) shouldBe Some(
        Endpoints.Jwt("abc123")
      )
    }

    "extract the token when there is no space after the comma" in {
      Endpoints.extractWsJwtToken(Some("daml.ws.auth,jwt.token.abc123")) shouldBe Some(
        Endpoints.Jwt("abc123")
      )
    }

    "extract the token regardless of extra surrounding whitespace" in {
      Endpoints.extractWsJwtToken(Some("daml.ws.auth ,  jwt.token.abc123  ")) shouldBe Some(
        Endpoints.Jwt("abc123")
      )
    }

    "extract the token when it appears before the daml.ws.auth marker" in {
      Endpoints.extractWsJwtToken(Some("jwt.token.abc123, daml.ws.auth")) shouldBe Some(
        Endpoints.Jwt("abc123")
      )
    }

    "return None when no jwt.token. prefixed value is present" in {
      Endpoints.extractWsJwtToken(Some("daml.ws.auth")) shouldBe None
    }

    "return None when the header is absent" in {
      Endpoints.extractWsJwtToken(None) shouldBe None
    }

    "return None for an empty header value" in {
      Endpoints.extractWsJwtToken(Some("")) shouldBe None
    }

    "take the first matching token when multiple are present" in {
      Endpoints.extractWsJwtToken(
        Some("daml.ws.auth, jwt.token.first, jwt.token.second")
      ) shouldBe Some(Endpoints.Jwt("first"))
    }
  }

  "checkCookieAuthOrigin" should {
    import Endpoints.CookieAuthDecision.*
    val allowed = Seq("https://app.example.com", "http://localhost:3000/")

    "be disabled when no origins are allowed" in {
      Endpoints.checkCookieAuthOrigin(Seq.empty, Some("https://app.example.com")) shouldBe Disabled
      Endpoints.checkCookieAuthOrigin(
        Seq(" ", ""),
        Some("https://app.example.com"),
      ) shouldBe Disabled
    }

    "allow an exactly matching origin" in {
      Endpoints.checkCookieAuthOrigin(allowed, Some("https://app.example.com")) shouldBe Allowed
    }

    "compare origins case-insensitively and ignore a trailing slash" in {
      Endpoints.checkCookieAuthOrigin(allowed, Some("HTTPS://App.Example.com/")) shouldBe Allowed
      Endpoints.checkCookieAuthOrigin(allowed, Some("http://localhost:3000")) shouldBe Allowed
    }

    "reject an origin that is not allowed" in {
      inside(Endpoints.checkCookieAuthOrigin(allowed, Some("https://evil.example.com"))) {
        case Rejected(reason) => reason should include("evil.example.com")
      }
    }

    "reject a different port or scheme" in {
      Endpoints.checkCookieAuthOrigin(allowed, Some("https://app.example.com:8443")) shouldBe a[
        Rejected
      ]
      Endpoints.checkCookieAuthOrigin(allowed, Some("http://app.example.com")) shouldBe a[Rejected]
    }

    "reject a missing Origin header" in {
      Endpoints.checkCookieAuthOrigin(allowed, None) shouldBe a[Rejected]
      Endpoints.checkCookieAuthOrigin(allowed, Some("")) shouldBe a[Rejected]
    }

    "reject the opaque null origin, even when any origin is allowed" in {
      Endpoints.checkCookieAuthOrigin(allowed, Some("null")) shouldBe a[Rejected]
      Endpoints.checkCookieAuthOrigin(Seq("*"), Some("null")) shouldBe a[Rejected]
    }

    "allow any origin with a wildcard entry" in {
      Endpoints.checkCookieAuthOrigin(
        Seq("*"),
        Some("https://anything.example.com"),
      ) shouldBe Allowed
    }
  }
}
