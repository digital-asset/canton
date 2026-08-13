// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EndpointsTest extends AnyWordSpec with Matchers {

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
}
