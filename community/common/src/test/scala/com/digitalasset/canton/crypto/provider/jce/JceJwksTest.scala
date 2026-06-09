// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.*
import io.circe.Json
import org.scalatest.wordspec.AsyncWordSpec

// The bulk of the tests for JceJwks are in JceCryptoTest.  This contains a few
// extra tests that don't exercise the main Crypto API.
class JceJwksTest extends AsyncWordSpec with BaseTest {

  "JceJwks" must {

    "match the example RSA JWK Thumbprint from RFC 7638" in {
      JceJwks.thumbprint(
        Map(
          "e" -> Json.fromString("AQAB"),
          "kty" -> Json.fromString("RSA"),
          "n" -> Json.fromString(
            "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
          ),
        )
      ) shouldBe "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"
    }

    "match the example EdDSA JWK Thumbprint from RFC 8037" in {
      JceJwks.thumbprint(
        Map(
          "crv" -> Json.fromString("Ed25519"),
          "kty" -> Json.fromString("OKP"),
          "x" -> Json.fromString("11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"),
        )
      ) shouldBe "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"
    }
  }
}
