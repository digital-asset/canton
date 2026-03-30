// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.daml.jwt.{DecodedJwt, JwtSigner, KeyUtils}
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import io.circe.Json

import java.time.Instant
import java.util.UUID

private[extension] final class HttpExtensionOAuthClientAssertionFactory(
    config: ExtensionServiceConfig,
    nowMillis: () => Long = () => System.currentTimeMillis(),
    newJti: () => String = () => UUID.randomUUID().toString,
) {

  private val oauthConfig = config.auth match {
    case oauth: ExtensionServiceAuthConfig.OAuth => oauth
    case other =>
      throw new IllegalArgumentException(
        s"OAuth client assertion factory requires auth.type = oauth, but got $other"
      )
  }

  private lazy val signingKey =
    KeyUtils
      .readRSAPrivateKeyFromDer(oauthConfig.privateKeyFile.toFile)
      .fold(
        error =>
          throw new IllegalStateException(
            s"Failed to load RSA private key from DER file ${oauthConfig.privateKeyFile}: ${error.getMessage}",
            error,
          ),
        identity,
      )

  def buildClientAssertion(): String = {
    val issuedAt = Instant.ofEpochMilli(nowMillis())
    val headerFields = Seq(
      "alg" -> Json.fromString("RS256"),
      "typ" -> Json.fromString("JWT"),
    ) ++ oauthConfig.keyId.toList.map("kid" -> Json.fromString(_))

    val payload = Json.obj(
      "iss" -> Json.fromString(oauthConfig.clientId),
      "sub" -> Json.fromString(oauthConfig.clientId),
      "aud" -> Json.fromString(oauthConfig.tokenEndpoint.uri.toString),
      "iat" -> Json.fromLong(issuedAt.getEpochSecond),
      "exp" -> Json.fromLong(issuedAt.plusSeconds(30).getEpochSecond),
      "jti" -> Json.fromString(newJti()),
    )

    JwtSigner.RSA256
      .sign(
        DecodedJwt(
          header = Json.obj(headerFields*).noSpaces,
          payload = payload.noSpaces,
        ),
        signingKey,
      )
      .fold(
        error =>
          throw new IllegalStateException(
            s"Failed to sign OAuth client assertion: ${error.prettyPrint}"
          ),
        _.value,
      )
  }
}
