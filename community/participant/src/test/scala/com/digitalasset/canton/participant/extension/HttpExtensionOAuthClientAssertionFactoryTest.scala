// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.daml.jwt.{Jwt, JwtDecoder, RSA256Verifier}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
  ExtensionServiceTokenEndpointConfig,
}
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Path}
import java.security.KeyPairGenerator
import java.security.interfaces.RSAPublicKey
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable

class HttpExtensionOAuthClientAssertionFactoryTest extends AnyWordSpec with BaseTest {

  private lazy val rsaKeyPair = {
    val generator = KeyPairGenerator.getInstance("RSA")
    generator.initialize(2048)
    generator.generateKeyPair()
  }

  private lazy val rsaPublicKey: RSAPublicKey =
    rsaKeyPair.getPublic.asInstanceOf[RSAPublicKey]

  private lazy val privateKeyFile: Path = {
    val file = Files.createTempFile("external-call-oauth-client", ".der")
    Files.write(file, rsaKeyPair.getPrivate.getEncoded)
    file.toFile.deleteOnExit()
    file
  }

  private def makeConfig(keyId: Option[String] = Some("participant1-key")): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = "test-ext",
      host = "resource.example.internal",
      port = Port.tryCreate(9443),
      useTls = true,
      auth = ExtensionServiceAuthConfig.OAuth(
        tokenEndpoint = ExtensionServiceTokenEndpointConfig(
          host = "issuer.example.internal",
          port = Port.tryCreate(443),
          path = "/oauth2/token",
        ),
        clientId = "participant1",
        privateKeyFile = privateKeyFile,
        keyId = keyId,
      ),
    )

  private def parseAssertionJson(assertion: String): (Json, Json) = {
    val decoded = JwtDecoder.decode(Jwt(assertion)).valueOrFail("decode JWT assertion")
    val header = parse(decoded.header).valueOrFail("parse JWT header")
    val payload = parse(decoded.payload).valueOrFail("parse JWT payload")
    header -> payload
  }

  "HttpExtensionOAuthClientAssertionFactory" should {

    "build a verifiable RS256 assertion with the required claims" in {
      val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
      val factory = new HttpExtensionOAuthClientAssertionFactory(
        config = makeConfig(),
        nowMillis = () => now.toEpochMilli,
        newJti = () => "jti-1",
      )

      val assertion = factory.buildClientAssertion()
      RSA256Verifier(rsaPublicKey).valueOrFail("create verifier").verify(Jwt(assertion)).valueOrFail(
        "verify assertion"
      )

      val (header, payload) = parseAssertionJson(assertion)
      val headerCursor = header.hcursor
      val payloadCursor = payload.hcursor

      headerCursor.get[String]("alg").valueOrFail("alg") shouldBe "RS256"
      headerCursor.get[String]("typ").valueOrFail("typ") shouldBe "JWT"
      headerCursor.get[String]("kid").valueOrFail("kid") shouldBe "participant1-key"

      payloadCursor.get[String]("iss").valueOrFail("iss") shouldBe "participant1"
      payloadCursor.get[String]("sub").valueOrFail("sub") shouldBe "participant1"
      payloadCursor.get[String]("aud").valueOrFail("aud") shouldBe "https://issuer.example.internal:443/oauth2/token"
      payloadCursor.get[Long]("iat").valueOrFail("iat") shouldBe now.getEpochSecond
      payloadCursor.get[Long]("exp").valueOrFail("exp") shouldBe now.plusSeconds(30).getEpochSecond
      payloadCursor.get[String]("jti").valueOrFail("jti") shouldBe "jti-1"
    }

    "omit kid when key-id is not configured" in {
      val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
      val factory = new HttpExtensionOAuthClientAssertionFactory(
        config = makeConfig(keyId = None),
        nowMillis = () => now.toEpochMilli,
        newJti = () => "jti-1",
      )

      val assertion = factory.buildClientAssertion()
      val (header, _) = parseAssertionJson(assertion)

      header.hcursor.get[Option[String]]("kid").valueOrFail("kid") shouldBe None
    }

    "produce one-use-only assertions across successive generations" in {
      val now = Instant.now().truncatedTo(ChronoUnit.SECONDS)
      val jtis = mutable.Queue("jti-1", "jti-2")
      val factory = new HttpExtensionOAuthClientAssertionFactory(
        config = makeConfig(),
        nowMillis = () => now.toEpochMilli,
        newJti = () => jtis.dequeue(),
      )

      val first = factory.buildClientAssertion()
      val second = factory.buildClientAssertion()

      first should not be second
      val (_, firstPayload) = parseAssertionJson(first)
      val (_, secondPayload) = parseAssertionJson(second)
      firstPayload.hcursor.get[String]("jti").valueOrFail("first jti") shouldBe "jti-1"
      secondPayload.hcursor.get[String]("jti").valueOrFail("second jti") shouldBe "jti-2"
    }
  }
}
