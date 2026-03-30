// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.BaseTest
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.ConfigSource

import java.net.URI
import java.nio.file.Paths

class ExtensionServiceConfigOAuthTest extends AnyWordSpec with BaseTest {

  private def load(configText: String) =
    ConfigSource.fromConfig(ConfigFactory.parseString(configText)).load[ExtensionServiceConfig]

  private def renderExtensionConfig(
      authBlock: String,
      useTls: Boolean = true,
      topLevelTrustCollection: Option[String] = Some("/etc/canton/ext-ca.pem"),
      requestIdHeader: String = "X-Request-Id",
      declaredFunctionsBlock: String = "",
  ): String = {
    val trustCollectionLine = topLevelTrustCollection.fold("")(path =>
      s"""trust-collection-file = "$path""""
    )

    s"""
       |name = "test-ext"
       |host = "ext.example.internal"
       |port = 443
       |use-tls = $useTls
       |$trustCollectionLine
       |auth = $authBlock
       |connect-timeout = 500ms
       |request-timeout = 8s
       |max-total-timeout = 25s
       |max-retries = 3
       |retry-initial-delay = 1s
       |retry-max-delay = 10s
       |request-id-header = "$requestIdHeader"
       |$declaredFunctionsBlock
       |""".stripMargin
  }

  private val oauthBlockWithOptionalFields =
    """{
      |  type = oauth
      |  token-endpoint = {
      |    host = "issuer.example.internal"
      |    port = 443
      |    path = "/oauth2/token"
      |    trust-collection-file = "/etc/canton/issuer-ca.pem"
      |    tls-insecure = true
      |  }
      |  client-id = "participant1"
      |  private-key-file = "/etc/canton/oauth-client-key.der"
      |  key-id = "participant1-key"
      |  scope = "external.call.invoke"
      |}""".stripMargin

  "ExtensionServiceConfig OAuth parsing" should {

    "parse auth.type = none while preserving top-level resource fields" in {
      val result = load(
        renderExtensionConfig(
          authBlock = """{ type = none }""",
          useTls = false,
          topLevelTrustCollection = Some("/etc/canton/resource-ca.pem"),
          requestIdHeader = "X-Correlation-Id",
        )
      )

      result match {
        case Right(config) =>
          config.name shouldBe "test-ext"
          config.host shouldBe "ext.example.internal"
          config.useTls shouldBe false
          config.trustCollectionFile shouldBe Some(Paths.get("/etc/canton/resource-ca.pem"))
          config.requestIdHeader shouldBe "X-Correlation-Id"
          config.auth shouldBe ExtensionServiceAuthConfig.NoAuth
          config.declaredFunctions shouldBe Seq.empty
        case Left(error) =>
          fail(error.toString)
      }
    }

    "parse auth.type = oauth with required and optional fields present" in {
      val result = load(
        renderExtensionConfig(
          authBlock = oauthBlockWithOptionalFields,
          declaredFunctionsBlock =
            """declared-functions = [
              |  {
              |    function-id = "echo"
              |    config-hash = "cafebabe"
              |  }
              |]""".stripMargin,
        )
      )

      result match {
        case Right(config) =>
          config.trustCollectionFile shouldBe Some(Paths.get("/etc/canton/ext-ca.pem"))
          config.declaredFunctions.map(_.functionId) shouldBe Seq("echo")
          config.declaredFunctions.map(_.configHash) shouldBe Seq("cafebabe")

          config.auth match {
            case oauth: ExtensionServiceAuthConfig.OAuth =>
              oauth.clientId shouldBe "participant1"
              oauth.privateKeyFile shouldBe Paths.get("/etc/canton/oauth-client-key.der")
              oauth.keyId shouldBe Some("participant1-key")
              oauth.scope shouldBe Some("external.call.invoke")
              oauth.tokenEndpoint.host shouldBe "issuer.example.internal"
              oauth.tokenEndpoint.trustCollectionFile shouldBe Some(
                Paths.get("/etc/canton/issuer-ca.pem")
              )
              oauth.tokenEndpoint.tlsInsecure shouldBe true
              oauth.tokenEndpoint.uri shouldBe URI.create(
                "https://issuer.example.internal:443/oauth2/token"
              )
            case other =>
              fail(s"Expected OAuth config but got $other")
          }
        case Left(error) =>
          fail(error.toString)
      }
    }

    "reject auth blocks without type" in {
      load(renderExtensionConfig(authBlock = "{}")).isLeft shouldBe true
    }

    "reject unknown auth types" in {
      load(renderExtensionConfig(authBlock = """{ type = unsupported }""")).isLeft shouldBe true
    }

    "reject oauth configs with missing required fields" in {
      val invalidConfigs = Seq(
        "missing client-id" ->
          renderExtensionConfig(
            authBlock =
              """{
                |  type = oauth
                |  token-endpoint = {
                |    host = "issuer.example.internal"
                |    port = 443
                |    path = "/oauth2/token"
                |  }
                |  private-key-file = "/etc/canton/oauth-client-key.der"
                |}""".stripMargin
          ),
        "missing private-key-file" ->
          renderExtensionConfig(
            authBlock =
              """{
                |  type = oauth
                |  token-endpoint = {
                |    host = "issuer.example.internal"
                |    port = 443
                |    path = "/oauth2/token"
                |  }
                |  client-id = "participant1"
                |}""".stripMargin
          ),
        "missing token-endpoint.host" ->
          renderExtensionConfig(
            authBlock =
              """{
                |  type = oauth
                |  token-endpoint = {
                |    port = 443
                |    path = "/oauth2/token"
                |  }
                |  client-id = "participant1"
                |  private-key-file = "/etc/canton/oauth-client-key.der"
                |}""".stripMargin
          ),
        "missing token-endpoint.port" ->
          renderExtensionConfig(
            authBlock =
              """{
                |  type = oauth
                |  token-endpoint = {
                |    host = "issuer.example.internal"
                |    path = "/oauth2/token"
                |  }
                |  client-id = "participant1"
                |  private-key-file = "/etc/canton/oauth-client-key.der"
                |}""".stripMargin
          ),
        "missing token-endpoint.path" ->
          renderExtensionConfig(
            authBlock =
              """{
                |  type = oauth
                |  token-endpoint = {
                |    host = "issuer.example.internal"
                |    port = 443
                |  }
                |  client-id = "participant1"
                |  private-key-file = "/etc/canton/oauth-client-key.der"
                |}""".stripMargin
          ),
      )

      invalidConfigs.foreach { case (clueText, configText) =>
        withClue(clueText) {
          load(configText).isLeft shouldBe true
        }
      }
    }

    "reject invalid token-endpoint paths" in {
      val invalidPaths = Seq(
        "oauth2/token" -> "Token endpoint path must start with '/'.",
        "/oauth2/token?scope=bad" -> "Token endpoint path must not contain a query string.",
        "/oauth2/token#fragment" -> "Token endpoint path must not contain a fragment.",
      )

      invalidPaths.foreach { case (path, expectedMessage) =>
        val result = load(
          renderExtensionConfig(
            authBlock =
              s"""{
                 |  type = oauth
                 |  token-endpoint = {
                 |    host = "issuer.example.internal"
                 |    port = 443
                 |    path = "$path"
                 |  }
                 |  client-id = "participant1"
                 |  private-key-file = "/etc/canton/oauth-client-key.der"
                 |}""".stripMargin
          )
        )

        withClue(path) {
          result.left.toOption.map(_.toString).getOrElse("") should include(expectedMessage)
        }
      }
    }

    "reject oauth configs when the resource server does not use TLS" in {
      val result = load(
        renderExtensionConfig(
          authBlock = oauthBlockWithOptionalFields,
          useTls = false,
        )
      )

      result.left.toOption.map(_.toString).getOrElse("") should include(
        "OAuth requires the resource server to use TLS."
      )
    }

    "omit optional key-id and scope when they are absent" in {
      val result = load(
        renderExtensionConfig(
          authBlock =
            """{
              |  type = oauth
              |  token-endpoint = {
              |    host = "issuer.example.internal"
              |    port = 443
              |    path = "/oauth2/token"
              |  }
              |  client-id = "participant1"
              |  private-key-file = "/etc/canton/oauth-client-key.der"
              |}""".stripMargin,
          topLevelTrustCollection = None,
        )
      )

      result match {
        case Right(config) =>
          config.trustCollectionFile shouldBe None
          config.auth match {
            case oauth: ExtensionServiceAuthConfig.OAuth =>
              oauth.keyId shouldBe None
              oauth.scope shouldBe None
              oauth.tokenEndpoint.trustCollectionFile shouldBe None
              oauth.tokenEndpoint.tlsInsecure shouldBe false
            case other =>
              fail(s"Expected OAuth config but got $other")
          }
        case Left(error) =>
          fail(error.toString)
      }
    }
  }
}
