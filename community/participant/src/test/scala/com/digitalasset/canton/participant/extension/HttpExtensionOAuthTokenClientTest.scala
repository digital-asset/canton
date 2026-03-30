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
import io.circe.Json
import org.scalatest.wordspec.AnyWordSpec

import java.net.http.HttpTimeoutException
import java.nio.file.{Path, Paths}
import java.time.Duration
import scala.collection.mutable

class HttpExtensionOAuthTokenClientTest extends AnyWordSpec with BaseTest {

  private def makeConfig(
      privateKeyFile: Path = Paths.get("/tmp/oauth-client-key.der"),
      requestIdHeader: String = "X-Request-Id",
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
        privateKeyFile = privateKeyFile,
        scope = Some("external.call.invoke"),
      ),
    )

  private def tokenResponse(
      accessToken: String = "opaque.access.token",
      tokenType: String = "Bearer",
      expiresIn: Long = 120L,
  ): String =
    Json.obj(
      "access_token" -> Json.fromString(accessToken),
      "token_type" -> Json.fromString(tokenType),
      "expires_in" -> Json.fromLong(expiresIn),
    ).noSpaces

  private def response(
      statusCode: Int,
      body: String,
      headers: Map[String, Seq[String]] = Map.empty,
  ): HttpExtensionClientResponse =
    HttpExtensionClientResponse(statusCode, body, headers)

  private final class FakeTransport(
      outcomes: Seq[Either[Throwable, HttpExtensionClientResponse]]
  ) extends HttpExtensionClientTransport {
    private val queuedOutcomes = mutable.Queue.from(outcomes)

    val requests: mutable.ArrayBuffer[HttpExtensionClientRequest] = mutable.ArrayBuffer.empty

    override def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse = {
      requests += request
      if (queuedOutcomes.nonEmpty) {
        queuedOutcomes.dequeue() match {
          case Left(exception) => throw exception
          case Right(resp) => resp
        }
      } else fail("No transport outcomes left to dequeue")
    }
  }

  private def makeClient(
      transport: FakeTransport,
      config: ExtensionServiceConfig = makeConfig(),
      buildClientAssertion: () => String = () => "signed.jwt.value",
      nowMillis: () => Long = () => 1000L,
  ): HttpExtensionOAuthTokenClient =
    new HttpExtensionOAuthTokenClient(
      transport = transport,
      requestBuilder = new HttpExtensionOAuthTokenRequestBuilder(config),
      buildClientAssertion = buildClientAssertion,
      responseParser = new HttpExtensionOAuthTokenResponseParser,
      nowMillis = nowMillis,
    )

  "HttpExtensionOAuthTokenClient" should {

    "acquire a token successfully through the token transport" in {
      val transport = new FakeTransport(
        Seq(Right(response(200, tokenResponse())))
      )
      val client = makeClient(transport)

      val result = client.acquireToken(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
      )

      result shouldBe Right(
        HttpExtensionOAuthAccessToken(
          value = "opaque.access.token",
          expiresAtMillis = 121000L,
        )
      )
      transport.requests should have size 1
      val request = transport.requests.head
      request.timeout shouldBe Duration.ofSeconds(3)
      request.headers should contain("X-Request-Id" -> "req-1")
      request.body should include("client_assertion=signed.jwt.value")
    }

    "preserve exact token-endpoint HTTP status codes and request ids" in {
      val retryAfterHeaders = Map("Retry-After" -> Seq("7"))
      val failingResponses = Seq(
        400 -> response(400, "bad-request"),
        401 -> response(401, "unauthorized"),
        403 -> response(403, "forbidden"),
        404 -> response(404, "not-found"),
        408 -> response(408, "timed-out"),
        429 -> response(429, "rate-limited", retryAfterHeaders),
        500 -> response(500, "boom"),
        502 -> response(502, "bad-gateway"),
        503 -> response(503, "service-down", retryAfterHeaders),
        504 -> response(504, "gateway-timeout"),
      )

      failingResponses.foreach { case (statusCode, resp) =>
        withClue(s"statusCode=$statusCode") {
          val transport = new FakeTransport(Seq(Right(resp)))
          val client = makeClient(transport)

          val result = client.acquireToken(
            timeout = Duration.ofSeconds(3),
            requestId = "req-1",
          )

          result.isLeft shouldBe true
          val error = result.left.value
          error.statusCode shouldBe statusCode
          error.requestId shouldBe Some("req-1")

          statusCode match {
            case 429 | 503 =>
              error shouldBe ExtensionCallErrorWithRetry(
                statusCode = statusCode,
                message = error.message,
                requestId = Some("req-1"),
                retryAfterSeconds = Some(7),
              )
            case _ =>
              error shouldBe ExtensionCallError(
                statusCode = statusCode,
                message = error.message,
                requestId = Some("req-1"),
              )
          }
        }
      }
    }

    "map token-endpoint transport exceptions to the specified status codes" in {
      val failingExceptions = Seq[(Throwable, Int)](
        new HttpTimeoutException("timed-out") -> 408,
        new java.net.ConnectException("connection-refused") -> 503,
        new java.io.IOException("broken-pipe") -> 503,
        new RuntimeException("boom") -> 500,
      )

      failingExceptions.foreach { case (exception, expectedStatusCode) =>
        withClue(exception.getClass.getSimpleName) {
          val transport = new FakeTransport(Seq(Left(exception)))
          val client = makeClient(transport)

          val result = client.acquireToken(
            timeout = Duration.ofSeconds(3),
            requestId = "req-1",
          )

          result.left.value shouldBe ExtensionCallError(
            statusCode = expectedStatusCode,
            message = result.left.value.message,
            requestId = Some("req-1"),
          )
        }
      }
    }

    "map local signing failure to 500 with no outbound request id" in {
      val transport = new FakeTransport(Seq.empty)
      val client = makeClient(
        transport = transport,
        buildClientAssertion = () => throw new RuntimeException("signing failed"),
      )

      val result = client.acquireToken(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
      )

      result.left.value shouldBe ExtensionCallError(
        statusCode = 500,
        message = result.left.value.message,
        requestId = None,
      )
      transport.requests shouldBe empty
    }

    "map local key-loading failure to 500 with no outbound request id" in {
      val config = makeConfig(privateKeyFile = Paths.get("/definitely/missing-oauth-client-key.der"))
      val assertionFactory = new HttpExtensionOAuthClientAssertionFactory(
        config = config,
        nowMillis = () => 1000L,
        newJti = () => "jti-1",
      )
      val transport = new FakeTransport(Seq.empty)
      val client = makeClient(
        transport = transport,
        config = config,
        buildClientAssertion = () => assertionFactory.buildClientAssertion(),
      )

      val result = client.acquireToken(
        timeout = Duration.ofSeconds(3),
        requestId = "req-1",
      )

      result.left.value shouldBe ExtensionCallError(
        statusCode = 500,
        message = result.left.value.message,
        requestId = None,
      )
      transport.requests shouldBe empty
    }
  }
}
