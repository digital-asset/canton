// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
  ExtensionServiceTokenEndpointConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Json
import org.scalatest.wordspec.AsyncWordSpec

import java.net.URI
import java.nio.file.Paths
import scala.collection.mutable

class HttpExtensionServiceClientOAuthTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private def makeConfig(
      name: String = "test-ext",
      port: Int = 9443,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "resource.example.internal",
      port = Port.tryCreate(port),
      useTls = true,
      auth = ExtensionServiceAuthConfig.OAuth(
        tokenEndpoint = ExtensionServiceTokenEndpointConfig(
          host = "issuer.example.internal",
          port = Port.tryCreate(443),
          path = "/oauth2/token",
        ),
        clientId = "participant1",
        privateKeyFile = Paths.get("/definitely/missing-test-key.der"),
        scope = Some("external.call.invoke"),
      ),
      connectTimeout = NonNegativeFiniteDuration.ofMillis(500),
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxTotalTimeout = NonNegativeFiniteDuration.ofSeconds(25),
      maxRetries = NonNegativeInt.tryCreate(2),
      retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1000),
      retryMaxDelay = NonNegativeFiniteDuration.ofSeconds(10),
    )

  private def tokenResponse(
      accessToken: String,
      expiresIn: Long,
  ): String =
    Json.obj(
      "access_token" -> Json.fromString(accessToken),
      "token_type" -> Json.fromString("Bearer"),
      "expires_in" -> Json.fromLong(expiresIn),
    ).noSpaces

  private def response(
      statusCode: Int,
      body: String,
      headers: Map[String, Seq[String]] = Map.empty,
  ): HttpExtensionClientResponse =
    HttpExtensionClientResponse(statusCode, body, headers)

  private final class FakeRuntime(
      initialNowMillis: Long = 1000L,
      requestIds: Seq[String] = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6"),
      jitter: Double = 0.0,
  ) extends HttpExtensionClientRuntime {
    private val ids = mutable.Queue.from(requestIds)

    val sleptMillis: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty

    private var now: Long = initialNowMillis

    override def nowMillis(): Long = now

    def advanceMillis(ms: Long): Unit = now += ms

    override def sleepMillis(ms: Long): Unit = {
      sleptMillis += ms
      advanceMillis(ms)
    }

    override def newRequestId(): String =
      if (ids.nonEmpty) ids.dequeue() else fail("No request IDs left in fake runtime")

    override def nextRetryJitterDouble(): Double = jitter
  }

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
      } else {
        fail("No transport outcomes left to dequeue")
      }
    }
  }

  private final class FakeResourcesFactory(
      resourceTransport: FakeTransport,
      tokenTransport: FakeTransport,
  ) extends HttpExtensionClientResourcesFactory {
    val createCalls: mutable.ArrayBuffer[ExtensionServiceConfig] = mutable.ArrayBuffer.empty

    override def create(config: ExtensionServiceConfig): HttpExtensionClientResources = {
      createCalls += config
      HttpExtensionClientResources(
        resourceTransport = resourceTransport,
        tokenTransport = Some(tokenTransport),
      )
    }
  }

  private def makeClient(
      resourcesFactory: HttpExtensionClientResourcesFactory,
      runtime: HttpExtensionClientRuntime,
      config: ExtensionServiceConfig = makeConfig(),
      buildClientAssertion: () => String = () => "signed.jwt.value",
  ): HttpExtensionServiceClient =
    new HttpExtensionServiceClient(
      extensionId = config.name,
      config = config,
      resourcesFactory = resourcesFactory,
      runtime = runtime,
      requestBuilder = new HttpExtensionRequestBuilder(config),
      responseMapper = new HttpExtensionResponseMapper,
      oauthAssertionFactory = Some(() => buildClientAssertion()),
      loggerFactory = loggerFactory,
    )

  "HttpExtensionServiceClient OAuth behavior" should {

    "acquire a token on the first business request and send it as a bearer token" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok")
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 1

        val tokenRequest = tokenTransport.requests.head
        tokenRequest.uri shouldBe URI.create("https://issuer.example.internal:443/oauth2/token")
        tokenRequest.headers should contain("X-Request-Id" -> "req-1")

        val resourceRequest = resourceTransport.requests.head
        resourceRequest.uri shouldBe URI.create("https://resource.example.internal:9443/api/v1/external-call")
        resourceRequest.headers should contain("X-Request-Id" -> "req-2")
        resourceRequest.headers should contain("Authorization" -> "Bearer token-1")
      }
    }

    "reuse an unexpired cached token on later business requests for the same extension" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, "ok-1")),
          Right(response(200, "ok-2")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      for {
        result1 <- client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        result2 <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
      } yield {
        result1 shouldBe Right("ok-1")
        result2 shouldBe Right("ok-2")
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 2
        all(resourceTransport.requests.map(_.headers.toMap)) should contain(
          "Authorization" -> "Bearer token-1"
        )
      }
    }

    "reacquire a token after local expiry without proactive or background refresh work" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 1L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, "ok-1")),
          Right(response(200, "ok-2")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      tokenTransport.requests shouldBe empty
      runtime.advanceMillis(5000L)
      tokenTransport.requests shouldBe empty

      for {
        result1 <- client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        _ = tokenTransport.requests should have size 1
        _ = runtime.advanceMillis(5000L)
        _ = tokenTransport.requests should have size 1
        result2 <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
      } yield {
        result1 shouldBe Right("ok-1")
        result2 shouldBe Right("ok-2")
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 2
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
        )
      }
    }
  }
}
