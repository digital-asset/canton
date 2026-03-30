// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import java.net.URI
import java.net.http.HttpTimeoutException
import java.time.Duration
import scala.collection.mutable

class HttpExtensionServiceClientTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private def makeConfig(
      name: String = "test-ext",
      port: Int = 8080,
      jwt: Option[String] = None,
      maxRetries: Int = 2,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = "localhost",
      port = Port.tryCreate(port),
      useTls = false,
      jwt = jwt,
      connectTimeout = NonNegativeFiniteDuration.ofMillis(500),
      requestTimeout = NonNegativeFiniteDuration.ofSeconds(10),
      maxTotalTimeout = NonNegativeFiniteDuration.ofSeconds(25),
      maxRetries = NonNegativeInt.tryCreate(maxRetries),
      retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1000),
      retryMaxDelay = NonNegativeFiniteDuration.ofSeconds(10),
    )

  private def makeClient(
      resourcesFactory: HttpExtensionClientResourcesFactory,
      runtime: HttpExtensionClientRuntime,
      config: ExtensionServiceConfig = makeConfig(),
  ): HttpExtensionServiceClient =
    new HttpExtensionServiceClient(
      extensionId = config.name,
      config = config,
      resourcesFactory = resourcesFactory,
      runtime = runtime,
      loggerFactory = loggerFactory,
    )

  private def response(
      statusCode: Int,
      body: String,
      headers: Map[String, Seq[String]] = Map.empty,
  ): HttpExtensionClientResponse =
    HttpExtensionClientResponse(statusCode, body, headers)

  private final class FakeRuntime(
      initialNowMillis: Long = 1000L,
      requestIds: Seq[String] = Seq("req-1", "req-2", "req-3", "req-4"),
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
          case Right(response) => response
        }
      } else {
        fail("No transport outcomes left to dequeue")
      }
    }
  }

  private final class FakeResourcesFactory(transport: FakeTransport)
      extends HttpExtensionClientResourcesFactory {
    val createCalls: mutable.ArrayBuffer[ExtensionServiceConfig] = mutable.ArrayBuffer.empty

    override def create(config: ExtensionServiceConfig): HttpExtensionClientResources = {
      createCalls += config
      HttpExtensionClientResources(resourceTransport = transport)
    }
  }

  "HttpExtensionServiceClient" should {

    "return the response body for a 200 response" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(200, "response-body")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result shouldBe Right("response-body")
        }
    }

    "preserve the current request protocol when sending a resource request" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(
        resourcesFactory = resourcesFactory,
        runtime = runtime,
        config = makeConfig(jwt = Some("static-token")),
      )

      client
        .call("echo", "cafebabe", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result shouldBe Right("ok")

          transport.requests should have size 1
          val request = transport.requests.head
          request.uri shouldBe URI.create("http://localhost:8080/api/v1/external-call")
          request.timeout shouldBe Duration.ofSeconds(10)
          request.headers should contain theSameElementsInOrderAs Seq(
            "Content-Type" -> "application/octet-stream",
            "X-Daml-External-Function-Id" -> "echo",
            "X-Daml-External-Config-Hash" -> "cafebabe",
            "X-Daml-External-Mode" -> "submission",
            "X-Request-Id" -> "req-1",
            "Authorization" -> "Bearer static-token",
          )
          request.body shouldBe "deadbeef"
        }
    }

    "preserve 400 terminal error mapping" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(400, "bad-request")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a 400 error"))
          error.statusCode shouldBe 400
          error.message shouldBe "Bad Request: bad-request"
          error.requestId shouldBe Some("req-1")
        }
    }

    "preserve 401 terminal error mapping" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(401, "unauthorized")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a 401 error"))
          error.statusCode shouldBe 401
          error.message shouldBe "Unauthorized - check JWT token: unauthorized"
          error.requestId shouldBe Some("req-1")
        }
    }

    "preserve default terminal error message when the response body is empty" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(400, "")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a 400 error"))
          error.statusCode shouldBe 400
          error.message shouldBe "Bad Request"
          error.requestId shouldBe Some("req-1")
        }
    }

    "preserve 503 retryable error mapping when retries are disabled" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(503, "service-down")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a 503 error"))
          error.statusCode shouldBe 503
          error.message shouldBe "Service unavailable: service-down"
          error.requestId shouldBe Some("req-1")
          transport.requests should have size 1
        }
    }

    "preserve default retryable error message when the response body is oversized" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(503, "x" * 500)))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a 503 error"))
          error.statusCode shouldBe 503
          error.message shouldBe "Service unavailable"
          error.requestId shouldBe Some("req-1")
        }
    }

    "retry a 503 once and sleep for the deterministic exponential backoff" in {
      val runtime = new FakeRuntime(jitter = 0.0)
      val transport = new FakeTransport(
        Seq(
          Right(response(503, "service-down")),
          Right(response(200, "ok")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 1))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result shouldBe Right("ok")
          runtime.sleptMillis.toSeq shouldBe Seq(1000L)
          runtime.nowMillis() shouldBe 2000L
          transport.requests should have size 2
          transport.requests.map(_.headers.find(_._1 == "X-Request-Id").map(_._2)) shouldBe Seq(
            Some("req-1"),
            Some("req-2"),
          )
        }
    }

    "use Retry-After instead of exponential backoff when provided" in {
      val runtime = new FakeRuntime(jitter = 0.0)
      val transport = new FakeTransport(
        Seq(
          Right(response(503, "service-down", Map("Retry-After" -> Seq("7")))),
          Right(response(200, "ok")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 1))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result shouldBe Right("ok")
          runtime.sleptMillis.toSeq shouldBe Seq(7000L)
        }
    }

    "map HttpTimeoutException to a 408 transport error" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Left(new HttpTimeoutException("timed-out")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a timeout error"))
          error.statusCode shouldBe 408
          error.message shouldBe "Request timeout: timed-out"
          error.requestId shouldBe Some("req-1")
        }
    }

    "map ConnectException to a 503 transport error" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Left(new java.net.ConnectException("connection-refused")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected a connection error"))
          error.statusCode shouldBe 503
          error.message shouldBe "Connection failed: connection-refused"
          error.requestId shouldBe Some("req-1")
        }
    }

    "map IOException to a 503 transport error" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Left(new java.io.IOException("broken-pipe")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected an I/O error"))
          error.statusCode shouldBe 503
          error.message shouldBe "I/O error: broken-pipe"
          error.requestId shouldBe Some("req-1")
        }
    }

    "map unexpected exceptions to a 500 transport error" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Left(new RuntimeException("boom")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client
        .call("echo", "00000000", "deadbeef", "submission")
        .failOnShutdown
        .map { result =>
          result.isLeft shouldBe true
          val error = result.swap.getOrElse(fail("Expected an unexpected error"))
          error.statusCode shouldBe 500
          error.message shouldBe "Unexpected error: boom"
          error.requestId shouldBe Some("req-1")
        }
    }

    "use the transport seam in validation and accept 5xx responses as reachable" in {
      val runtime = new FakeRuntime()
      val transport = new FakeTransport(
        Seq(Right(response(503, "service-down")))
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      client.validateConfiguration().failOnShutdown.map { result =>
        result shouldBe ExtensionValidationResult.Valid

        transport.requests should have size 1
        val request = transport.requests.head
        request.uri shouldBe URI.create("http://localhost:8080/api/v1/external-call")
        request.timeout shouldBe Duration.ofMillis(500)
        request.headers should contain theSameElementsInOrderAs Seq(
          "Content-Type" -> "application/octet-stream",
          "X-Daml-External-Function-Id" -> "_health",
          "X-Daml-External-Config-Hash" -> "",
          "X-Daml-External-Mode" -> "validation",
          "X-Request-Id" -> "req-1",
        )
        request.body shouldBe ""
      }
    }

    "create resource transport only once per client instance" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val transport = new FakeTransport(
        Seq(
          Right(response(200, "first-response")),
          Right(response(200, "second-response")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(transport)
      val client = makeClient(resourcesFactory, runtime)

      for {
        result1 <- client.call("echo", "00000000", "deadbeef", "submission").failOnShutdown
        result2 <- client.call("echo", "00000000", "cafebabe", "submission").failOnShutdown
      } yield {
        result1 shouldBe Right("first-response")
        result2 shouldBe Right("second-response")
        resourcesFactory.createCalls should have size 1
      }
    }
  }
}
