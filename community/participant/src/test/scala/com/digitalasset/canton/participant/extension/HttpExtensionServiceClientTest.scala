// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.daml.tls.{ServerAuthRequirementConfig, TlsClientConfigOnlyTrustFile, TlsServerConfig}
import com.digitalasset.canton.config.RequireTypes.{ExistingFile, NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PemFile, PositiveFiniteDuration}
import com.digitalasset.canton.http.HttpService
import com.digitalasset.canton.lifecycle.{FlagCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
}
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.sun.net.httpserver.{HttpHandler, HttpServer, HttpsConfigurator, HttpsServer}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.io.IOException
import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext

class HttpExtensionServiceClientTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "HttpExtensionServiceClient" should {
    "connect to an HTTPS service using the configured trust collection" in {
      val server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.setHttpsConfigurator(new HttpsConfigurator(testServerSslContext()))
      server.createContext(
        "/",
        exchange => writeResponse(exchange, 200, "beef"),
      )
      server.start()

      try {
        val config = configFor(server).copy(
          tls = Some(TlsClientConfigOnlyTrustFile(trustCollectionFile = Some(testPem("ca.crt")))),
          connectTimeout = PositiveFiniteDuration.ofSeconds(2),
          requestTimeout = PositiveFiniteDuration.ofSeconds(5),
        )
        val client = newClient(config)

        client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue shouldBe Right("beef")
      } finally {
        server.stop(0)
      }
    }

    "send the expected external-call HTTP request" in {
      val observedMethod = new AtomicReference[String]("")
      val observedPath = new AtomicReference[String]("")
      val observedContentType = new AtomicReference[String]("")
      val observedFunctionId = new AtomicReference[String]("")
      val observedConfigHash = new AtomicReference[String]("")
      val observedMode = new AtomicReference[String]("")
      val observedRequestId = new AtomicReference[String]("")
      val observedIdempotencyKey = new AtomicReference[String]("")
      val observedAccept = new AtomicReference[String]("")
      val observedBody = new AtomicReference[String]("")

      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          val headers = exchange.getRequestHeaders
          observedMethod.set(exchange.getRequestMethod)
          observedPath.set(exchange.getRequestURI.getPath)
          observedContentType.set(headers.getFirst("Content-Type"))
          observedFunctionId.set(headers.getFirst("X-Daml-External-Function-Id"))
          observedConfigHash.set(headers.getFirst("X-Daml-External-Config-Hash"))
          observedMode.set(headers.getFirst("X-Daml-External-Mode"))
          observedRequestId.set(Option(headers.getFirst("X-Request-Id")).getOrElse(""))
          observedIdempotencyKey.set(Option(headers.getFirst("Idempotency-Key")).getOrElse(""))
          observedAccept.set(headers.getFirst("Accept"))
          observedBody.set(
            new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          )
          writeResponse(exchange, 200, "beef")
        }): HttpHandler,
      )
      server.start()

      try {
        val config = configFor(server).copy(version = "v-test")
        val client = newClient(config)

        client
          .call("external-function", "config-hash-123", "001122", ExternalCallMode.Validation)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue shouldBe Right("beef")

        observedMethod.get shouldBe "POST"
        observedPath.get shouldBe "/api/v-test/external-call"
        observedContentType.get shouldBe "text/plain; charset=utf-8"
        observedAccept.get shouldBe "text/plain"
        observedFunctionId.get shouldBe "external-function"
        observedConfigHash.get shouldBe "config-hash-123"
        observedMode.get shouldBe "validation"
        observedRequestId.get should not be empty
        observedIdempotencyKey.get should not be empty
        observedBody.get shouldBe "001122"
      } finally {
        server.stop(0)
      }
    }

    "build service URIs for IPv6 literals" in {
      val uri = HttpExtensionServiceClient.serviceUri(
        scheme = "http",
        address = "::1",
        port = Port.tryCreate(8080),
        path = "/api/v0/external-call",
      )

      uri.toString shouldBe "http://[::1]:8080/api/v0/external-call"
      Set("::1", "[::1]") should contain(uri.getHost)
    }

    "send empty config hash and input as an empty HTTP header and body" in {
      val observedConfigHash = new AtomicReference[String]("not observed")
      val observedBody = new AtomicReference[String]("not observed")

      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          val headers = exchange.getRequestHeaders
          observedConfigHash.set(headers.getFirst("X-Daml-External-Config-Hash"))
          observedBody.set(
            new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          )
          writeResponse(exchange, 200, "beef")
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(configFor(server))

        client
          .call("external-function", "", "", ExternalCallMode.Validation)(TraceContext.empty)
          .failOnShutdown
          .futureValue shouldBe Right("beef")

        observedConfigHash.get shouldBe ""
        observedBody.get shouldBe ""
      } finally {
        server.stop(0)
      }
    }

    "reject malformed outbound header values without sending a request" in {
      val requests = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          val _ = requests.incrementAndGet()
          writeResponse(exchange, 200, "beef")
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(configFor(server))
        val oversizedValue = "a" * 1025

        Seq(
          ("X-Daml-External-Function-Id", "bad\u0100function", "config-hash", "bad"),
          ("X-Daml-External-Function-Id", oversizedValue, "config-hash", oversizedValue),
          ("X-Daml-External-Config-Hash", "function", oversizedValue, oversizedValue),
        ).foreach { case (headerName, functionId, configHash, redactedValue) =>
          val error = client
            .call(functionId, configHash, "input", ExternalCallMode.Submission)(
              TraceContext.empty
            )
            .failOnShutdown
            .futureValue
            .left
            .value

          error.statusCode shouldBe 400
          // Full equality proves the rejected value does not leak.
          error.message shouldBe s"Invalid external-call HTTP header value for $headerName"
        }

        requests.get shouldBe 0
      } finally {
        server.stop(0)
      }
    }

    "send bearer token auth from the configured token file" in {
      val observedAuthorization = new AtomicReference[Option[String]](None)
      val tokenFile = Files.createTempFile("canton-extension-token", ".txt")
      Files.writeString(tokenFile, "test-token\n", StandardCharsets.UTF_8)

      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          observedAuthorization.set(
            Option(exchange.getRequestHeaders.getFirst("Authorization"))
          )
          exchange.sendResponseHeaders(200, 0)
          exchange.getResponseBody.close()
        }): HttpHandler,
      )
      server.start()

      try {
        val config = ExtensionServiceConfig(
          address = "127.0.0.1",
          port = Port.tryCreate(server.getAddress.getPort),
          version = "v0",
          tls = None,
          auth = ExtensionServiceAuthConfig.BearerTokenFile(
            ExistingFile.tryCreate(tokenFile.toFile)
          ),
        )
        val client = newClient(config)

        client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue shouldBe Right("")
        observedAuthorization.get shouldBe Some("Bearer test-token")
      } finally {
        server.stop(0)
        Files.deleteIfExists(tokenFile)
      }
    }

    "validate the configured API version endpoint on startup" in {
      val observedRequests = new CopyOnWriteArrayList[String]()
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          observedRequests.add(s"${exchange.getRequestMethod} ${exchange.getRequestURI.getPath}")
          writeResponse(exchange, 200, """{"application":"external-call-server","version":"v0"}""")
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(configFor(server))

        client
          .validateConnection()
          .failOnShutdown
          .futureValue shouldBe ExtensionValidationResult.Valid
        observedRequests.size shouldBe 1
        observedRequests.get(0) shouldBe "GET /api/v0/version"
      } finally {
        server.stop(0)
      }
    }

    "retry retryable 412 responses and return the later successful response" in {
      val attempts = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (
            exchange =>
              if (attempts.incrementAndGet() == 1) {
                writeResponse(exchange, 412, "try again")
              } else {
                writeResponse(exchange, 200, "cafe")
              }
        ): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
            retryMaxDelay = NonNegativeFiniteDuration.ofMillis(10),
          )
        )

        // Retries with a finite maxRetries log at INFO only; there is no warning to suppress.
        client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)
          .failOnShutdown
          .futureValue shouldBe Right("cafe")
        attempts.get() shouldBe 2
      } finally {
        server.stop(0)
      }
    }

    "reuse the same idempotency key across retry attempts" in {
      val attempts = new AtomicInteger(0)
      val observedIdempotencyKeys = new CopyOnWriteArrayList[String]()
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          observedIdempotencyKeys.add(
            Option(exchange.getRequestHeaders.getFirst("Idempotency-Key")).getOrElse("")
          )
          if (attempts.incrementAndGet() == 1) {
            writeResponse(exchange, 503, "try again")
          } else {
            writeResponse(exchange, 200, "cafe")
          }
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
            retryMaxDelay = NonNegativeFiniteDuration.ofMillis(10),
          )
        )

        client
          .call(
            "function",
            "config-hash",
            "input",
            ExternalCallMode.Submission,
          )
          .failOnShutdown
          .futureValue shouldBe Right("cafe")

        observedIdempotencyKeys.size shouldBe 2
        observedIdempotencyKeys.get(0) should not be empty
        observedIdempotencyKeys.get(1) shouldBe observedIdempotencyKeys.get(0)
      } finally {
        server.stop(0)
      }
    }

    "retry retryable responses using Canton backoff even when Retry-After is present" in {
      val attempts = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (
            exchange =>
              if (attempts.incrementAndGet() == 1) {
                exchange.getResponseHeaders.add("Retry-After", "5")
                writeResponse(exchange, 429, "retry later")
              } else {
                writeResponse(exchange, 200, "cafe")
              }
        ): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            connectTimeout = PositiveFiniteDuration.ofSeconds(5),
            requestTimeout = PositiveFiniteDuration.ofSeconds(5),
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
            retryMaxDelay = NonNegativeFiniteDuration.ofMillis(1),
          )
        )

        client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)
          .failOnShutdown
          .futureValue shouldBe Right("cafe")

        attempts.get() shouldBe 2
      } finally {
        server.stop(0)
      }
    }

    "stop after maxRetries and return the last retryable error" in {
      val attempts = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          attempts.incrementAndGet()
          writeResponse(exchange, 503, "still unavailable")
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
            retryMaxDelay = NonNegativeFiniteDuration.ofMillis(10),
          )
        )

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 503
        error.message shouldBe "Service unavailable"
        error.message should not include "still unavailable"
        attempts.get() shouldBe 2
      } finally {
        server.stop(0)
      }
    }

    "not retry non-retryable HTTP errors" in {
      val attempts = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          attempts.incrementAndGet()
          writeResponse(exchange, 401, "missing token")
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
            retryMaxDelay = NonNegativeFiniteDuration.ofMillis(10),
          )
        )

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 401
        error.message shouldBe "Unauthorized - check bearer token"
        error.message should not include "missing token"
        attempts.get() shouldBe 1
      } finally {
        server.stop(0)
      }
    }

    "reject oversized response bodies before interpreting the HTTP status" in {
      Seq(200, 400, 500).foreach { statusCode =>
        withClue(s"statusCode=$statusCode") {
          val attempts = new AtomicInteger(0)
          val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
          server.createContext(
            "/",
            (exchange => {
              attempts.incrementAndGet()
              writeResponse(exchange, statusCode, "beef00")
            }): HttpHandler,
          )
          server.start()

          try {
            val client = newClient(
              configFor(server).copy(
                maxRetries = NonNegativeInt.tryCreate(1),
                retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1),
                retryMaxDelay = NonNegativeFiniteDuration.ofMillis(10),
                maxResponseBodyBytes = PositiveInt.tryCreate(4),
              )
            )

            val error = loggerFactory.assertLogs(
              client
                .call("function", "config-hash", "input", ExternalCallMode.Submission)
                .failOnShutdown
                .futureValue
                .left
                .value,
              { entry =>
                entry.warningMessage should fullyMatch regex
                  ("External call to extension 'test-extension' response body exceeded " +
                    s"maximum size of 4 bytes: externalCallId=$uuidRegex")
                entry.warningMessage should not include "beef00"
              },
            )

            error.statusCode shouldBe 413
            error.message shouldBe
              "External call response body exceeded maximum size of 4 bytes"
            error.message should not include "beef00"
            attempts.get() shouldBe 1
          } finally {
            server.stop(0)
          }
        }
      }
    }

    "map stalled response body timeout failures to 408" in {
      val releaseResponse = new CountDownLatch(1)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          val body = "too late".getBytes(StandardCharsets.UTF_8)
          exchange.sendResponseHeaders(200, body.length.toLong)
          val responseBody = exchange.getResponseBody
          try {
            responseBody.flush()
            releaseResponse.await(5, TimeUnit.SECONDS)
            responseBody.write(body)
          } catch {
            case _: IOException => ()
          } finally {
            try responseBody.close()
            catch {
              case _: IOException => ()
            }
          }
        }): HttpHandler,
      )
      server.start()

      try {
        val client = newClient(
          configFor(server).copy(
            requestTimeout = PositiveFiniteDuration.ofMillis(100),
            maxRetries = NonNegativeInt.tryCreate(0),
          )
        )

        val error = loggerFactory.assertLogs(
          client
            .call("function", "config-hash", "input", ExternalCallMode.Submission)
            .failOnShutdown
            .futureValue
            .left
            .value,
          _.warningMessage should fullyMatch regex
            s"External call to extension 'test-extension' timed out: externalCallId=$uuidRegex",
        )

        error.statusCode shouldBe 408
        error.message shouldBe "Request timeout"
      } finally {
        releaseResponse.countDown()
        server.stop(0)
      }
    }

    "map connection failures to 503" in {
      val client = newClient(
        configForUnusedPort()
      )

      val error = loggerFactory.assertLogs(
        client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)
          .failOnShutdown
          .futureValue
          .left
          .value,
        _.warningMessage should fullyMatch regex
          ("External call to extension 'test-extension' connection failed: " +
            s"externalCallId=$uuidRegex"),
      )

      error.statusCode shouldBe 503
      error.message shouldBe "Connection failed"
    }

    "abort retry delay when the extension manager is closing" in {
      val firstResponseReturned = new CountDownLatch(1)
      val attempts = new AtomicInteger(0)
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        (exchange => {
          attempts.incrementAndGet()
          writeResponse(exchange, 503, "try again")
          firstResponseReturned.countDown()
        }): HttpHandler,
      )
      server.start()

      val manager = new ExtensionServiceManager(
        Map(
          "test-extension" -> configFor(
            server
          ).copy(
            maxRetries = NonNegativeInt.tryCreate(1),
            retryInitialDelay = NonNegativeFiniteDuration.ofSeconds(2),
            retryMaxDelay = NonNegativeFiniteDuration.ofSeconds(2),
          )
        ),
        loggerFactory,
        timeouts,
      )

      try {
        loggerFactory.suppressWarnings {
          val resultF =
            manager.handleExternalCall(
              "test-extension",
              "function",
              "config-hash",
              "input",
              ExternalCallMode.Submission,
            )
          firstResponseReturned.await(5, TimeUnit.SECONDS) shouldBe true

          manager.close()

          resultF.unwrap.futureValue shouldBe UnlessShutdown.AbortedDueToShutdown
        }
        attempts.get() shouldBe 1
      } finally {
        manager.close()
        server.stop(0)
      }
    }

    "treat invalid bearer token auth as a non-retryable configuration error" in {
      val tokenFile = Files.createTempFile("canton-extension-empty-token", ".txt")

      try {
        val config = ExtensionServiceConfig(
          address = "127.0.0.1",
          port = Port.tryCreate(12345),
          version = "v0",
          tls = None,
          auth = ExtensionServiceAuthConfig.BearerTokenFile(
            ExistingFile.tryCreate(tokenFile.toFile)
          ),
          maxRetries = NonNegativeInt.tryCreate(0),
        )
        val client = newClient(config)

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 400
        error.message shouldBe
          "Invalid extension service authentication configuration: Bearer token file is empty"
      } finally {
        Files.deleteIfExists(tokenFile)
      }
    }

    "not expose the bearer token file path when the token file cannot be read" in {
      val tokenFile = Files.createTempFile("canton-extension-missing-token", ".txt")
      val existingTokenFile = ExistingFile.tryCreate(tokenFile.toFile)
      Files.deleteIfExists(tokenFile)

      val config = ExtensionServiceConfig(
        address = "127.0.0.1",
        port = Port.tryCreate(12345),
        version = "v0",
        tls = None,
        auth = ExtensionServiceAuthConfig.BearerTokenFile(existingTokenFile),
        maxRetries = NonNegativeInt.tryCreate(0),
      )
      val client = newClient(config)

      val error = client
        .call("function", "config-hash", "input", ExternalCallMode.Submission)(
          TraceContext.empty
        )
        .failOnShutdown
        .futureValue
        .left
        .value

      error.statusCode shouldBe 400
      error.message shouldBe
        "Invalid extension service authentication configuration: Failed to read bearer token file"
    }

    "reject malformed bearer token contents without exposing the token" in {
      val tokenFile = Files.createTempFile("canton-extension-malformed-token", ".txt")
      Files.writeString(tokenFile, "secret-token\nInjected: value", StandardCharsets.UTF_8)

      try {
        val config = ExtensionServiceConfig(
          address = "127.0.0.1",
          port = Port.tryCreate(12345),
          version = "v0",
          tls = None,
          auth = ExtensionServiceAuthConfig.BearerTokenFile(
            ExistingFile.tryCreate(tokenFile.toFile)
          ),
          maxRetries = NonNegativeInt.tryCreate(0),
        )
        val client = newClient(config)

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 400
        // Full equality proves neither the token nor the file path leaks.
        error.message shouldBe
          "Invalid extension service authentication configuration: " +
          "Bearer token file contains invalid characters"
      } finally {
        Files.deleteIfExists(tokenFile)
      }
    }

    "reject bearer token contents with surrounding whitespace" in {
      val tokenFile = Files.createTempFile("canton-extension-whitespace-token", ".txt")
      Files.writeString(tokenFile, " secret-token\n", StandardCharsets.UTF_8)

      try {
        val config = configForPort(12345).copy(
          auth = ExtensionServiceAuthConfig.BearerTokenFile(
            ExistingFile.tryCreate(tokenFile.toFile)
          ),
          maxRetries = NonNegativeInt.tryCreate(0),
        )
        val client = newClient(config)

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 400
        // Full equality proves neither the token nor the file path leaks.
        error.message shouldBe
          "Invalid extension service authentication configuration: " +
          "Bearer token file contains invalid characters"
      } finally {
        Files.deleteIfExists(tokenFile)
      }
    }

    "reject non-ASCII bearer token contents without exposing the token" in {
      val tokenFile = Files.createTempFile("canton-extension-non-ascii-token", ".txt")
      Files.writeString(tokenFile, "secret-token\u0100", StandardCharsets.UTF_8)

      try {
        val config = ExtensionServiceConfig(
          address = "127.0.0.1",
          port = Port.tryCreate(12345),
          version = "v0",
          tls = None,
          auth = ExtensionServiceAuthConfig.BearerTokenFile(
            ExistingFile.tryCreate(tokenFile.toFile)
          ),
          maxRetries = NonNegativeInt.tryCreate(0),
        )
        val client = newClient(config)

        val error = client
          .call("function", "config-hash", "input", ExternalCallMode.Submission)(
            TraceContext.empty
          )
          .failOnShutdown
          .futureValue
          .left
          .value

        error.statusCode shouldBe 400
        // Full equality proves neither the token nor the file path leaks.
        error.message shouldBe
          "Invalid extension service authentication configuration: " +
          "Bearer token file contains invalid characters"
      } finally {
        Files.deleteIfExists(tokenFile)
      }
    }
  }

  "ExtensionServiceManager" should {
    "return a helpful 404 when an extension id is not configured" in {
      val manager = new ExtensionServiceManager(
        Map(
          "first-extension" -> configForUnusedPort(),
          "second-extension" -> configForUnusedPort(),
        ),
        loggerFactory,
        timeouts,
      )

      try {
        loggerFactory.assertLogs(
          {
            val error = manager
              .handleExternalCall(
                "missing-extension",
                "function",
                "config-hash",
                "input",
                ExternalCallMode.Submission,
              )
              .failOnShutdown
              .futureValue
              .left
              .value

            error.statusCode shouldBe 404
            error.externalCallId shouldBe None
            // Full equality proves the configured extension ids do not leak.
            error.message shouldBe "Extension 'missing-extension' not configured"
          },
          _.warningMessage shouldBe
            "External call to extension 'missing-extension' (function 'function') failed: " +
            "ExtensionCallError(status code = 404, message = \"Extension 'missing-extension' not configured\", retryable = false, trace id = tid:)",
        )
      } finally {
        manager.close()
      }
    }

    "skip remote startup validation by default" in {
      val manager = new ExtensionServiceManager(
        Map("test-extension" -> configForUnusedPort()),
        loggerFactory,
        timeouts,
      )

      try {
        manager.initializeOnStartup().failOnShutdown.futureValue shouldBe Right(())
      } finally {
        manager.close()
      }
    }

    "fail local startup preflight before remote validation" in {
      val tokenFile = Files.createTempFile("canton-extension-empty-token", ".txt")

      try {
        val manager = new ExtensionServiceManager(
          Map(
            "test-extension" -> configForUnusedPort().copy(
              auth = ExtensionServiceAuthConfig.BearerTokenFile(
                ExistingFile.tryCreate(tokenFile.toFile)
              )
            )
          ),
          loggerFactory,
          timeouts,
        )

        // The exact message proves the token file path does not leak.
        val preflightFailure =
          "Extension startup local preflight failed: Extension 'test-extension': " +
            "Invalid extension service authentication configuration: Bearer token file is empty"

        try {
          loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.ERROR))(
            manager.initializeOnStartup().failOnShutdown.futureValue shouldBe
              Left(preflightFailure),
            entries =>
              inside(entries) { case Seq(entry) =>
                entry.errorMessage shouldBe preflightFailure
              },
          )
        } finally {
          manager.close()
        }
      } finally {
        Files.deleteIfExists(tokenFile)
      }
    }

    "fail startup when enabled remote validation cannot reach the extension service" in {
      val manager = new ExtensionServiceManager(
        Map("test-extension" -> configForUnusedPort().copy(validateOnStartup = true)),
        loggerFactory,
        timeouts,
      )

      // The tail is the JDK's connection-refused message, so pin the structure via regex.
      val validationFailureRegex =
        "Extension startup validation failed: Extension 'test-extension': Connection failed: .*"

      try {
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.ERROR))(
          inside(manager.initializeOnStartup().failOnShutdown.futureValue) { case Left(error) =>
            error should fullyMatch regex validationFailureRegex
          },
          entries =>
            inside(entries) { case Seq(entry) =>
              entry.errorMessage should fullyMatch regex validationFailureRegex
            },
        )
      } finally {
        manager.close()
      }
    }
  }

  private val uuidRegex: String =
    "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

  private def newClient(config: ExtensionServiceConfig)(implicit
      ec: ExecutionContext
  ): HttpExtensionServiceClient =
    new HttpExtensionServiceClient(
      extensionId = "test-extension",
      config = config,
      httpClient = ExtensionServiceManager.createHttpClient(config)(ec),
      timeoutScheduler = scheduledExecutor(),
      performUnlessClosing = FlagCloseable(logger, timeouts),
      loggerFactory = loggerFactory,
    )(ec)

  private def configFor(server: HttpServer): ExtensionServiceConfig =
    configForPort(server.getAddress.getPort)

  private def configForPort(port: Int): ExtensionServiceConfig =
    ExtensionServiceConfig(
      address = "127.0.0.1",
      port = Port.tryCreate(port),
      version = "v0",
      auth = ExtensionServiceAuthConfig.None,
      connectTimeout = PositiveFiniteDuration.ofMillis(100),
      requestTimeout = PositiveFiniteDuration.ofSeconds(1),
      maxRetries = NonNegativeInt.tryCreate(0),
      retryInitialDelay = NonNegativeFiniteDuration.ofMillis(1000),
      retryMaxDelay = NonNegativeFiniteDuration.ofSeconds(1),
    )

  private def configForUnusedPort(): ExtensionServiceConfig =
    configForPort(unusedPort()).copy(
      requestTimeout = PositiveFiniteDuration.ofMillis(500)
    )

  private def testPem(fileName: String): PemFile =
    PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile(s"test-certificates/$fileName")))

  private def testServerSslContext(): javax.net.ssl.SSLContext =
    HttpService.buildSSLContext(
      TlsServerConfig(
        certChainFile = testPem("server.crt"),
        privateKeyFile = testPem("server.pem"),
        trustCollectionFile = Some(testPem("ca.crt")),
        clientAuth = ServerAuthRequirementConfig.None,
      )
    )

  private def writeResponse(
      exchange: com.sun.net.httpserver.HttpExchange,
      statusCode: Int,
      body: String,
  ): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(statusCode, bytes.length.toLong)
    val responseBody = exchange.getResponseBody
    try responseBody.write(bytes)
    finally responseBody.close()
  }

  private def unusedPort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}
