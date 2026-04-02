// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
  ExtensionServiceTokenEndpointConfig,
}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpsConfigurator, HttpsServer}
import org.scalatest.Assertion
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Json
import org.scalatest.wordspec.AsyncWordSpec

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, KeyPairGenerator, KeyStore, SecureRandom}
import java.time.Duration
import java.util.Base64
import java.util.concurrent.{CopyOnWriteArrayList, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import org.slf4j.event.Level
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.Using

class HttpExtensionServiceClientOAuthTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private val tlsDirectory = Paths.get("community/app/src/test/resources/tls")
  private val rootCaFile = tlsDirectory.resolve("root-ca.crt")
  private val serverCertFile = tlsDirectory.resolve("public-api.crt")
  private val serverPrivateKeyFile = tlsDirectory.resolve("public-api.pem")

  private def makeConfig(
      name: String = "test-ext",
      host: String = "resource.example.internal",
      port: Int = 9443,
      tokenHost: String = "issuer.example.internal",
      tokenPort: Int = 443,
      privateKeyFile: Path = Paths.get("/definitely/missing-test-key.der"),
      resourceTrustCollectionFile: Option[Path] = None,
      tokenTrustCollectionFile: Option[Path] = None,
      requestTimeoutMillis: Long = 10000L,
      maxTotalTimeoutMillis: Long = 25000L,
      maxRetries: Int = 2,
  ): ExtensionServiceConfig =
    ExtensionServiceConfig(
      name = name,
      host = host,
      port = Port.tryCreate(port),
      useTls = true,
      trustCollectionFile = resourceTrustCollectionFile,
      auth = ExtensionServiceAuthConfig.OAuth(
        tokenEndpoint = ExtensionServiceTokenEndpointConfig(
          host = tokenHost,
          port = Port.tryCreate(tokenPort),
          path = "/oauth2/token",
          trustCollectionFile = tokenTrustCollectionFile,
        ),
        clientId = "participant1",
        privateKeyFile = privateKeyFile,
        scope = Some("external.call.invoke"),
      ),
      connectTimeout = NonNegativeFiniteDuration.ofMillis(500),
      requestTimeout = NonNegativeFiniteDuration.ofMillis(requestTimeoutMillis),
      maxTotalTimeout = NonNegativeFiniteDuration.ofMillis(maxTotalTimeoutMillis),
      maxRetries = NonNegativeInt.tryCreate(maxRetries),
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

  private final class ScriptedHttpsOAuthServer(
      val port: Int,
      val tokenRequests: CopyOnWriteArrayList[String],
      val resourceRequests: CopyOnWriteArrayList[String],
  )

  private def withScriptedHttpsOAuthServer(
      tokenResponses: Seq[HttpExtensionClientResponse],
      resourceResponses: Seq[HttpExtensionClientResponse],
  )(test: ScriptedHttpsOAuthServer => Future[Assertion]): Future[Assertion] = {
    val queuedTokenResponses = mutable.Queue.from(tokenResponses)
    val queuedResourceResponses = mutable.Queue.from(resourceResponses)
    val tokenRequests = new CopyOnWriteArrayList[String]()
    val resourceRequests = new CopyOnWriteArrayList[String]()

    val server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0)
    server.setHttpsConfigurator(new HttpsConfigurator(buildServerSslContext()))
    server.createContext(
      "/oauth2/token",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          tokenRequests.add(exchange.getRequestURI.toString)
          writeServerResponse(
            exchange,
            dequeueServerResponse(queuedTokenResponses, "token response"),
          )
        }
      },
    )
    server.createContext(
      "/api/v1/external-call",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          resourceRequests.add(exchange.getRequestURI.toString)
          writeServerResponse(
            exchange,
            dequeueServerResponse(queuedResourceResponses, "resource response"),
          )
        }
      },
    )
    server.start()

    test(
      new ScriptedHttpsOAuthServer(
        port = server.getAddress.getPort,
        tokenRequests = tokenRequests,
        resourceRequests = resourceRequests,
      )
    ).transform { result =>
      server.stop(0)
      result
    }(executionContext)
  }

  private def dequeueServerResponse(
      queue: mutable.Queue[HttpExtensionClientResponse],
      label: String,
  ): HttpExtensionClientResponse = queue.synchronized {
    if (queue.nonEmpty) {
      queue.dequeue()
    } else {
      fail(s"No $label left to dequeue")
    }
  }

  private def writeServerResponse(
      exchange: HttpExchange,
      response: HttpExtensionClientResponse,
  ): Unit = {
    response.headers.foreach { case (header, values) =>
      values.foreach(exchange.getResponseHeaders.add(header, _))
    }
    val body = response.body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(response.statusCode, body.length.toLong)
    Using.resource(exchange.getResponseBody)(_.write(body))
    exchange.close()
  }

  private def buildServerSslContext(): SSLContext = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val serverCertificate = Using.resource(Files.newInputStream(serverCertFile)) { inputStream =>
      certificateFactory.generateCertificate(inputStream).asInstanceOf[X509Certificate]
    }
    val privateKey = loadPkcs8PrivateKey(serverPrivateKeyFile)
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null, null)
    keyStore.setKeyEntry("server", privateKey, Array.emptyCharArray, Array(serverCertificate))

    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, Array.emptyCharArray)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, null, new SecureRandom())
    sslContext
  }

  private def loadPkcs8PrivateKey(path: Path) = {
    val pem = Files.readString(path, StandardCharsets.US_ASCII)
    val encoded = pem.linesIterator
      .filterNot(line => line.startsWith("-----BEGIN") || line.startsWith("-----END"))
      .mkString
    val keySpec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder.decode(encoded))
    KeyFactory.getInstance("RSA").generatePrivate(keySpec)
  }

  private final class FakeRuntime(
      initialNowMillis: Long = 1000L,
      requestIds: Seq[String] = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6"),
      jitter: Double = 0.0,
  ) extends HttpExtensionClientRuntime {
    private val ids = mutable.Queue.from(requestIds)

    val sleptMillis: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty

    private var now: Long = initialNowMillis

    override def nowMillis(): Long = this.synchronized(now)

    def advanceMillis(ms: Long): Unit = this.synchronized {
      now += ms
    }

    override def sleepMillis(ms: Long): Unit = {
      sleptMillis += ms
      advanceMillis(ms)
    }

    override def newRequestId(): String = this.synchronized {
      if (ids.nonEmpty) {
        ids.dequeue()
      } else {
        fail("No request IDs left in fake runtime")
      }
    }

    override def nextRetryJitterDouble(): Double = jitter
  }

  private final class FakeTransport(
      outcomes: Seq[Either[Throwable, HttpExtensionClientResponse]],
      onSend: HttpExtensionClientRequest => Unit = _ => (),
  ) extends HttpExtensionClientTransport {
    private val queuedOutcomes = mutable.Queue.from(outcomes)

    val requests: mutable.ArrayBuffer[HttpExtensionClientRequest] = mutable.ArrayBuffer.empty

    override def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse = {
      this.synchronized {
        requests += request
      }
      onSend(request)
      val outcome = this.synchronized {
        if (queuedOutcomes.nonEmpty) {
          queuedOutcomes.dequeue()
        } else {
          fail("No transport outcomes left to dequeue")
        }
      }
      outcome match {
        case Left(exception) => throw exception
        case Right(resp) => resp
      }
    }
  }

  private final class FakeResourcesFactory(
      resourceTransport: HttpExtensionClientTransport,
      tokenTransport: HttpExtensionClientTransport,
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

  private final class MissingTokenTransportResourcesFactory(
      resourceTransport: HttpExtensionClientTransport
  ) extends HttpExtensionClientResourcesFactory {
    val createCalls: mutable.ArrayBuffer[ExtensionServiceConfig] = mutable.ArrayBuffer.empty

    override def create(config: ExtensionServiceConfig): HttpExtensionClientResources = {
      createCalls += config
      HttpExtensionClientResources(
        resourceTransport = resourceTransport,
        tokenTransport = None,
      )
    }
  }

  private final class ThrowingResourcesFactory(exception: RuntimeException)
      extends HttpExtensionClientResourcesFactory {
    val createCalls: mutable.ArrayBuffer[ExtensionServiceConfig] = mutable.ArrayBuffer.empty

    override def create(config: ExtensionServiceConfig): HttpExtensionClientResources = {
      createCalls += config
      throw exception
    }
  }

  private def makeClient(
      resourcesFactory: HttpExtensionClientResourcesFactory,
      runtime: HttpExtensionClientRuntime,
      config: ExtensionServiceConfig = makeConfig(),
      oauthAssertionFactory: Option[() => String] = Some(() => "signed.jwt.value"),
      callExecutionContext: ExecutionContext = executionContext,
  ): HttpExtensionServiceClient =
    new HttpExtensionServiceClient(
      extensionId = config.name,
      config = config,
      resourcesFactory = resourcesFactory,
      runtime = runtime,
      requestBuilder = new HttpExtensionRequestBuilder(config),
      responseMapper = new HttpExtensionResponseMapper,
      oauthAssertionFactory = oauthAssertionFactory,
      loggerFactory = loggerFactory,
    )(callExecutionContext)

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

    "log token acquisition start and success on the first business request" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result shouldBe Right("ok")
            result
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Starting OAuth token acquisition")) shouldBe true
            messages.exists(_.contains("OAuth token acquisition succeeded")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "log cache reuse for later business requests with an unexpired token" in {
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

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          for {
            result1 <- client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
            result2 <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
          } yield {
            result1 shouldBe Right("ok-1")
            result2 shouldBe Right("ok-2")
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Reusing cached OAuth token")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "avoid logging access tokens, client assertions, or token-endpoint request bodies" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val accessToken = "opaque.access.secret"
      val clientAssertion = "signed.jwt.secret"
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = accessToken, expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        oauthAssertionFactory = Some(() => clientAssertion),
      )

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result shouldBe Right("ok")
            result
          },
          logs => {
            val messages = logs.map(_.message).mkString("\n")

            messages should not include accessToken
            messages should not include clientAssertion
            messages should not include "grant_type=client_credentials"
            messages should not include "client_assertion="
          },
        )
        .map(_ => succeed)
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

    "log token reacquisition after local expiry" in {
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

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          for {
            result1 <- client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
            _ = runtime.advanceMillis(1500L)
            result2 <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
          } yield {
            result1 shouldBe Right("ok-1")
            result2 shouldBe Right("ok-2")
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Reacquiring OAuth token")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "keep token caches isolated per extension" in {
      val runtimeA = new FakeRuntime(requestIds = Seq("a-req-1", "a-req-2", "a-req-3"))
      val runtimeB = new FakeRuntime(requestIds = Seq("b-req-1", "b-req-2", "b-req-3"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-a", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-b", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, "ok-a-1")),
          Right(response(200, "ok-b-1")),
          Right(response(200, "ok-a-2")),
          Right(response(200, "ok-b-2")),
        )
      )
      val clientA = makeClient(
        resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport),
        runtime = runtimeA,
        config = makeConfig(name = "ext-a"),
      )
      val clientB = makeClient(
        resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport),
        runtime = runtimeB,
        config = makeConfig(name = "ext-b"),
      )

      for {
        resultA1 <- clientA.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        resultB1 <- clientB.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
        resultA2 <- clientA.call("echo", "cafebabe", "beadfeed", "submission").failOnShutdown
        resultB2 <- clientB.call("echo", "cafebabe", "facefeed", "submission").failOnShutdown
      } yield {
        resultA1 shouldBe Right("ok-a-1")
        resultB1 shouldBe Right("ok-b-1")
        resultA2 shouldBe Right("ok-a-2")
        resultB2 shouldBe Right("ok-b-2")

        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 4
        tokenTransport.requests.map(_.headers.toMap.apply("X-Request-Id")) shouldBe Seq(
          "a-req-1",
          "b-req-1",
        )
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-a",
          "Bearer token-b",
          "Bearer token-a",
          "Bearer token-b",
        )
      }
    }

    "serialize concurrent cold-cache misses so waiting requests reuse the same successful acquisition" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseTokenResponse = Promise[Unit]()
      val firstTokenSendStarted = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val tokenTransport = new FakeTransport(
        outcomes = Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L)))),
        onSend = _ => {
          firstTokenSendStarted.trySuccess(())
          Await.result(releaseTokenResponse.future, 5.seconds)
        },
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, "ok-1")),
          Right(response(200, "ok-2")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        callExecutionContext = clientEc,
      )
      val call1 = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
      val call2 = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown

      val resultF = for {
        _ <- firstTokenSendStarted.future.map(_ => releaseTokenResponse.success(()))(executionContext)
        results <- Future.sequence(Seq(call1, call2))
      } yield {
        results.toSet shouldBe Set(Right("ok-1"), Right("ok-2"))
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 2
        all(resourceTransport.requests.map(_.headers.toMap.apply("Authorization"))) shouldBe "Bearer token-1"
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "propagate one failed cold-cache acquisition to concurrent waiters instead of starting a second acquisition" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseTokenFailure = Promise[Unit]()
      val firstTokenSendStarted = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        outcomes = Seq(
          Left(new RuntimeException("issuer-boom")),
          Left(new RuntimeException("unexpected-second-acquisition")),
        ),
        onSend = _ => {
          firstTokenSendStarted.trySuccess(())
          Await.result(releaseTokenFailure.future, 5.seconds)
        },
      )
      val resourceTransport = new FakeTransport(Seq.empty)
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
        callExecutionContext = clientEc,
      )
      val call1 = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
      val call2 = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown

      val resultF = for {
        _ <- firstTokenSendStarted.future.map(_ => releaseTokenFailure.success(()))(executionContext)
        results <- Future.sequence(Seq(call1, call2))
      } yield {
        results.map(_.left.map(err => (err.statusCode, err.message, err.requestId))) shouldBe Seq(
          Left((500, "Unexpected error: issuer-boom", Some("req-1"))),
          Left((500, "Unexpected error: issuer-boom", Some("req-1"))),
        )
        tokenTransport.requests should have size 1
        resourceTransport.requests shouldBe empty
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "allow a later acquisition attempt after a shared cold-cache failure has completed" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseTokenFailure = Promise[Unit]()
      val firstTokenSendStarted = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val tokenTransport = new FakeTransport(
        outcomes = Seq(
          Left(new RuntimeException("issuer-boom")),
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
        ),
        onSend = _ =>
          if (firstTokenSendStarted.trySuccess(())) {
            Await.result(releaseTokenFailure.future, 5.seconds)
          },
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok-after-retry"))))
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
        callExecutionContext = clientEc,
      )
      val failingCall = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown

      val resultF = for {
        _ <- firstTokenSendStarted.future.map(_ => releaseTokenFailure.success(()))(executionContext)
        firstResult <- failingCall
        secondResult <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
      } yield {
        firstResult.left.value.statusCode shouldBe 500
        secondResult shouldBe Right("ok-after-retry")
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 1
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "serialize concurrent expiry-driven refreshes and share the successful refreshed token" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseRefreshResponse = Promise[Unit]()
      val refreshStarted = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5"))
      lazy val tokenTransport: FakeTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 1L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        ),
        onSend = _ =>
          if (tokenTransport.requests.lengthCompare(2) == 0) {
            refreshStarted.trySuccess(())
            Await.result(releaseRefreshResponse.future, 5.seconds)
          },
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, "ok-initial")),
          Right(response(200, "ok-a")),
          Right(response(200, "ok-b")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime, callExecutionContext = clientEc)

      val resultF = for {
        initial <- client.call("echo", "cafebabe", "warmup", "submission").failOnShutdown
        _ = initial shouldBe Right("ok-initial")
        _ = runtime.advanceMillis(5000L)
        call1 = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        call2 = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
        _ <- refreshStarted.future.map(_ => releaseRefreshResponse.success(()))(executionContext)
        results <- Future.sequence(Seq(call1, call2))
      } yield {
        results.toSet shouldBe Set(Right("ok-a"), Right("ok-b"))
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 3
        resourceTransport.requests.drop(1).map(_.headers.toMap.apply("Authorization")).toSet shouldBe Set(
          "Bearer token-2"
        )
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "serialize concurrent 401-driven refreshes and share the successful refreshed token" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseRefreshResponse = Promise[Unit]()
      val refreshStarted = Promise[Unit]()
      val concurrentToken1RequestsSeen = Promise[Unit]()
      val observeConcurrentCalls = new AtomicBoolean(false)
      val token1ConcurrentRequestCount = new AtomicInteger(0)
      val runtime =
        new FakeRuntime(
          requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6", "req-7")
        )
      lazy val tokenTransport: FakeTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        ),
        onSend = _ =>
          if (tokenTransport.requests.lengthCompare(2) == 0) {
            refreshStarted.trySuccess(())
            Await.result(concurrentToken1RequestsSeen.future, 5.seconds)
            Await.result(releaseRefreshResponse.future, 5.seconds)
          },
      )
      val resourceTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, "ok-initial")),
          Right(response(401, "unauthorized-a")),
          Right(response(401, "unauthorized-b")),
          Right(response(200, "ok-a")),
          Right(response(200, "ok-b")),
        ),
        onSend = request => {
          val authorization = request.headers.toMap.get("Authorization")
          if (observeConcurrentCalls.get() && authorization.contains("Bearer token-1")) {
            if (token1ConcurrentRequestCount.incrementAndGet() == 2) {
              concurrentToken1RequestsSeen.trySuccess(())
            }
          }
        },
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime, callExecutionContext = clientEc)

      val resultF = for {
        initial <- client.call("echo", "cafebabe", "warmup", "submission").failOnShutdown
        _ = initial shouldBe Right("ok-initial")
        _ = observeConcurrentCalls.set(true)
        call1 = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        call2 = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
        _ <- refreshStarted.future.map(_ => releaseRefreshResponse.success(()))(executionContext)
        results <- Future.sequence(Seq(call1, call2))
      } yield {
        results.toSet shouldBe Set(Right("ok-a"), Right("ok-b"))
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 5
        resourceTransport.requests.slice(1, 3).map(_.headers.toMap.apply("Authorization")).toSet shouldBe Set(
          "Bearer token-1"
        )
        resourceTransport.requests.drop(3).map(_.headers.toMap.apply("Authorization")).toSet shouldBe Set(
          "Bearer token-2"
        )
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "propagate one failed 401-driven refresh to concurrent waiters and allow a later refresh attempt" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseRefreshFailure = Promise[Unit]()
      val refreshStarted = Promise[Unit]()
      val concurrentToken1RequestsSeen = Promise[Unit]()
      val observeConcurrentCalls = new AtomicBoolean(false)
      val token1ConcurrentRequestCount = new AtomicInteger(0)
      val runtime =
        new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6", "req-7"))
      lazy val tokenTransport: FakeTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Left(new RuntimeException("refresh-boom")),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        ),
        onSend = _ =>
          if (tokenTransport.requests.lengthCompare(2) == 0) {
            refreshStarted.trySuccess(())
            Await.result(concurrentToken1RequestsSeen.future, 5.seconds)
            Await.result(releaseRefreshFailure.future, 5.seconds)
          },
      )
      val resourceTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, "ok-initial")),
          Right(response(401, "unauthorized-a")),
          Right(response(401, "unauthorized-b")),
          Right(response(200, "ok-after-retry")),
        ),
        onSend = request => {
          val authorization = request.headers.toMap.get("Authorization")
          if (observeConcurrentCalls.get() && authorization.contains("Bearer token-1")) {
            if (token1ConcurrentRequestCount.incrementAndGet() == 2) {
              concurrentToken1RequestsSeen.trySuccess(())
            }
          }
        },
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
        callExecutionContext = clientEc,
      )

      val resultF = for {
        initial <- client.call("echo", "cafebabe", "warmup", "submission").failOnShutdown
        _ = initial shouldBe Right("ok-initial")
        _ = observeConcurrentCalls.set(true)
        call1 = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        call2 = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
        _ <- refreshStarted.future.map(_ => releaseRefreshFailure.success(()))(executionContext)
        results <- Future.sequence(Seq(call1, call2))
        laterResult <- client.call("echo", "cafebabe", "beadfeed", "submission").failOnShutdown
      } yield {
        val refreshRequestId = tokenTransport.requests(1).headers.toMap.apply("X-Request-Id")
        results.map(_.left.map(err => (err.statusCode, err.message, err.requestId))).toSet shouldBe Set(
          Left((500, "Unexpected error: refresh-boom", Some(refreshRequestId)))
        )
        laterResult shouldBe Right("ok-after-retry")
        tokenTransport.requests should have size 3
        resourceTransport.requests should have size 4
        resourceTransport.requests.last.headers.toMap.apply("Authorization") shouldBe "Bearer token-2"
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "invalidate a rejected cached token, reacquire a fresh token, replay exactly once, and reuse the fresh token afterwards" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized")),
          Right(response(200, "ok-replayed")),
          Right(response(200, "ok-reused")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      for {
        result1 <- client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        result2 <- client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
      } yield {
        result1 shouldBe Right("ok-replayed")
        result2 shouldBe Right("ok-reused")

        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 3

        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
          "Bearer token-2",
        )
        resourceTransport.requests.map(_.headers.toMap.apply("X-Request-Id")) shouldBe Seq(
          "req-2",
          "req-4",
          "req-5",
        )
      }
    }

    "preserve a newer cached token when a late 401 arrives for an older token value" in {
      val executor = Executors.newFixedThreadPool(2)
      val clientEc = ExecutionContext.fromExecutorService(executor)
      val releaseLateUnauthorized = Promise[Unit]()
      val lateUnauthorizedStarted = Promise[Unit]()
      val runtime = new FakeRuntime(
        requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6")
      )
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 1L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val recordedRequests = new CopyOnWriteArrayList[HttpExtensionClientRequest]()
      val requestCounter = new AtomicInteger(0)
      val resourceTransport = new HttpExtensionClientTransport {
        override def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse = {
          recordedRequests.add(request)
          requestCounter.incrementAndGet() match {
            case 1 =>
              response(200, "ok-initial")
            case 2 =>
              lateUnauthorizedStarted.trySuccess(())
              Await.result(releaseLateUnauthorized.future, 5.seconds)
              response(401, "late-unauthorized")
            case 3 =>
              response(200, "ok-fresh")
            case 4 =>
              response(200, "ok-replayed")
            case other =>
              fail(s"Unexpected resource request number: $other")
          }
        }
      }
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime, callExecutionContext = clientEc)

      val resultF = for {
        initial <- client.call("echo", "cafebabe", "warmup", "submission").failOnShutdown
        _ = initial shouldBe Right("ok-initial")
        late401Call = client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown
        _ <- lateUnauthorizedStarted.future.map { _ =>
          runtime.advanceMillis(5000L)
          ()
        }(executionContext)
        freshCall = client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown
        freshResult <- freshCall
        _ = releaseLateUnauthorized.success(())
        late401Result <- late401Call
      } yield {
        freshResult shouldBe Right("ok-fresh")
        late401Result shouldBe Right("ok-replayed")
        tokenTransport.requests should have size 2
        recordedRequests.asScala.toSeq.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-1",
          "Bearer token-2",
          "Bearer token-2",
        )
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "return terminal 401 with the OAuth-specific rejection message when the replay also gets 401 and do not replay again" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized-1")),
          Right(response(401, "unauthorized-2")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 401
        error.message shouldBe "Unauthorized - OAuth token rejected by resource server"
        error.requestId shouldBe Some("req-4")

        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 2
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
        )
      }
    }

    "log token invalidation after a resource-server 401 triggers replay" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized")),
          Right(response(200, "ok-replayed")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result shouldBe Right("ok-replayed")
            result
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Invalidated cached OAuth token")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "do not trigger auth-local replay on non-401 resource responses" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(403, "forbidden")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 403
        error.message shouldBe "Forbidden - insufficient permissions: forbidden"
        error.requestId shouldBe Some("req-2")
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 1
        resourceTransport.requests.head.headers should contain("Authorization" -> "Bearer token-1")
      }
    }

    "allow the OAuth-specific 401 replay even when maxRetries is zero" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized")),
          Right(response(200, "ok-replayed")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok-replayed")
        runtime.sleptMillis shouldBe empty
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 2
      }
    }

    "feed a retryable replay result back into the normal outer retry loop" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized")),
          Right(response(503, "service-down")),
          Right(response(200, "ok-after-outer-retry")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 1),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok-after-outer-retry")
        runtime.sleptMillis.toSeq shouldBe Seq(1000L)
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 3
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
          "Bearer token-2",
        )
      }
    }

    "clamp both token and resource request timeouts to the remaining outer budget" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(requestTimeoutMillis = 10000L, maxTotalTimeoutMillis = 3000L),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok")
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 1
        tokenTransport.requests.head.timeout shouldBe Duration.ofMillis(3000L)
        resourceTransport.requests.head.timeout shouldBe Duration.ofMillis(3000L)
      }
    }

    "recompute the resource-request timeout from the remaining outer budget after token acquisition work" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        outcomes = Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L)))),
        onSend = _ => runtime.advanceMillis(1200L),
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(requestTimeoutMillis = 10000L, maxTotalTimeoutMillis = 3000L),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok")
        tokenTransport.requests.head.timeout shouldBe Duration.ofMillis(3000L)
        resourceTransport.requests.head.timeout shouldBe Duration.ofMillis(1800L)
      }
    }

    "carry one absolute deadline across token acquisition, 401 refresh, and replay" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4"))
      val tokenRequests = new AtomicInteger(0)
      val tokenTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        ),
        onSend = _ =>
          tokenRequests.incrementAndGet() match {
            case 1 => runtime.advanceMillis(600L)
            case 2 => runtime.advanceMillis(500L)
            case other => fail(s"Unexpected token request number: $other")
          },
      )
      val resourceRequests = new AtomicInteger(0)
      val resourceTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(401, "unauthorized")),
          Right(response(200, "ok-replayed")),
        ),
        onSend = _ =>
          resourceRequests.incrementAndGet() match {
            case 1 => runtime.advanceMillis(900L)
            case 2 => runtime.advanceMillis(200L)
            case other => fail(s"Unexpected resource request number: $other")
          },
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(requestTimeoutMillis = 5000L, maxTotalTimeoutMillis = 3000L, maxRetries = 0),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok-replayed")
        tokenTransport.requests.map(_.timeout).toSeq shouldBe Seq(
          Duration.ofMillis(3000L),
          Duration.ofMillis(1500L),
        )
        resourceTransport.requests.map(_.timeout).toSeq shouldBe Seq(
          Duration.ofMillis(2400L),
          Duration.ofMillis(1000L),
        )
      }
    }

    "refuse to start the resource request when token acquisition exhausts the remaining outer budget" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1"))
      val tokenTransport = new FakeTransport(
        outcomes = Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L)))),
        onSend = _ => runtime.advanceMillis(4000L),
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(
          requestTimeoutMillis = 10000L,
          maxTotalTimeoutMillis = 3000L,
          maxRetries = 0,
        ),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 504
        error.message shouldBe "Total timeout exceeded"
        resourceTransport.requests shouldBe empty
      }
    }

    "refuse to start token acquisition when the remaining outer budget is non-positive" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(
          requestTimeoutMillis = 10000L,
          maxTotalTimeoutMillis = 0L,
          maxRetries = 0,
        ),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 504
        error.message shouldBe "Total timeout exceeded"
        error.requestId shouldBe None
        tokenTransport.requests shouldBe empty
        resourceTransport.requests shouldBe empty
      }
    }

    "defer OAuth-specific HTTP client initialization until the first OAuth use" in {
      val runtime = new FakeRuntime()
      val resourcesFactory = new ThrowingResourcesFactory(new RuntimeException("bad-auth-material"))

      makeClient(resourcesFactory, runtime)

      resourcesFactory.createCalls shouldBe empty
      succeed
    }

    "perform OAuth remote validation by acquiring a token and sending an authenticated _health request" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      client.validateConfiguration().failOnShutdown.map { result =>
        result shouldBe ExtensionValidationResult.Valid
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 1
        resourceTransport.requests.head.headers should contain("Authorization" -> "Bearer token-1")
        resourceTransport.requests.head.headers should contain("X-Daml-External-Function-Id" -> "_health")
      }
    }

    "treat OAuth remote validation as invalid when token acquisition fails" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(401, "bad-client")))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(resourcesFactory, runtime)

      client.validateConfiguration().failOnShutdown.map { result =>
        result shouldBe ExtensionValidationResult.Invalid(
          Seq("OAuth token acquisition failed: Unauthorized: bad-client")
        )
        tokenTransport.requests should have size 1
        resourceTransport.requests shouldBe empty
      }
    }

    "treat OAuth remote validation resource authorization failures as invalid" in {
      Future
        .traverse(Seq(401 -> "Unauthorized", 403 -> "Forbidden")) { case (statusCode, label) =>
          withClue(s"statusCode=$statusCode") {
            val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
            val tokenTransport = new FakeTransport(
              Seq(Right(response(200, tokenResponse(accessToken = s"token-$statusCode", expiresIn = 120L))))
            )
            val resourceTransport = new FakeTransport(
              Seq(Right(response(statusCode, "not-allowed")))
            )
            val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
            val client = makeClient(resourcesFactory, runtime)

            client.validateConfiguration().failOnShutdown.map { result =>
              result shouldBe ExtensionValidationResult.Invalid(
                Seq(s"OAuth validation request failed with $label: not-allowed")
              )
              tokenTransport.requests should have size 1
              resourceTransport.requests should have size 1
              resourceTransport.requests.head.headers should contain(
                "Authorization" -> s"Bearer token-$statusCode"
              )
            }
          }
        }
        .map(_ => succeed)
    }

    "run startup local preflight without outbound HTTP and build one OAuth client assertion" in {
      val runtime = new FakeRuntime()
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val assertionCalls = new AtomicInteger(0)
      val client = makeClient(
        resourcesFactory = resourcesFactory,
        runtime = runtime,
        oauthAssertionFactory = Some(() => {
          assertionCalls.incrementAndGet()
          "startup.jwt.value"
        }),
      )

      client.startupLocalPreflight() shouldBe Right(())
      assertionCalls.get() shouldBe 1
      resourcesFactory.createCalls should have size 1
      tokenTransport.requests shouldBe empty
      resourceTransport.requests shouldBe empty
    }

    "fail startup local preflight on invalid OAuth signing key material before outbound HTTP" in {
      val runtime = new FakeRuntime()
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory = resourcesFactory,
        runtime = runtime,
        oauthAssertionFactory = None,
      )

      val result = client.startupLocalPreflight()

      result.left.value should include("Failed to load RSA private key from DER file")
      resourcesFactory.createCalls should have size 1
      tokenTransport.requests shouldBe empty
      resourceTransport.requests shouldBe empty
    }

    "fail startup local preflight when OAuth resources omit the dedicated token transport" in {
      val runtime = new FakeRuntime()
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))),
          Right(response(200, "unexpected-success")),
        )
      )
      val resourcesFactory = new MissingTokenTransportResourcesFactory(resourceTransport)
      val client = makeClient(resourcesFactory, runtime)

      val result = client.startupLocalPreflight()

      result.left.value should include("requires a dedicated token transport")
      resourcesFactory.createCalls should have size 1
      resourceTransport.requests shouldBe empty
    }

    "fail startup local preflight on invalid OAuth trust material before outbound HTTP" in {
      val runtime = new FakeRuntime()
      val invalidTrustCollectionFile = Paths.get("/definitely/missing-test-trust.pem")
      val client = makeClient(
        resourcesFactory = new JdkHttpExtensionClientResourcesFactory(loggerFactory),
        runtime = runtime,
        config = makeConfig(
          tokenTrustCollectionFile = Some(invalidTrustCollectionFile),
          maxRetries = 0,
        ),
      )

      val result = client.startupLocalPreflight()

      result.left.value should include(invalidTrustCollectionFile.toString)
    }

    "fail before outbound HTTP when OAuth resources omit the dedicated token transport" in {
      val runtime = new FakeRuntime()
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))),
          Right(response(200, "unexpected-success")),
        )
      )
      val resourcesFactory = new MissingTokenTransportResourcesFactory(resourceTransport)
      val client = makeClient(resourcesFactory, runtime, config = makeConfig(maxRetries = 0))

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 500
        error.requestId shouldBe None
        error.message should include("requires a dedicated token transport")
        resourcesFactory.createCalls should have size 1
        resourceTransport.requests shouldBe empty
      }
    }

    "defer OAuth trust-material loading until the first OAuth use and map invalid trust collections before outbound HTTP" in {
      val runtime = new FakeRuntime()
      val invalidTrustCollectionFile = Paths.get("/definitely/missing-test-trust.pem")
      val client = makeClient(
        resourcesFactory = new JdkHttpExtensionClientResourcesFactory(loggerFactory),
        runtime = runtime,
        config = makeConfig(
          tokenTrustCollectionFile = Some(invalidTrustCollectionFile),
          maxRetries = 0,
        ),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 500
        error.requestId shouldBe None
        error.message should include(invalidTrustCollectionFile.toString)
      }
    }

    "reuse loaded trust material and HTTP client state across token reacquisition without rereading the trust files" in {
      val resourceTrustCollectionFile =
        Files.createTempFile("external-call-resource-trust", ".crt")
      val tokenTrustCollectionFile =
        Files.createTempFile("external-call-token-trust", ".crt")
      Files.copy(rootCaFile, resourceTrustCollectionFile, StandardCopyOption.REPLACE_EXISTING)
      Files.copy(rootCaFile, tokenTrustCollectionFile, StandardCopyOption.REPLACE_EXISTING)
      resourceTrustCollectionFile.toFile.deleteOnExit()
      tokenTrustCollectionFile.toFile.deleteOnExit()

      withScriptedHttpsOAuthServer(
        tokenResponses = Seq(
          response(200, tokenResponse(accessToken = "token-1", expiresIn = 1L)),
          response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L)),
        ),
        resourceResponses = Seq(
          response(200, "ok-1"),
          response(200, "ok-2"),
        ),
      ) { server =>
        val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3", "req-4"))
        val client = makeClient(
          resourcesFactory = new JdkHttpExtensionClientResourcesFactory(loggerFactory),
          runtime = runtime,
          config = makeConfig(
            host = "localhost",
            port = server.port,
            tokenHost = "localhost",
            tokenPort = server.port,
            resourceTrustCollectionFile = Some(resourceTrustCollectionFile),
            tokenTrustCollectionFile = Some(tokenTrustCollectionFile),
            maxRetries = 0,
          ),
        )

        client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.flatMap { firstResult =>
          firstResult shouldBe Right("ok-1")
          Files.delete(resourceTrustCollectionFile)
          Files.delete(tokenTrustCollectionFile)
          runtime.advanceMillis(1500L)

          client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown.map { secondResult =>
            secondResult shouldBe Right("ok-2")
            server.tokenRequests.asScala.toSeq should have size 2
            server.resourceRequests.asScala.toSeq should have size 2
          }
        }
      }
    }

    "defer OAuth signing-key loading until the first OAuth use and map missing keys before outbound HTTP" in {
      val runtime = new FakeRuntime()
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)

      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
        oauthAssertionFactory = None,
      )

      resourcesFactory.createCalls shouldBe empty

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 500
        error.requestId shouldBe None
        error.message should include("Failed to load RSA private key from DER file")
        resourcesFactory.createCalls should have size 1
        tokenTransport.requests shouldBe empty
        resourceTransport.requests shouldBe empty
      }
    }

    "log token acquisition failure and the final external-call failure classification" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(503, "issuer-down")))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
      )

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result.isLeft shouldBe true
            result.left.value.statusCode shouldBe 503
            result
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("OAuth token acquisition failed")) shouldBe true
            messages.exists(_.contains("failed with non-retryable error")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "avoid logging private key material when signing-key loading fails" in {
      val runtime = new FakeRuntime()
      val privateKeySentinel = "PRIVATE-KEY-SHOULD-NOT-LEAK"
      val invalidKeyFile = Files.createTempFile("external-call-oauth-invalid-key", ".der")
      Files.write(invalidKeyFile, privateKeySentinel.getBytes("UTF-8"))
      invalidKeyFile.toFile.deleteOnExit()
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "unexpected-token", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(privateKeyFile = invalidKeyFile, maxRetries = 0),
        oauthAssertionFactory = None,
      )

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result.isLeft shouldBe true
            result.left.value.statusCode shouldBe 500
            result
          },
          logs => {
            val messages = logs.map(_.message).mkString("\n")
            messages should not include privateKeySentinel
          },
        )
        .map(_ => succeed)
    }

    "reuse the loaded signing key across token reacquisition without rereading the key file" in {
      val generator = KeyPairGenerator.getInstance("RSA")
      generator.initialize(2048)
      val keyPair = generator.generateKeyPair()
      val privateKeyFile = Files.createTempFile("external-call-oauth-client-service", ".der")
      Files.write(privateKeyFile, keyPair.getPrivate.getEncoded)
      privateKeyFile.toFile.deleteOnExit()

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
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(privateKeyFile = privateKeyFile),
        oauthAssertionFactory = None,
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.flatMap { firstResult =>
        firstResult shouldBe Right("ok-1")
        Files.delete(privateKeyFile)
        runtime.advanceMillis(1500L)

        client.call("echo", "cafebabe", "feedface", "submission").failOnShutdown.map { secondResult =>
          secondResult shouldBe Right("ok-2")
          tokenTransport.requests should have size 2
          resourceTransport.requests should have size 2
        }
      }
    }

    "map lazy OAuth auth-material initialization failure to 500 before any outbound HTTP request is sent" in {
      val runtime = new FakeRuntime()
      val resourcesFactory = new ThrowingResourcesFactory(new RuntimeException("bad-auth-material"))
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 0),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 500
        error.message shouldBe "Unexpected error: bad-auth-material"
        error.requestId shouldBe None
        resourcesFactory.createCalls should have size 1
      }
    }

    "retry a retryable token-endpoint failure through the outer loop and honor Retry-After" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(503, "issuer-down", Map("Retry-After" -> Seq("7")))),
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 1),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok")
        runtime.sleptMillis.toSeq shouldBe Seq(7000L)
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 1
      }
    }

    "retry a retryable token-endpoint failure through the outer loop with exponential backoff when Retry-After is absent" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"), jitter = 0.0)
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(503, "issuer-down")),
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "ok")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 1),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok")
        runtime.sleptMillis.toSeq shouldBe Seq(1000L)
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 1
      }
    }

    "treat a terminal token-endpoint failure as final and do not send the resource request" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1"))
      val tokenTransport = new FakeTransport(
        Seq(Right(response(400, "bad-client-assertion")))
      )
      val resourceTransport = new FakeTransport(
        Seq(Right(response(200, "unexpected-success")))
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 1),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result.isLeft shouldBe true
        val error = result.left.value
        error.statusCode shouldBe 400
        error.message shouldBe "Bad Request: bad-client-assertion"
        error.requestId shouldBe Some("req-1")
        tokenTransport.requests should have size 1
        resourceTransport.requests shouldBe empty
      }
    }

    "treat token-endpoint 401, 403, and 404 responses as terminal through the outer loop" in {
      val terminalStatuses = Seq(
        401 -> "Unauthorized: bad-client",
        403 -> "Forbidden: forbidden-client",
        404 -> "Not found: missing-token-endpoint",
      )

      Future
        .sequence(terminalStatuses.map { case (statusCode, expectedMessage) =>
          val runtime = new FakeRuntime(requestIds = Seq(s"req-$statusCode"))
          val tokenTransport = new FakeTransport(
            Seq(Right(response(statusCode, expectedMessage.split(": ").lastOption.getOrElse(""))))
          )
          val resourceTransport = new FakeTransport(
            Seq(Right(response(200, "unexpected-success")))
          )
          val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
          val client = makeClient(
            resourcesFactory,
            runtime,
            config = makeConfig(maxRetries = 1),
          )

          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result.isLeft shouldBe true
            val error = result.left.value
            error.statusCode shouldBe statusCode
            error.requestId shouldBe Some(s"req-$statusCode")
            resourceTransport.requests shouldBe empty
            tokenTransport.requests should have size 1
            statusCode match {
              case 401 => error.message shouldBe "Unauthorized: bad-client"
              case 403 => error.message shouldBe "Forbidden: forbidden-client"
              case 404 => error.message shouldBe "Not found: missing-token-endpoint"
            }
          }
        })
        .map(_ => succeed)
    }

    "retry token-endpoint 408, 429, 500, 502, 503, and 504 responses through the outer loop" in {
      val retryableStatuses = Seq(408, 429, 500, 502, 503, 504)

      Future
        .sequence(retryableStatuses.map { statusCode =>
          val runtime = new FakeRuntime(
            requestIds = Seq(s"req-$statusCode-1", s"req-$statusCode-2", s"req-$statusCode-3"),
            jitter = 0.0,
          )
          val tokenTransport = new FakeTransport(
            Seq(
              Right(response(statusCode, s"issuer-$statusCode")),
              Right(response(200, tokenResponse(accessToken = s"token-$statusCode", expiresIn = 120L))),
            )
          )
          val resourceTransport = new FakeTransport(
            Seq(Right(response(200, s"ok-$statusCode")))
          )
          val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
          val client = makeClient(
            resourcesFactory,
            runtime,
            config = makeConfig(maxRetries = 1),
          )

          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result shouldBe Right(s"ok-$statusCode")
            runtime.sleptMillis.toSeq shouldBe Seq(1000L)
            tokenTransport.requests should have size 2
            resourceTransport.requests should have size 1
            resourceTransport.requests.head.headers should contain(
              "Authorization" -> s"Bearer token-$statusCode"
            )
          }
        })
        .map(_ => succeed)
    }

    "treat resource 408, 429, 500, 502, 503, and 504 responses as retryable through the outer loop under OAuth" in {
      val retryableStatuses = Seq(408, 429, 500, 502, 503, 504)

      Future
        .sequence(retryableStatuses.map { statusCode =>
          val runtime = new FakeRuntime(
            requestIds = Seq(s"req-$statusCode-1", s"req-$statusCode-2", s"req-$statusCode-3"),
            jitter = 0.0,
          )
          val tokenTransport = new FakeTransport(
            Seq(Right(response(200, tokenResponse(accessToken = s"token-$statusCode", expiresIn = 120L))))
          )
          val resourceTransport = new FakeTransport(
            Seq(
              Right(response(statusCode, s"resource-$statusCode")),
              Right(response(200, s"ok-$statusCode")),
            )
          )
          val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
          val client = makeClient(
            resourcesFactory,
            runtime,
            config = makeConfig(maxRetries = 1),
          )

          client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
            result shouldBe Right(s"ok-$statusCode")
            runtime.sleptMillis.toSeq shouldBe Seq(1000L)
            tokenTransport.requests should have size 1
            resourceTransport.requests should have size 2
            resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
              s"Bearer token-$statusCode",
              s"Bearer token-$statusCode",
            )
          }
        })
        .map(_ => succeed)
    }

    "count only outer retries against maxRetries and still allow auth-local replay on the last outer attempt" in {
      val runtime = new FakeRuntime(
        requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5", "req-6", "req-7"),
        jitter = 0.0,
      )
      val tokenTransport = new FakeTransport(
        Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-3", expiresIn = 120L))),
        )
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(401, "unauthorized-1")),
          Right(response(503, "service-down")),
          Right(response(401, "unauthorized-2")),
          Right(response(200, "ok-after-final-replay")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(maxRetries = 1),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok-after-final-replay")
        runtime.sleptMillis.toSeq shouldBe Seq(1000L)
        tokenTransport.requests should have size 3
        resourceTransport.requests should have size 4
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")) shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
          "Bearer token-2",
          "Bearer token-3",
        )
      }
    }

    "consume total budget across token acquisition, replay, and outer retry backoff" in {
      val runtime = new FakeRuntime(
        requestIds = Seq("req-1", "req-2", "req-3", "req-4", "req-5"),
        jitter = 0.0,
      )
      val tokenRequests = new AtomicInteger(0)
      val tokenTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))),
          Right(response(200, tokenResponse(accessToken = "token-2", expiresIn = 120L))),
        ),
        onSend = _ =>
          tokenRequests.incrementAndGet() match {
            case 1 => runtime.advanceMillis(300L)
            case 2 => runtime.advanceMillis(300L)
            case other => fail(s"Unexpected token request number: $other")
          },
      )
      val resourceRequests = new AtomicInteger(0)
      val resourceTransport = new FakeTransport(
        outcomes = Seq(
          Right(response(401, "unauthorized")),
          Right(response(503, "service-down")),
          Right(response(200, "ok-after-outer-retry")),
        ),
        onSend = _ =>
          resourceRequests.incrementAndGet() match {
            case 1 => runtime.advanceMillis(400L)
            case 2 => runtime.advanceMillis(400L)
            case 3 => ()
            case other => fail(s"Unexpected resource request number: $other")
          },
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(
          requestTimeoutMillis = 600L,
          maxTotalTimeoutMillis = 2600L,
          maxRetries = 1,
        ),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("ok-after-outer-retry")
        runtime.sleptMillis.toSeq shouldBe Seq(1000L)
        tokenTransport.requests should have size 2
        resourceTransport.requests should have size 3
        resourceTransport.requests(2).timeout shouldBe Duration.ofMillis(200L)
        resourceTransport.requests.map(_.headers.toMap.apply("Authorization")).toSeq shouldBe Seq(
          "Bearer token-1",
          "Bearer token-2",
          "Bearer token-2",
        )
      }
    }

    "retry with a clamped timeout when positive budget remains for another outer attempt" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"), jitter = 0.0)
      val tokenTransport = new FakeTransport(
        Seq(Right(response(200, tokenResponse(accessToken = "token-1", expiresIn = 120L))))
      )
      val resourceTransport = new FakeTransport(
        Seq(
          Right(response(503, "service-down")),
          Right(response(200, "unexpected-success")),
        )
      )
      val resourcesFactory = new FakeResourcesFactory(resourceTransport, tokenTransport)
      val client = makeClient(
        resourcesFactory,
        runtime,
        config = makeConfig(
          requestTimeoutMillis = 600L,
          maxTotalTimeoutMillis = 1050L,
          maxRetries = 1,
        ),
      )

      client.call("echo", "cafebabe", "deadbeef", "submission").failOnShutdown.map { result =>
        result shouldBe Right("unexpected-success")
        runtime.sleptMillis.toSeq shouldBe Seq(1000L)
        tokenTransport.requests should have size 1
        resourceTransport.requests should have size 2
        resourceTransport.requests.head.timeout shouldBe Duration.ofMillis(600L)
        resourceTransport.requests(1).timeout shouldBe Duration.ofMillis(50L)
      }
    }
  }
}
