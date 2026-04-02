// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer, HttpsConfigurator, HttpsServer}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, KeyStore, SecureRandom}
import java.util.Base64
import java.util.concurrent.Executors
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scala.jdk.CollectionConverters.*
import scala.collection.concurrent.TrieMap
import scala.util.Using
import scala.util.{Failure, Success, Try}

/** Request data for external call handlers */
final case class ExternalCallRequest(
    extensionId: String,
    functionId: String,
    config: Array[Byte],
    input: Array[Byte],
    mode: String, // "submission" or "validation"
    participantId: Option[String],
    requestId: Option[String],
    headers: Map[String, Seq[String]],
)

/** Request data for OAuth token-endpoint handlers */
final case class TokenEndpointRequest(
    method: String,
    path: String,
    body: String,
    headers: Map[String, Seq[String]],
    requestId: Option[String],
)

/** Response data from external call handlers */
final case class ExternalCallResponse(
    statusCode: Int,
    body: Array[Byte],
    headers: Map[String, String] = Map.empty,
)

object ExternalCallResponse {
  def ok(body: Array[Byte]): ExternalCallResponse = ExternalCallResponse(200, body)
  def ok(body: String): ExternalCallResponse = ExternalCallResponse(200, body.getBytes(StandardCharsets.UTF_8))
  def error(statusCode: Int, message: String): ExternalCallResponse =
    ExternalCallResponse(statusCode, message.getBytes(StandardCharsets.UTF_8))
}

/** A mock HTTP server for testing external call functionality.
  *
  * Provides a configurable HTTP server that can simulate various external service behaviors
  * including success responses, errors, delays, and participant-specific responses.
  */
class MockExternalCallServer(
    port: Int,
    override protected val loggerFactory: NamedLoggerFactory,
    useTls: Boolean = false,
    tokenEndpointPath: Option[String] = None,
) extends NamedLogging {

  // Implicit trace context for logging in test infrastructure
  private implicit val traceContext: TraceContext = TraceContext.empty

  private var server: HttpServer = _
  private val handlers = TrieMap[String, ExternalCallRequest => ExternalCallResponse]()
  private val callCounts = TrieMap[String, Int]().withDefaultValue(0)
  private val lastRequests = TrieMap[String, ExternalCallRequest]()
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var tokenHandler: Option[TokenEndpointRequest => ExternalCallResponse] = None
  private val tokenCallCount = new java.util.concurrent.atomic.AtomicInteger(0)
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lastTokenRequest: Option[TokenEndpointRequest] = None

  /** Set a handler for a specific function ID */
  def setHandler(functionId: String)(handler: ExternalCallRequest => ExternalCallResponse): Unit = {
    handlers.put(functionId, handler)
    logger.debug(s"Set handler for function: $functionId")
  }

  /** Set an echo handler that returns the input as output */
  def setEchoHandler(functionId: String): Unit = {
    setHandler(functionId)(req => ExternalCallResponse.ok(req.input))
  }

  /** Set an error handler that always returns the specified status code */
  def setErrorHandler(functionId: String, statusCode: Int, message: String = "Error"): Unit = {
    setHandler(functionId)(_ => ExternalCallResponse.error(statusCode, message))
  }

  /** Set a handler that returns different results based on participant ID */
  def setParticipantSpecificHandler(functionId: String)(
      handler: (String, ExternalCallRequest) => ExternalCallResponse
  ): Unit = {
    setHandler(functionId) { req =>
      val participantId = req.participantId.getOrElse("unknown")
      handler(participantId, req)
    }
  }

  /** Set a handler that fails N times before succeeding */
  def setRetryHandler(functionId: String, failuresBeforeSuccess: Int): Unit = {
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    setHandler(functionId) { req =>
      if (counter.incrementAndGet() <= failuresBeforeSuccess) {
        ExternalCallResponse.error(503, "Service temporarily unavailable")
      } else {
        ExternalCallResponse.ok(req.input)
      }
    }
  }

  /** Set a handler for the OAuth token endpoint */
  def setTokenHandler(handler: TokenEndpointRequest => ExternalCallResponse): Unit = {
    tokenHandler = Some(handler)
    logger.debug("Set token handler")
  }

  /** Set a token handler that returns a successful OAuth access token response */
  def setTokenSuccessHandler(
      accessToken: String,
      expiresIn: Long = 120L,
      tokenType: String = "Bearer",
  ): Unit =
    setTokenHandler { _ =>
      ExternalCallResponse(
        statusCode = 200,
        body =
          s"""{"access_token":"$accessToken","token_type":"$tokenType","expires_in":$expiresIn}"""
            .getBytes(StandardCharsets.UTF_8),
        headers = Map("Content-Type" -> "application/json"),
      )
    }

  /** Set a token handler that returns a fixed error response */
  def setTokenErrorHandler(
      statusCode: Int,
      message: String,
      headers: Map[String, String] = Map.empty,
  ): Unit =
    setTokenHandler { _ =>
      ExternalCallResponse(
        statusCode = statusCode,
        body = message.getBytes(StandardCharsets.UTF_8),
        headers = headers,
      )
    }

  /** Start the mock server */
  def start(): Unit = {
    server =
      if (useTls) {
        val httpsServer = HttpsServer.create(new InetSocketAddress(port), 0)
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(buildServerSslContext()))
        httpsServer
      } else {
        HttpServer.create(new InetSocketAddress(port), 0)
      }
    server.setExecutor(Executors.newFixedThreadPool(4))
    server.createContext("/", new MockHandler)
    server.start()
    logger.info(
      s"Mock external call server started on port $port (${if (useTls) "https" else "http"})"
    )
  }

  /** Stop the mock server */
  def stop(): Unit = {
    if (server != null) {
      server.stop(0)
      logger.info("Mock external call server stopped")
    }
  }

  /** Reset all handlers and counters */
  def reset(): Unit = {
    handlers.clear()
    callCounts.clear()
    lastRequests.clear()
    tokenHandler = None
    tokenCallCount.set(0)
    lastTokenRequest = None
    logger.debug("Mock server reset")
  }

  /** Get the number of times a function was called */
  def getCallCount(functionId: String): Int = callCounts.getOrElse(functionId, 0)

  /** Get the last request for a function */
  def getLastRequest(functionId: String): Option[ExternalCallRequest] = lastRequests.get(functionId)

  /** Get total call count across all functions */
  def getTotalCallCount: Int = callCounts.values.sum

  /** Get the number of OAuth token-endpoint calls */
  def getTokenCallCount: Int = tokenCallCount.get()

  /** Get the last OAuth token-endpoint request */
  def getLastTokenRequest: Option[TokenEndpointRequest] = lastTokenRequest

  private def buildServerSslContext(): SSLContext = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val serverCertificate = Using.resource(
      Files.newInputStream(Paths.get(BaseTest.getResourcePath("tls/public-api.crt")))
    ) { inputStream =>
      certificateFactory.generateCertificate(inputStream).asInstanceOf[X509Certificate]
    }
    val privateKey = loadPkcs8PrivateKey(Paths.get(BaseTest.getResourcePath("tls/public-api.pem")))
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(null, null)
    keyStore.setKeyEntry("server", privateKey, Array.emptyCharArray, Array(serverCertificate))

    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(keyStore, Array.emptyCharArray)

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, null, new SecureRandom())
    sslContext
  }

  private def loadPkcs8PrivateKey(path: java.nio.file.Path) = {
    val pem = Files.readString(path, StandardCharsets.US_ASCII)
    val encoded = pem.linesIterator
      .filterNot(line => line.startsWith("-----BEGIN") || line.startsWith("-----END"))
      .mkString
    val keySpec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder.decode(encoded))
    KeyFactory.getInstance("RSA").generatePrivate(keySpec)
  }

  private class MockHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      Try {
        if (tokenEndpointPath.contains(exchange.getRequestURI.getPath)) {
          handleTokenRequest(exchange)
        } else {
          handleExternalCallRequest(exchange)
        }
      } match {
        case Success(_) => // Response already sent
        case Failure(ex) =>
          logger.error(s"Error handling request: ${ex.getMessage}", ex)
          Try(sendResponse(exchange, 500, s"Internal error: ${ex.getMessage}"))
      }
    }

    private def handleTokenRequest(exchange: HttpExchange): Unit = {
      val body = Using.resource(exchange.getRequestBody)(_.readAllBytes())
      val headers = exchange.getRequestHeaders.asScala.iterator.map { case (name, values) =>
        name -> values.asScala.toSeq
      }.toMap
      val request = TokenEndpointRequest(
        method = exchange.getRequestMethod,
        path = exchange.getRequestURI.getPath,
        body = new String(body, StandardCharsets.UTF_8),
        headers = headers,
        requestId = Option(exchange.getRequestHeaders.getFirst("X-Request-Id")),
      )

      tokenCallCount.incrementAndGet()
      lastTokenRequest = Some(request)
      logger.debug(
        s"Received token request: method=${request.method}, path=${request.path}, requestId=${request.requestId.getOrElse("none")}"
      )

      tokenHandler match {
        case Some(handler) =>
          val response = handler(request)
          sendResponse(exchange, response.statusCode, response.body, response.headers)
        case None =>
          logger.warn(s"No token handler configured for path ${request.path}")
          sendResponse(exchange, 404, s"Unknown token endpoint: ${request.path}")
      }
    }

    private def handleExternalCallRequest(exchange: HttpExchange): Unit = {
      // Canton sends all external calls to /api/v1/external-call with metadata in headers:
      //   X-Daml-External-Function-Id: the function identifier
      //   X-Daml-External-Config-Hash: the config hash
      //   X-Daml-External-Mode: "submission" or "validation"
      //   Body: hex-encoded input as text
      val headers = exchange.getRequestHeaders
      val functionIdOpt = Option(headers.getFirst("X-Daml-External-Function-Id"))
      val configHashOpt = Option(headers.getFirst("X-Daml-External-Config-Hash"))

      functionIdOpt match {
        case None =>
          sendResponse(exchange, 400, "Missing X-Daml-External-Function-Id header")

        case Some(functionId) =>
          val extensionId = "test-ext"
          val input = Using.resource(exchange.getRequestBody)(_.readAllBytes())
          val mode = Option(headers.getFirst("X-Daml-External-Mode")).getOrElse("submission")
          val participantId = Option(headers.getFirst("X-Participant-Id"))
          val requestId = Option(headers.getFirst("X-Request-Id"))
          val config =
            configHashOpt.map(_.getBytes(StandardCharsets.UTF_8)).getOrElse(Array.emptyByteArray)

          val request = ExternalCallRequest(
            extensionId = extensionId,
            functionId = functionId,
            config = config,
            input = input,
            mode = mode,
            participantId = participantId,
            requestId = requestId,
            headers = headers.asScala.iterator.map { case (name, values) =>
              name -> values.asScala.toSeq
            }.toMap,
          )

          callCounts.updateWith(functionId) {
            case Some(count) => Some(count + 1)
            case None => Some(1)
          }
          lastRequests.put(functionId, request)

          logger.debug(
            s"Received request: extensionId=$extensionId, functionId=$functionId, mode=$mode, participantId=$participantId, inputSize=${input.length}"
          )

          handlers.get(functionId) match {
            case Some(handler) =>
              val response = handler(request)
              sendResponse(exchange, response.statusCode, response.body, response.headers)

            case None =>
              logger.warn(s"No handler for function: $functionId")
              sendResponse(exchange, 404, s"Unknown function: $functionId")
          }
      }
    }

    private def sendResponse(exchange: HttpExchange, statusCode: Int, body: String): Unit = {
      sendResponse(exchange, statusCode, body.getBytes(StandardCharsets.UTF_8))
    }

    private def sendResponse(exchange: HttpExchange, statusCode: Int, body: Array[Byte]): Unit = {
      sendResponse(exchange, statusCode, body, Map.empty)
    }

    private def sendResponse(
        exchange: HttpExchange,
        statusCode: Int,
        body: Array[Byte],
        headers: Map[String, String],
    ): Unit = {
      headers.foreach { case (name, value) =>
        exchange.getResponseHeaders.add(name, value)
      }
      exchange.sendResponseHeaders(statusCode, body.length.toLong)
      val os = exchange.getResponseBody
      os.write(body)
      os.close()
    }
  }
}

object MockExternalCallServer {
  /** Find a free port for the mock server */
  def findFreePort(): Int = {
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
