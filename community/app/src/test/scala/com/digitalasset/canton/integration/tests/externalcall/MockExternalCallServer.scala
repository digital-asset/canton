// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import scala.collection.concurrent.TrieMap
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
) extends NamedLogging {

  // Implicit trace context for logging in test infrastructure
  private implicit val traceContext: TraceContext = TraceContext.empty

  private var server: HttpServer = _
  private val handlers = TrieMap[String, ExternalCallRequest => ExternalCallResponse]()
  private val callCounts = TrieMap[String, Int]().withDefaultValue(0)
  private val lastRequests = TrieMap[String, ExternalCallRequest]()

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

  /** Start the mock server */
  def start(): Unit = {
    server = HttpServer.create(new InetSocketAddress(port), 0)
    server.setExecutor(Executors.newFixedThreadPool(4))
    server.createContext("/", new MockHandler)
    server.start()
    logger.info(s"Mock external call server started on port $port")
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
    logger.debug("Mock server reset")
  }

  /** Get the number of times a function was called */
  def getCallCount(functionId: String): Int = callCounts.getOrElse(functionId, 0)

  /** Get the last request for a function */
  def getLastRequest(functionId: String): Option[ExternalCallRequest] = lastRequests.get(functionId)

  /** Get total call count across all functions */
  def getTotalCallCount: Int = callCounts.values.sum

  private class MockHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      Try {
        // Canton sends all external calls to /api/v1/external-call with metadata in headers:
        //   X-Daml-External-Function-Id: the function identifier
        //   X-Daml-External-Config-Hash: the config hash
        //   X-Daml-External-Mode: "submission" or "validation"
        //   Body: hex-encoded input as text

        // Extract headers
        val headers = exchange.getRequestHeaders
        val functionIdOpt = Option(headers.getFirst("X-Daml-External-Function-Id"))
        val configHashOpt = Option(headers.getFirst("X-Daml-External-Config-Hash"))

        functionIdOpt match {
          case None =>
            sendResponse(exchange, 400, "Missing X-Daml-External-Function-Id header")
          case Some(functionId) =>
          val extensionId = "test-ext" // Extension ID is determined by config, not sent in request

          // Read request body
          val inputStream = exchange.getRequestBody
          val input = inputStream.readAllBytes()
          inputStream.close()

          val mode = Option(headers.getFirst("X-Daml-External-Mode")).getOrElse("submission")
          val participantId = Option(headers.getFirst("X-Participant-Id"))
          val requestId = Option(headers.getFirst("X-Request-Id"))

          // Config hash from header
          val config = configHashOpt.map(_.getBytes(StandardCharsets.UTF_8)).getOrElse(Array.emptyByteArray)

          val request = ExternalCallRequest(
            extensionId = extensionId,
            functionId = functionId,
            config = config,
            input = input,
            mode = mode,
            participantId = participantId,
            requestId = requestId,
          )

          // Track the call
          callCounts.updateWith(functionId) {
            case Some(count) => Some(count + 1)
            case None => Some(1)
          }
          lastRequests.put(functionId, request)

          logger.debug(
            s"Received request: extensionId=$extensionId, functionId=$functionId, " +
              s"mode=$mode, participantId=$participantId, inputSize=${input.length}"
          )

          // Find and invoke handler
          handlers.get(functionId) match {
            case Some(handler) =>
              val response = handler(request)
              response.headers.foreach { case (k, v) =>
                exchange.getResponseHeaders.add(k, v)
              }
              sendResponse(exchange, response.statusCode, response.body)

            case None =>
              logger.warn(s"No handler for function: $functionId")
              sendResponse(exchange, 404, s"Unknown function: $functionId")
          }
        }
      } match {
        case Success(_) => // Response already sent
        case Failure(ex) =>
          logger.error(s"Error handling request: ${ex.getMessage}", ex)
          Try(sendResponse(exchange, 500, s"Internal error: ${ex.getMessage}"))
      }
    }

    private def sendResponse(exchange: HttpExchange, statusCode: Int, body: String): Unit = {
      sendResponse(exchange, statusCode, body.getBytes(StandardCharsets.UTF_8))
    }

    private def sendResponse(exchange: HttpExchange, statusCode: Int, body: Array[Byte]): Unit = {
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
