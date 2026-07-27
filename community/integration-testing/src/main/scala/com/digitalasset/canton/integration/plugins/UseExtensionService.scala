// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import monocle.macros.syntax.lens.*

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import scala.jdk.CollectionConverters.*

/** Runs a mock extension service over HTTP and configures it on every participant under
  * `parameters.engine.extensions`, so that integration tests can exercise Daml external calls
  * end-to-end. The mock serves the external-call endpoint and the version endpoint of the
  * extension-service API, records every external call it receives, and answers with
  * [[UseExtensionService.defaultResponseHex]] unless a test installs its own responder via
  * [[respondWith]].
  */
class UseExtensionService(
    protected val loggerFactory: NamedLoggerFactory,
    val extensionId: String = "test-extension",
) extends EnvironmentSetupPlugin {
  import UseExtensionService.*

  private val port = UniquePortGenerator.next

  private val calls = new CopyOnWriteArrayList[RecordedCall]

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var responder: RecordedCall => Response = defaultResponder

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var server: HttpServer = _

  /** The external calls received so far, in arrival order. */
  def observedCalls: Seq[RecordedCall] = calls.asScala.toSeq

  /** Install a responder for subsequent external calls; use [[reset]] to restore the default. */
  def respondWith(newResponder: RecordedCall => Response): Unit = responder = newResponder

  /** Forget the recorded calls and restore the default responder. */
  def reset(): Unit = {
    calls.clear()
    responder = defaultResponder
  }

  override def beforeTests(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", port.unwrap), 0)
    server.createContext("/", (exchange: HttpExchange) => handle(exchange))
    server.start()
  }

  override def afterTests(): Unit =
    server.stop(0)

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.parameters.engine.extensions)
        .modify(
          _ + (extensionId -> ExtensionServiceConfig(
            address = "127.0.0.1",
            port = port,
            // No retries, so that failure scenarios observe a deterministic number of calls.
            maxRetries = NonNegativeInt.zero,
          ))
        )
    )(config)

  private def handle(exchange: HttpExchange): Unit = {
    val path = exchange.getRequestURI.getPath
    (exchange.getRequestMethod, path) match {
      case ("GET", VersionEndpoint()) =>
        respond(exchange, Response(200, "{}"))
      case ("POST", ExternalCallEndpoint()) =>
        val headers = exchange.getRequestHeaders
        def header(name: String): String = Option(headers.getFirst(name)).getOrElse("")
        val call = RecordedCall(
          path = path,
          mode = header("X-Daml-External-Mode"),
          functionId = header("X-Daml-External-Function-Id"),
          configHash = header("X-Daml-External-Config-Hash"),
          externalCallId = header("X-Request-Id"),
          idempotencyKey = header("Idempotency-Key"),
          body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8),
        )
        calls.add(call).discard
        respond(exchange, responder(call))
      case _ =>
        respond(exchange, Response(404, ""))
    }
  }

  private def respond(exchange: HttpExchange, response: Response): Unit = {
    val bytes = response.body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(response.status, bytes.length.toLong)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }
}

object UseExtensionService {

  /** The hex-encoded output the mock returns unless a test installs its own responder. */
  val defaultResponseHex: String = "c0ffee"

  private val defaultResponder: RecordedCall => Response = _ => Response(200, defaultResponseHex)

  private val VersionEndpoint = raw"/api/[^/]+/version".r
  private val ExternalCallEndpoint = raw"/api/[^/]+/external-call".r

  /** One external call as received by the mock service, header values empty when absent. */
  final case class RecordedCall(
      path: String,
      mode: String,
      functionId: String,
      configHash: String,
      externalCallId: String,
      idempotencyKey: String,
      body: String,
  )

  final case class Response(status: Int, body: String)
}
