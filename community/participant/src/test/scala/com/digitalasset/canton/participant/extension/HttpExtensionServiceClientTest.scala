// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.sun.net.httpserver.HttpServer
import org.scalatest.wordspec.AnyWordSpec

import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.util.concurrent.atomic.AtomicReference

class HttpExtensionServiceClientTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  "HttpExtensionServiceClient" should {
    "use the configured API version in request paths" in {
      val observedPath = new AtomicReference[String]("")
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
      server.createContext(
        "/",
        exchange => {
          observedPath.set(exchange.getRequestURI.getPath)
          exchange.sendResponseHeaders(200, 0)
          exchange.getResponseBody.close()
        },
      )
      server.start()

      try {
        val config = ExtensionServiceConfig(
          name = "test-extension",
          host = "127.0.0.1",
          port = Port.tryCreate(server.getAddress.getPort),
          version = "v0",
          useTls = false,
        )
        val client = new HttpExtensionServiceClient(
          extensionId = "test-extension",
          config = config,
          sharedHttpClient = HttpClient.newHttpClient(),
          loggerFactory = loggerFactory,
        )

        client
          .validateConfiguration()(TraceContext.empty)
          .failOnShutdown
          .futureValue shouldBe ExtensionValidationResult.Valid
        observedPath.get shouldBe "/api/v0/external-call"
      } finally {
        server.stop(0)
      }
    }
  }
}
