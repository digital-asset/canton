// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.participant.config.ExtensionServiceConfig

import java.net.URI
import java.nio.file.Files
import java.time.Duration
import scala.util.Try

private[extension] final class HttpExtensionRequestBuilder(config: ExtensionServiceConfig) {

  private val scheme = if (config.useTls) "https" else "http"
  private val endpoint: URI = URI.create(s"$scheme://${config.host}:${config.port}/api/v1/external-call")

  private lazy val jwtToken: Option[String] = {
    config.jwt.orElse {
      config.jwtFile.flatMap { path =>
        Try {
          new String(Files.readAllBytes(path)).trim
        }.toOption
      }
    }
  }

  def buildCallRequest(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
      timeout: Duration,
      requestId: String,
  ): HttpExtensionClientRequest = {
    val baseHeaders = Seq(
      "Content-Type" -> "application/octet-stream",
      "X-Daml-External-Function-Id" -> functionId,
      "X-Daml-External-Config-Hash" -> configHash,
      "X-Daml-External-Mode" -> mode,
      config.requestIdHeader -> requestId,
    )
    val authHeaders = jwtToken.toList.map(token => "Authorization" -> s"Bearer $token")

    HttpExtensionClientRequest(
      uri = endpoint,
      timeout = timeout,
      headers = baseHeaders ++ authHeaders,
      body = input,
    )
  }

  def buildValidationRequest(
      timeout: Duration,
      requestId: String,
  ): HttpExtensionClientRequest =
    buildCallRequest(
      functionId = "_health",
      configHash = "",
      input = "",
      mode = "validation",
      timeout = timeout,
      requestId = requestId,
    )
}
