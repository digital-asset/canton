// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Duration

private[extension] object HttpExtensionOAuthTokenRequestBuilder {
  private val ClientAssertionType =
    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"

  private def formEncode(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8)
}

private[extension] final class HttpExtensionOAuthTokenRequestBuilder(
    config: ExtensionServiceConfig
) {
  import HttpExtensionOAuthTokenRequestBuilder.*

  private val oauthConfig = config.auth match {
    case oauth: ExtensionServiceAuthConfig.OAuth => oauth
    case other =>
      throw new IllegalArgumentException(
        s"OAuth token request builder requires auth.type = oauth, but got $other"
      )
  }

  def buildTokenRequest(
      timeout: Duration,
      requestId: String,
      clientAssertion: String,
  ): HttpExtensionClientRequest = {
    val formFields = Seq(
      "grant_type" -> "client_credentials",
      "client_assertion_type" -> ClientAssertionType,
      "client_assertion" -> clientAssertion,
    ) ++ oauthConfig.scope.toList.map("scope" -> _)

    val requestBody = formFields
      .map { case (name, value) =>
        s"${formEncode(name)}=${formEncode(value)}"
      }
      .mkString("&")

    HttpExtensionClientRequest(
      uri = oauthConfig.tokenEndpoint.uri,
      timeout = timeout,
      headers = Seq(
        "Content-Type" -> "application/x-www-form-urlencoded",
        config.requestIdHeader -> requestId,
      ),
      body = requestBody,
    )
  }
}
