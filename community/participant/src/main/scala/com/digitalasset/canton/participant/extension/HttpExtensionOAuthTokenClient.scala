// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.participant.extension.HttpExtensionResponseMapper.FailureProfile

import java.time.Duration
import scala.util.Try

private[extension] final class HttpExtensionOAuthTokenClient(
    transport: HttpExtensionClientTransport,
    requestBuilder: HttpExtensionOAuthTokenRequestBuilder,
    buildClientAssertion: () => String,
    responseParser: HttpExtensionOAuthTokenResponseParser,
    nowMillis: () => Long,
    responseMapper: HttpExtensionResponseMapper = new HttpExtensionResponseMapper,
) {

  def acquireToken(
      timeout: Duration,
      requestId: String,
  ): Either[ExtensionCallError, HttpExtensionOAuthAccessToken] = {
    val requestOrError: Either[ExtensionCallError, HttpExtensionClientRequest] =
      Try {
        val clientAssertion = buildClientAssertion()
        requestBuilder.buildTokenRequest(
          timeout = timeout,
          requestId = requestId,
          clientAssertion = clientAssertion,
        )
      }.toEither.left.map(error =>
        ExtensionCallError(
          500,
          s"OAuth token acquisition setup failed: ${error.getMessage}",
          None,
        )
      )

    requestOrError.flatMap { request =>
      try {
        val response = transport.send(request)
        if (response.statusCode == 200) {
          responseParser.parse(response, requestId, nowMillis())
        } else {
          Left(responseMapper.mapHttpFailure(response, requestId, FailureProfile.OAuthTokenEndpoint))
        }
      } catch {
        case error: Exception =>
          Left(responseMapper.mapException(error, requestId))
      }
    }
  }
}
