// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

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
          Left(mapTokenEndpointFailure(response, requestId))
        }
      } catch {
        case error: Exception =>
          Left(responseMapper.mapException(error, requestId))
      }
    }
  }

  private def mapTokenEndpointFailure(
      response: HttpExtensionClientResponse,
      requestId: String,
  ): ExtensionCallError =
    response.statusCode match {
      case 400 => simpleHttpFailure(response, requestId, "Bad Request")
      case 401 => simpleHttpFailure(response, requestId, "Unauthorized")
      case 403 => simpleHttpFailure(response, requestId, "Forbidden")
      case 404 => simpleHttpFailure(response, requestId, "Not found")
      case 408 => simpleHttpFailure(response, requestId, "Request timeout")
      case 429 => retryableHttpFailure(response, requestId, "Rate limit exceeded")
      case 500 => simpleHttpFailure(response, requestId, "Internal server error")
      case 502 => simpleHttpFailure(response, requestId, "Bad gateway")
      case 503 => retryableHttpFailure(response, requestId, "Service unavailable")
      case 504 => simpleHttpFailure(response, requestId, "Gateway timeout")
      case code => simpleHttpFailure(response, requestId, s"HTTP $code")
    }

  private def simpleHttpFailure(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallError =
    ExtensionCallError(
      statusCode = response.statusCode,
      message = responseMessage(response, defaultMessage),
      requestId = Some(requestId),
    )

  private def retryableHttpFailure(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallErrorWithRetry =
    ExtensionCallErrorWithRetry(
      statusCode = response.statusCode,
      message = responseMessage(response, defaultMessage),
      requestId = Some(requestId),
      retryAfterSeconds = firstHeaderValue(response, "Retry-After").flatMap(value =>
        Try(value.toInt).toOption
      ),
    )

  private def responseMessage(
      response: HttpExtensionClientResponse,
      defaultMessage: String,
  ): String =
    if (response.body.nonEmpty && response.body.length < 500) {
      s"$defaultMessage: ${response.body}"
    } else {
      defaultMessage
    }

  private def firstHeaderValue(
      response: HttpExtensionClientResponse,
      headerName: String,
  ): Option[String] =
    response.headers.iterator.collectFirst {
      case (name, values) if name.equalsIgnoreCase(headerName) => values.headOption
    }.flatten
}
