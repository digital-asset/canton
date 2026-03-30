// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import java.net.ConnectException
import java.net.http.HttpTimeoutException
import scala.util.Try

private[extension] final class HttpExtensionResponseMapper {

  def mapResponse(
      response: HttpExtensionClientResponse,
      requestId: String,
  ): Either[ExtensionCallError, String] =
    response.statusCode match {
      case 200 =>
        Right(response.body)

      case 400 =>
        Left(parseErrorResponse(response, requestId, "Bad Request"))

      case 401 =>
        Left(parseErrorResponse(response, requestId, "Unauthorized"))

      case 403 =>
        Left(parseErrorResponse(response, requestId, "Forbidden - insufficient permissions"))

      case 404 =>
        Left(parseErrorResponse(response, requestId, "Function not found"))

      case 408 =>
        Left(parseErrorResponse(response, requestId, "Request timeout"))

      case 429 =>
        Left(parseErrorResponseWithRetry(response, requestId, "Rate limit exceeded"))

      case 500 =>
        Left(parseErrorResponse(response, requestId, "Internal server error"))

      case 502 =>
        Left(parseErrorResponse(response, requestId, "Bad gateway"))

      case 503 =>
        Left(parseErrorResponseWithRetry(response, requestId, "Service unavailable"))

      case 504 =>
        Left(parseErrorResponse(response, requestId, "Gateway timeout"))

      case code =>
        Left(parseErrorResponse(response, requestId, s"HTTP $code"))
    }

  def mapException(exception: Exception, requestId: String): ExtensionCallError =
    exception match {
      case e: HttpTimeoutException =>
        ExtensionCallError(408, s"Request timeout: ${e.getMessage}", Some(requestId))

      case e: ConnectException =>
        ExtensionCallError(503, s"Connection failed: ${e.getMessage}", Some(requestId))

      case e: java.io.IOException =>
        ExtensionCallError(503, s"I/O error: ${e.getMessage}", Some(requestId))

      case e: Exception =>
        ExtensionCallError(500, s"Unexpected error: ${e.getMessage}", Some(requestId))
    }

  def retryAfter(error: ExtensionCallError): Option[Int] = error match {
    case e: ExtensionCallErrorWithRetry => e.retryAfterSeconds
    case _ => None
  }

  private def parseErrorResponse(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallError = {
    val body = response.body
    val message = if (body.nonEmpty && body.length < 500) {
      s"$defaultMessage: $body"
    } else {
      defaultMessage
    }
    ExtensionCallError(response.statusCode, message, Some(requestId))
  }

  private def parseErrorResponseWithRetry(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallErrorWithRetry = {
    val retryAfter = firstHeaderValue(response, "Retry-After").flatMap(s => Try(s.toInt).toOption)

    val body = response.body
    val message = if (body.nonEmpty && body.length < 500) {
      s"$defaultMessage: $body"
    } else {
      defaultMessage
    }

    ExtensionCallErrorWithRetry(response.statusCode, message, Some(requestId), retryAfter)
  }

  private def firstHeaderValue(
      response: HttpExtensionClientResponse,
      headerName: String,
  ): Option[String] =
    response.headers.iterator.collectFirst {
      case (name, values) if name.equalsIgnoreCase(headerName) => values.headOption
    }.flatten
}
