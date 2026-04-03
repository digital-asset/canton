// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.participant.extension.HttpExtensionResponseMapper.FailureProfile

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
      case _ =>
        Left(mapHttpFailure(response, requestId, FailureProfile.Resource))
    }

  def mapHttpFailure(
      response: HttpExtensionClientResponse,
      requestId: String,
      profile: FailureProfile,
  ): ExtensionCallError = {
    val defaultLabel = defaultMessage(response.statusCode, profile)

    response.statusCode match {
      case 429 | 503 =>
        parseErrorResponseWithRetry(response, requestId, defaultLabel)
      case _ =>
        parseErrorResponse(response, requestId, defaultLabel)
    }
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

  def responseBodyOrDefault(
      response: HttpExtensionClientResponse,
      defaultMessage: String,
  ): String =
    compactBody(response).getOrElse(defaultMessage)

  private def parseErrorResponse(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallError = {
    val message =
      compactBody(response).fold(defaultMessage)(body => s"$defaultMessage: $body")
    ExtensionCallError(response.statusCode, message, Some(requestId))
  }

  private def parseErrorResponseWithRetry(
      response: HttpExtensionClientResponse,
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallErrorWithRetry = {
    val retryAfter = firstHeaderValue(response, "Retry-After").flatMap(s => Try(s.toInt).toOption)

    val message =
      compactBody(response).fold(defaultMessage)(body => s"$defaultMessage: $body")

    ExtensionCallErrorWithRetry(response.statusCode, message, Some(requestId), retryAfter)
  }

  private def defaultMessage(statusCode: Int, profile: FailureProfile): String =
    (statusCode, profile) match {
      case (400, _) => "Bad Request"
      case (401, _) => "Unauthorized"
      case (403, FailureProfile.Resource) => "Forbidden - insufficient permissions"
      case (403, FailureProfile.OAuthTokenEndpoint) => "Forbidden"
      case (404, FailureProfile.Resource) => "Function not found"
      case (404, FailureProfile.OAuthTokenEndpoint) => "Not found"
      case (408, _) => "Request timeout"
      case (429, _) => "Rate limit exceeded"
      case (500, _) => "Internal server error"
      case (502, _) => "Bad gateway"
      case (503, _) => "Service unavailable"
      case (504, _) => "Gateway timeout"
      case (code, _) => s"HTTP $code"
    }

  private def compactBody(response: HttpExtensionClientResponse): Option[String] =
    Option.when(response.body.nonEmpty && response.body.length < 500)(response.body)

  private def firstHeaderValue(
      response: HttpExtensionClientResponse,
      headerName: String,
  ): Option[String] =
    response.headers.iterator.collectFirst {
      case (name, values) if name.equalsIgnoreCase(headerName) => values.headOption
    }.flatten
}

private[extension] object HttpExtensionResponseMapper {
  sealed trait FailureProfile extends Product with Serializable

  object FailureProfile {
    case object Resource extends FailureProfile
    case object OAuthTokenEndpoint extends FailureProfile
  }
}
