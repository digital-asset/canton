// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.concurrent.duration.*
import scala.util.Try

/** Honours an optional, client-supplied per-request timeout carried in the `Request-Timeout` header
  * (value expressed in milliseconds).
  *
  * The header value is validated against a `[lowerBound; upperBound]` window, configurable via
  * [[ClientRequestTimeoutConfig.lowerBound]] and [[ClientRequestTimeoutConfig.upperBound]]
  * (defaulting to [[JsonApiConfig.defaultRequestTimeoutLowerBound]] and
  * [[JsonApiConfig.defaultRequestTimeoutUpperBound]]):
  *   - no header -> the server default request timeout ([[JsonApiConfig.requestTimeout]]) applies,
  *     unchanged, and Pekko's default timeout response (503) is used;
  *   - value below the lower bound -> `400 Bad Request`, the request is not processed;
  *   - value above the upper bound -> `400 Bad Request`, the request is not processed;
  *   - value within the window -> used as supplied; if the request exceeds it a `408 Request
  *     Timeout` is returned.
  *
  * A header that cannot be parsed as a positive integer number of milliseconds is rejected with
  * `400 Bad Request`, like a value below the lower bound.
  */
object RequestTimeoutDirective {

  val RequestTimeoutHeaderName: String = "Request-Timeout"

  private val timeoutResponse: HttpRequest => HttpResponse = _ =>
    HttpResponse(StatusCodes.RequestTimeout)

  private[http] sealed trait Validated extends Product with Serializable
  private[http] object Validated {

    case object Invalid extends Validated

    case object BelowLowerBound extends Validated

    case object AboveUpperBound extends Validated

    final case class Enforce(timeout: FiniteDuration) extends Validated
  }

  private[http] def validate(
      rawHeaderValue: String,
      lowerBound: FiniteDuration = JsonApiConfig.defaultRequestTimeoutLowerBound,
      upperBound: FiniteDuration = JsonApiConfig.defaultRequestTimeoutUpperBound,
  ): Validated =
    Try(rawHeaderValue.trim.toLong).toOption match {
      case None => Validated.Invalid
      case Some(millis) if millis < lowerBound.toMillis => Validated.BelowLowerBound
      case Some(millis) if millis > upperBound.toMillis => Validated.AboveUpperBound
      case Some(millis) => Validated.Enforce(millis.millis)
    }

  def apply(
      inner: Route,
      lowerBound: FiniteDuration = JsonApiConfig.defaultRequestTimeoutLowerBound,
      upperBound: FiniteDuration = JsonApiConfig.defaultRequestTimeoutUpperBound,
  ): Route =
    optionalHeaderValueByName(RequestTimeoutHeaderName) {
      case None =>
        inner
      case Some(rawHeaderValue) =>
        validate(rawHeaderValue, lowerBound, upperBound) match {
          case Validated.Invalid =>
            complete(
              StatusCodes.BadRequest,
              s"Invalid $RequestTimeoutHeaderName header value '$rawHeaderValue': " +
                "expected a positive integer number of milliseconds.",
            )
          case Validated.BelowLowerBound =>
            complete(
              StatusCodes.BadRequest,
              s"$RequestTimeoutHeaderName '$rawHeaderValue' ms is below the minimum of " +
                s"${lowerBound.toMillis} ms.",
            )
          case Validated.AboveUpperBound =>
            complete(
              StatusCodes.BadRequest,
              s"$RequestTimeoutHeaderName '$rawHeaderValue' ms is above the maximum of " +
                s"${upperBound.toMillis} ms.",
            )
          case Validated.Enforce(timeout) =>
            withRequestTimeout(timeout, timeoutResponse)(inner)
        }
    }
}
