// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import scala.concurrent.duration.*
import scala.util.Try

/** Honours an optional, client-supplied per-request timeout carried in the `Request-Timeout` header.
  *
  * The value is either a bare integer, interpreted as milliseconds (e.g. `5000`), or a duration with
  * a unit as accepted by `scala.concurrent.duration.Duration(String)` (e.g. `5s`, `1500ms`, `1.5s`,
  * `2 minutes`).
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
  * A header that cannot be parsed as either form, or that denotes an infinite duration, is rejected
  * with `400 Bad Request`, like a value below the lower bound.
  */
object RequestTimeoutDirective {

  val RequestTimeoutHeaderName: String = "Request-Timeout"

  /** A bare integer, interpreted as milliseconds (e.g. `5000`). */
  private object AsMillis {
    def unapply(value: String): Option[FiniteDuration] = value.toLongOption.map(_.millis)
  }

  /** A finite duration with a unit (e.g. `5s`, `1500ms`); `Inf` and the like don't match. */
  private object AsDuration {
    def unapply(value: String): Option[FiniteDuration] =
      Try(Duration(value)).toOption.collect { case timeout: FiniteDuration => timeout }
  }

  private def parse(rawHeaderValue: String): Option[FiniteDuration] =
    rawHeaderValue.trim match {
      case AsMillis(timeout) => Some(timeout)
      case AsDuration(timeout) => Some(timeout)
      case _ => None
    }

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
    parse(rawHeaderValue) match {
      case None => Validated.Invalid
      case Some(timeout) if timeout < lowerBound => Validated.BelowLowerBound
      case Some(timeout) if timeout > upperBound => Validated.AboveUpperBound
      case Some(timeout) => Validated.Enforce(timeout)
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
                "expected a number of milliseconds (e.g. '5000') or a duration with a unit " +
                "(e.g. '5s', '1500ms').",
            )
          case Validated.BelowLowerBound =>
            complete(
              StatusCodes.BadRequest,
              s"$RequestTimeoutHeaderName '$rawHeaderValue' is below the minimum of " +
                s"${lowerBound.toMillis} ms.",
            )
          case Validated.AboveUpperBound =>
            complete(
              StatusCodes.BadRequest,
              s"$RequestTimeoutHeaderName '$rawHeaderValue' is above the maximum of " +
                s"${upperBound.toMillis} ms.",
            )
          case Validated.Enforce(timeout) =>
            withRequestTimeout(timeout, timeoutResponse)(inner)
        }
    }
}
