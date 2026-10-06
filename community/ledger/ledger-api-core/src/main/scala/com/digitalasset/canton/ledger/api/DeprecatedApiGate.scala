// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.canton.config.DeprecatedApiConfig
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.DeprecatedApiDisabled
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.ApiDeprecation
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.Future

/** Knows which [[com.digitalasset.canton.version.ApiDeprecation]]s the node has re-enabled and
  * rejects the use of the others with `DEPRECATED_API_DISABLED`, on both the gRPC and the JSON
  * Ledger API:
  *   - [[rejection]] returns the error as a value, for code that has to answer with it itself (the
  *     `gated` and `gatedWebsocket` endpoint wrappers of the JSON API),
  *   - [[requireEnabled]] throws it and [[DeprecatedApiGate.GatedFutureOps.gated]] fails a `Future`
  *     with it, for handler code whose surrounding error handling turns the exception into the API
  *     error.
  */
final class DeprecatedApiGate(isEnabledFn: ApiDeprecation => Boolean) {

  def isEnabled(deprecation: ApiDeprecation): Boolean = isEnabledFn(deprecation)

  /** The `DEPRECATED_API_DISABLED` rejection of `api` if `deprecation` is disabled, `None` if it is
    * enabled.
    */
  def rejection(deprecation: ApiDeprecation, api: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Option[DeprecatedApiDisabled.Reject] =
    Option.unless(isEnabled(deprecation))(DeprecatedApiDisabled.Reject(api, deprecation))

  /** Throws the [[rejection]] as a status exception if `deprecation` is disabled. Meant for request
    * mapping code (e.g. inside a `Flow.map`) that cannot return the error as a value.
    */
  def requireEnabled(deprecation: ApiDeprecation, api: String)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Unit =
    rejection(deprecation, api).foreach(reject => throw reject.asGrpcError)
}

object DeprecatedApiGate {

  implicit class GatedFutureOps[T](body: => Future[T]) {

    /** Runs `body` if `deprecation` is enabled in `deprecatedApiGate` and fails with the
      * [[DeprecatedApiGate.rejection]] otherwise, without evaluating `body`. Meant for service
      * methods returning a `Future`: the counterpart of the `gated` endpoint wrapper of the JSON
      * API for gRPC services.
      */
    def gated(
        deprecatedApiGate: DeprecatedApiGate,
        deprecation: ApiDeprecation,
        api: String,
    )(implicit errorLoggingContext: ErrorLoggingContext): Future[T] =
      deprecatedApiGate
        .rejection(deprecation, api)
        .fold(body)(reject => Future.failed(reject.asGrpcError))
  }

  /** The gate configured by the participant's `features.deprecated` flags. */
  def apply(config: DeprecatedApiConfig): DeprecatedApiGate =
    new DeprecatedApiGate(_.isEnabledBy(config))

  /** A gate that enables exactly the given deprecations. */
  @VisibleForTesting
  def enabling(deprecations: ApiDeprecation*): DeprecatedApiGate =
    new DeprecatedApiGate(deprecations.toSet)
}
