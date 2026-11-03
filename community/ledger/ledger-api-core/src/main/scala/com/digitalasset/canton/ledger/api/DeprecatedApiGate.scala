// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.digitalasset.canton.config.DeprecatedApiConfig
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.DeprecatedApiDisabled
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.ApiDeprecation

import scala.concurrent.Future

/** Rejects the use of the [[com.digitalasset.canton.version.ApiDeprecation]]s that the node has
  * disabled with `DEPRECATED_API_DISABLED`.
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
}

object DeprecatedApiGate {

  implicit class GatedFutureOps[T](body: => Future[T]) {

    /** Runs `body` if `deprecation` is enabled in `deprecatedApiGate` and fails with the
      * [[DeprecatedApiGate.rejection]] otherwise, without evaluating `body`.
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
}
