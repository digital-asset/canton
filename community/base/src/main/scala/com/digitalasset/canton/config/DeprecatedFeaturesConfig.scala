// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** The `canton.features.deprecated` flags re-enable deprecated features, which are disabled by
  * default, until they are removed. They apply to every node of the process.
  *
  * @param enableScopeOnlyJwt
  *   Re-enables scope-only JWTs, i.e. access tokens that carry a scope but no audience, on the
  *   Ledger API and on the admin API of every node. When disabled (the default), an `auth-services`
  *   entry with a `target-scope` but no `target-audience` is rejected at startup, and a token
  *   relying on the `daml_ledger_api` scope instead of an audience is rejected as unauthenticated.
  */
final case class DeprecatedFeaturesConfig(
    enableScopeOnlyJwt: Boolean = false
)

object DeprecatedFeaturesConfig {

  /** The configuration key of [[DeprecatedFeaturesConfig.enableScopeOnlyJwt]]. */
  val EnableScopeOnlyJwtKey: String = "canton.features.deprecated.enable-scope-only-jwt"

  /** The reason given when a scope-only JWT, or a configuration that only scope-only JWTs can
    * satisfy, is rejected because [[DeprecatedFeaturesConfig.enableScopeOnlyJwt]] is off.
    */
  val ScopeOnlyJwtDisabledMessage: String =
    "Scope-only JWTs (tokens with a scope but no audience) are deprecated and disabled by default. " +
      s"Migrate to audience-based tokens, or set $EnableScopeOnlyJwtKey = true to accept " +
      "scope-only JWTs until they are removed."
}
