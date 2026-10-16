// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.config.DeprecatedApiConfig

/** To deprecate APIs in a new release (say 3.8):
  *   1. add `enableDeprecatedEndpoints38` / `enableDeprecatedParameters38` flags to
  *      [[com.digitalasset.canton.config.DeprecatedApiConfig]] (which makes them the
  *      `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-38` and
  *      `canton.participants.<participant>.features.deprecated.enable-deprecated-parameters-38`
  *      configuration keys),
  *   1. add a `Canton38` object to [[ApiDeprecation$]] with the schedule and the flags and list its
  *      deprecations in [[ApiDeprecation.all]], which extends the `CantonConfigTest` round trip of
  *      the feature flags to them,
  *   1. mark the JSON API endpoints with `.deprecatedSince(Canton38.Endpoints, ...)` and gate gRPC
  *      methods and request fields with `DeprecatedApiGate`,
  *   1. mention the flags in the `DEPRECATED_API_DISABLED` error resolution and the release notes.
  */
abstract class ApiDeprecation(
    val deprecatedIn: CantonMinorVersion,
    val disabledIn: CantonMinorVersion,
    val removedIn: Option[CantonMinorVersion],
    val featureFlag: String,
) {

  /** Whether the node's feature flags re-enable the deprecated APIs; must agree with
    * [[featureFlag]].
    */
  def isEnabledBy(config: DeprecatedApiConfig): Boolean

  /** Notice for the documentation of a deprecated API, naming what to use instead. */
  def documentationNotice(useInstead: String): String =
    s"Deprecated since Canton $deprecatedIn: disabled by default since Canton $disabledIn " +
      s"(re-enable temporarily with `$featureFlag = true`) and $removalNotice. Use $useInstead instead."

  /** The cause of `DEPRECATED_API_DISABLED` when `api` (an endpoint, a gRPC method or request
    * fields) is used while the deprecation is disabled.
    */
  def disabledMessage(api: String): String =
    s"$api was deprecated in Canton $deprecatedIn, is disabled by default since Canton $disabledIn " +
      s"and $removalNotice. Set `$featureFlag = true` to re-enable it temporarily."

  private def removalNotice: String =
    removedIn.fold("will be removed in a future Canton release")(v =>
      s"will be removed in Canton $v"
    )

  override def toString: String = s"ApiDeprecation($deprecatedIn, $featureFlag)"
}

object ApiDeprecation {

  /** The APIs deprecated in Canton 3.4. */
  // TODO(#35974) remove together with the deprecated APIs in 3.7
  object Canton34 {

    /** Endpoints and gRPC methods that are removed as a whole. */
    case object Endpoints
        extends ApiDeprecation(
          deprecatedIn = CantonMinorVersion(3, 4),
          disabledIn = CantonMinorVersion(3, 6),
          removedIn = Some(CantonMinorVersion(3, 7)),
          featureFlag =
            "canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-34",
        ) {
      override def isEnabledBy(config: DeprecatedApiConfig): Boolean =
        config.enableDeprecatedEndpoints34
    }

    /** Request fields that are removed from endpoints that stay. */
    case object Parameters
        extends ApiDeprecation(
          deprecatedIn = CantonMinorVersion(3, 4),
          disabledIn = CantonMinorVersion(3, 6),
          removedIn = Some(CantonMinorVersion(3, 7)),
          featureFlag =
            "canton.participants.<participant>.features.deprecated.enable-deprecated-parameters-34",
        ) {
      override def isEnabledBy(config: DeprecatedApiConfig): Boolean =
        config.enableDeprecatedParameters34
    }
  }

  /** The APIs deprecated in Canton 3.5. */
  // TODO(#35974) remove together with the deprecated APIs in 3.7
  object Canton35 {

    /** Endpoints and gRPC methods that are removed as a whole. */
    case object Endpoints
        extends ApiDeprecation(
          deprecatedIn = CantonMinorVersion(3, 5),
          disabledIn = CantonMinorVersion(3, 6),
          removedIn = Some(CantonMinorVersion(3, 7)),
          featureFlag =
            "canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-35",
        ) {
      override def isEnabledBy(config: DeprecatedApiConfig): Boolean =
        config.enableDeprecatedEndpoints35
    }
  }

  /** All declared deprecations, one per `features.deprecated` participant feature flag. */
  val all: Seq[ApiDeprecation] =
    Seq[ApiDeprecation](Canton34.Endpoints, Canton34.Parameters, Canton35.Endpoints)
}

final case class CantonMinorVersion(major: Int, minor: Int) {
  override def toString: String = s"$major.$minor"
}
