// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.config.DeprecatedApiConfig

/** To deprecate APIs in a new release (say 3.8):
  *   1. add an `enableDeprecatedEndpoints38: Boolean = true` flag to
  *      [[com.digitalasset.canton.config.DeprecatedApiConfig]] (which makes it the
  *      `canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-38`
  *      configuration key),
  *   1. add a `Canton38` object to [[ApiDeprecation$]] with the schedule and the flag,
  *   1. mark the JSON API endpoints as deprecated with the [[documentationNotice]] in their
  *      description, and gate the gRPC methods with `DeprecatedApiGate`,
  *   1. mention the flag in the release notes.
  */
abstract class ApiDeprecation(
    val deprecatedIn: CantonMinorVersion,
    val disabledByDefaultIn: CantonMinorVersion,
    val removedIn: CantonMinorVersion,
    val featureFlag: String,
) {

  /** Whether the node's feature flags keep the deprecated APIs enabled. Must read the flag named by
    * [[featureFlag]].
    */
  def isEnabledBy(config: DeprecatedApiConfig): Boolean

  /** Notice for the documentation of a deprecated API, naming what to use instead. */
  def documentationNotice(useInstead: String): String =
    s"Deprecated since Canton $deprecatedIn, disabled by default in Canton $disabledByDefaultIn " +
      s"and removed in Canton $removedIn " +
      s"(disable it earlier with `$featureFlag = false`). Use $useInstead instead."

  /** The cause of `DEPRECATED_API_DISABLED` when `api` (an endpoint or a gRPC method) is used while
    * the deprecation is disabled.
    */
  def disabledMessage(api: String): String =
    s"$api was deprecated in Canton $deprecatedIn and will be removed in Canton $removedIn. " +
      s"This participant disabled it with `$featureFlag = false`."

  override def toString: String = s"ApiDeprecation($deprecatedIn, $featureFlag)"
}

object ApiDeprecation {

  /** The APIs deprecated in Canton 3.7. */
  object Canton37 {

    /** Endpoints and gRPC methods that are removed as a whole. */
    case object Endpoints
        extends ApiDeprecation(
          deprecatedIn = CantonMinorVersion(3, 7),
          disabledByDefaultIn = CantonMinorVersion(3, 8),
          removedIn = CantonMinorVersion(3, 9),
          featureFlag =
            "canton.participants.<participant>.features.deprecated.enable-deprecated-endpoints-37",
        ) {
      override def isEnabledBy(config: DeprecatedApiConfig): Boolean =
        config.enableDeprecatedEndpoints37
    }
  }
}

final case class CantonMinorVersion(major: Int, minor: Int) {
  override def toString: String = s"$major.$minor"
}
