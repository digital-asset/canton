// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.digitalasset.canton.config.{PerAttributeRateLimitConfig, RateLimitConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps

/** Ported from splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/IpCidrRateLimits.scala`,
  * unchanged.
  */
object IpCidrRateLimits {

  private val noOverride: String => Option[RateLimitConfig.Simple] = _ => None

  /** Returns a matcher for the given config, resolving a client IP to the limit of the most
    * specific network it is contained in. The config is parsed and sorted once here so that the
    * per-request path only has to parse the client IP and run bitwise comparisons.
    */
  def matchClientIp(
      config: PerAttributeRateLimitConfig
  ): String => Option[RateLimitConfig.Simple] = {
    val parsedNetworks = networks(config.attributeOverrides).toArray
    if (parsedNetworks.isEmpty) noOverride
    else
      clientIp =>
        IpCidr.parse(clientIp).flatMap { ip =>
          parsedNetworks.collectFirst { case (network, limit) if network.contains(ip) => limit }
        }
  }

  def tryValidate(config: PerAttributeRateLimitConfig): Unit =
    networks(config.attributeOverrides).discard

  private def networks(
      overrides: Map[String, RateLimitConfig.Simple]
  ): Seq[(IpCidr, RateLimitConfig.Simple)] =
    overrides.toSeq
      .map { case (cidr, limit) => IpCidr.tryParse(cidr) -> limit }
      // most specific network first, so that it takes precedence over the networks containing it
      .sortBy { case (network, _) => -network.prefixLength }
}
