// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.networking.grpc.ratelimiting.TokenBucketRateLimiter

/** A rate limit, expressed as a burst rate together with an optional sustained rate enforced over a
  * longer window.
  *
  * Ported from `SpliceRateLimitConfig` in splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiter.scala`,
  * renamed for a splice-agnostic home. The shape (including `enabled` and the zero-rate "reject
  * everything" semantics implemented by
  * [[com.digitalasset.canton.networking.grpc.ratelimiting.TokenBucketRateLimiter]]) is unchanged,
  * so splice can migrate onto this type without adapting its own config values.
  */
trait RateLimitConfig {

  def enabled: Boolean

  def ratePerSecond: Double

  def sustainedRatePerSecond: Option[Double]

  def sustainedWindowSeconds: Long
}

object RateLimitConfig {

  final case class Simple(
      enabled: Boolean = true,
      ratePerSecond: Double,
      sustainedRatePerSecond: Option[Double] = None,
      sustainedWindowSeconds: Long = TokenBucketRateLimiter.DefaultSustainedWindowSeconds,
  ) extends RateLimitConfig

  def apply(
      enabled: Boolean = true,
      ratePerSecond: Double,
      sustainedRatePerSecond: Option[Double] = None,
      sustainedWindowSeconds: Long = TokenBucketRateLimiter.DefaultSustainedWindowSeconds,
  ): Simple =
    Simple(enabled, ratePerSecond, sustainedRatePerSecond, sustainedWindowSeconds)
}

/** Rate limits requests per value of some attribute (e.g. the client IP), with optional overrides
  * for specific attribute values.
  *
  * Ported from `PerAttributeRateLimitConfig` in splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiter.scala`,
  * unchanged.
  *
  * @param maxAttributeValues
  *   the maximum number of distinct attribute values to track limiter state for at once
  * @param attributeOverrides
  *   overrides keyed by attribute value. When used for a client IP (see
  *   [[com.digitalasset.canton.networking.grpc.ratelimiting.IpCidrRateLimits]]), a key is an IP
  *   network in CIDR notation (a bare IP address denotes a single host)
  */
final case class PerAttributeRateLimitConfig(
    enabled: Boolean = true,
    limit: RateLimitConfig.Simple = PerAttributeRateLimitConfig.DefaultLimit,
    maxAttributeValues: Long = 10000,
    attributeOverrides: Map[String, RateLimitConfig.Simple] = Map.empty,
)

object PerAttributeRateLimitConfig {

  val DefaultLimit: RateLimitConfig.Simple = RateLimitConfig(ratePerSecond = 10)

  def disabled: PerAttributeRateLimitConfig =
    PerAttributeRateLimitConfig(enabled = false)
}

/** An overall rate limit that additionally limits per client IP.
  *
  * Ported from splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/config/RateLimitersConfig.scala`
  * (`PerClientIpRateLimitConfig`), unchanged.
  *
  * The `attributeOverrides` of `perClientIp` (`ip-overrides` in the config) are keyed by an IP
  * network in CIDR notation (a bare IP address denotes a single host), e.g. `{ "10.0.0.0/8" = {
  * rate-per-second = 100 } }`.
  */
final case class PerClientIpRateLimitConfig(
    enabled: Boolean = true,
    ratePerSecond: Double,
    sustainedRatePerSecond: Option[Double] = None,
    sustainedWindowSeconds: Long = TokenBucketRateLimiter.DefaultSustainedWindowSeconds,
    perClientIp: PerAttributeRateLimitConfig = PerAttributeRateLimitConfig.disabled,
) extends RateLimitConfig

/** Configuration for a rate limiter with a global limit, per-operation overrides, and an optional
  * per-client-IP dimension on each of those.
  *
  * Ported from splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/config/RateLimitersConfig.scala`,
  * unchanged. That way splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/http/HttpRateLimiter.scala` and
  * canton's `IpRateLimitInterceptor` can both be configured the same way from the same type.
  *
  * @param default
  *   Overall rate limiter applied per operation (per gRPC method, for `IpRateLimitInterceptor`).
  *   Used when there is no operation-specific override in `rateLimiters`. The embedded
  *   `perClientIp` limiter is disabled by default; enable it to additionally limit per client IP.
  * @param rateLimiters
  *   Per-operation overrides of the overall `default` rate limiter.
  * @param clientIpHeaders
  *   Names of the headers from which the client IP used for per-client-IP rate limiting is
  *   extracted, in order of precedence: the first header that is present and whose value (or, for
  *   comma separated lists such as `X-Forwarded-For`, whose first entry) parses as an IP literal is
  *   used. Set to an empty list to disable per-client-IP rate limiting.
  *
  * Note that the default headers are client-controlled and can hence be spoofed unless they are
  * overwritten by infrastructure the client cannot bypass. In deployments with a trusted reverse
  * proxy, configure the (non-spoofable) header set by that proxy instead, e.g.
  * `["x-envoy-external-address"]` behind an Envoy proxy.
  */
final case class RateLimitersConfig(
    default: PerClientIpRateLimitConfig = PerClientIpRateLimitConfig(ratePerSecond = 200),
    rateLimiters: Map[String, PerClientIpRateLimitConfig] = Map.empty,
    global: PerClientIpRateLimitConfig = RateLimitersConfig.DefaultGlobal,
    clientIpHeaders: Seq[String] = RateLimitersConfig.DefaultClientIpHeaders,
) {
  def forRateLimiter(name: String): PerClientIpRateLimitConfig =
    rateLimiters.getOrElse(name, default)
}

object RateLimitersConfig {

  /** The commonly used client IP headers, in order of precedence. Both are set by clients or
    * reverse proxies and are hence only trustworthy if a proxy the client cannot bypass overwrites
    * them.
    */
  val DefaultClientIpHeaders: Seq[String] = Seq("x-forwarded-for", "x-real-ip")

  private val DefaultGlobal: PerClientIpRateLimitConfig =
    PerClientIpRateLimitConfig(
      ratePerSecond = 200,
      perClientIp = PerAttributeRateLimitConfig(),
    )
}
