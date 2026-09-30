// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.caching.{CaffeineCache, ConcurrentCache}
import com.digitalasset.canton.config.{PerAttributeRateLimitConfig, RateLimitConfig}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.metrics.RateLimitMetrics
import com.digitalasset.canton.tracing.TraceContext
import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** Ported from `PerAttributeRateLimiter` in splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiter.scala`.
  */
class PerAttributeRateLimiter(
    name: String,
    attribute: String,
    config: PerAttributeRateLimitConfig,
    metrics: RateLimitMetrics,
    logger: TracedLogger,
    attributeMatcherFactory: PerAttributeRateLimitConfig => String => Option[
      RateLimitConfig.Simple
    ] = PerAttributeRateLimiter.exactMatch,
) {

  private val attributeMatcher: String => Option[RateLimitConfig.Simple] =
    attributeMatcherFactory(config)

  private val attributeLabel = Map("limiter_attribute" -> attribute)

  // evictions by size can happen for every single request (e.g. when a large number of distinct
  // attribute values is seen), so the warning is throttled to avoid flooding the logs
  private val lastSizeEvictionWarning =
    new AtomicLong(System.nanoTime() - PerAttributeRateLimiter.EvictionWarningIntervalNanos)

  private val evictionListener: RemovalListener[String, TokenBucketRateLimiter] =
    (key: String, _: TokenBucketRateLimiter, cause: RemovalCause) => {
      if (cause == RemovalCause.SIZE) {
        implicit val tc: TraceContext = TraceContext.empty
        val message =
          s"Rate limiter cache for $name (attribute '$attribute') exceeded its maximum size of " +
            s"${config.maxAttributeValues}; evicting the rate limiter for attribute value '$key'. " +
            "Its rate limiting state is lost. Consider increasing max-attribute-values."
        val now = System.nanoTime()
        val last = lastSizeEvictionWarning.get()
        if (
          now - last >= PerAttributeRateLimiter.EvictionWarningIntervalNanos && lastSizeEvictionWarning
            .compareAndSet(last, now)
        ) {
          logger.warn(message)
        } else {
          logger.debug(message)
        }
      }
    }

  // lazy so that neither the cache nor its metrics are created if the limiter is disabled
  private lazy val cache: ConcurrentCache[String, TokenBucketRateLimiter] = CaffeineCache[
    String,
    TokenBucketRateLimiter,
  ](
    Caffeine
      .newBuilder()
      .maximumSize(config.maxAttributeValues)
      // Evict limiters that have not been used for a full sustained rate limiting window (the bucket
      // size of the interval rate limiter): after that time an idle limiter would have refilled its
      // budget anyway, so dropping it does not change the enforced rate.
      // The longest window of the default limit and all overrides is used, so that no limiter is
      // evicted before its own window has elapsed.
      .expireAfterAccess(
        (config.limit +: config.attributeOverrides.values.toSeq)
          .map(TokenBucketRateLimiter.sustainedWindow)
          .foldLeft(Duration.ZERO)((longest, window) =>
            if (window.compareTo(longest) > 0) window else longest
          )
      )
      .evictionListener(evictionListener),
    Some(new CacheMetrics(s"$name-$attribute-rate-limiter", metrics.otelFactory)),
  )

  private lazy val reportedMaxLimit: Unit =
    metrics.recordMaxLimit(config.limit.ratePerSecond)(
      MetricsContext(
        attributeLabel ++ Map(
          "limiter" -> name,
          "limiter_type" -> TokenBucketRateLimiter.PerAttributeLimiterType,
        )
      )
    )

  def markRun(attributeValue: Option[String]): Boolean =
    if (config.enabled) attributeValue match {
      case Some(value) => limiterFor(value).markRun()
      case None =>
        metrics.recordUnknownAttributeNotLimited()(
          MetricsContext(
            attributeLabel ++ Map(
              "limiter" -> name,
              "limiter_type" -> TokenBucketRateLimiter.PerAttributeLimiterType,
            )
          )
        )
        true
    }
    else true

  private def limiterFor(attributeValue: String): TokenBucketRateLimiter = {
    reportedMaxLimit
    cache.getOrAcquire(
      attributeValue,
      (_: String) =>
        new TokenBucketRateLimiter(
          name,
          attributeMatcher(attributeValue).getOrElse(config.limit),
          metrics,
          limiterType = TokenBucketRateLimiter.PerAttributeLimiterType,
          extraLabels = attributeLabel,
          reportMaxLimit = false,
        ),
    )
  }
}

object PerAttributeRateLimiter {

  private val EvictionWarningIntervalNanos: Long = TimeUnit.MINUTES.toNanos(1)

  val exactMatch: PerAttributeRateLimitConfig => String => Option[RateLimitConfig.Simple] =
    config => attributeValue => config.attributeOverrides.get(attributeValue)

  val noOverrides: PerAttributeRateLimitConfig => String => Option[RateLimitConfig.Simple] =
    _ => _ => None
}
