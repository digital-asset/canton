// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RateLimitConfig
import com.digitalasset.canton.metrics.RateLimitMetrics
import com.google.common.util.concurrent.{BurstyRateLimiterFactory, RateLimiter}

import java.time.Duration
import scala.concurrent.Future

/** Ported from splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiter.scala`,
  * renamed to avoid colliding with [[com.digitalasset.canton.util.RateLimiter]] and to avoid a
  * splice-specific name in a splice-agnostic library. Behavior, fields, and metrics are otherwise
  * unchanged.
  */
object TokenBucketRateLimiter {

  val GlobalLimiterType = "global"
  val PerAttributeLimiterType = "per-attribute"

  val DefaultSustainedWindowSeconds: Long = 60

  private[ratelimiting] def sustainedWindow(config: RateLimitConfig): Duration =
    Duration.ofSeconds(Math.max(1L, config.sustainedWindowSeconds))
}

// noinspection UnstableApiUsage
class TokenBucketRateLimiter(
    name: String,
    config: RateLimitConfig,
    metrics: RateLimitMetrics,
    limiterType: String = TokenBucketRateLimiter.GlobalLimiterType,
    extraLabels: Map[String, String] = Map.empty,
    // must be disabled for the per-attribute limiters as they'd all report the same value
    // and would explode the number of registered gauges
    reportMaxLimit: Boolean = true,
) {

  private val metricsContext = MetricsContext(
    extraLabels ++ Map("limiter" -> name, "limiter_type" -> limiterType)
  )

  private val rejectAll: Boolean =
    config.enabled &&
      (config.ratePerSecond <= 0 || config.sustainedRatePerSecond.exists(_ <= 0))

  // The limiters are created with one second worth of permits already available
  private val limiter: Option[RateLimiter] =
    Option.when(config.enabled && !rejectAll)(
      BurstyRateLimiterFactory.create(config.ratePerSecond)
    )
  // enforces the sustained limit over the sustained window, while still allowing bursts within its budget.
  private val sustainedLimiter: Option[RateLimiter] =
    Option
      .when(config.enabled && !rejectAll)(config.sustainedRatePerSecond)
      .flatten
      .map(
        BurstyRateLimiterFactory
          .create(_, TokenBucketRateLimiter.sustainedWindow(config).toSeconds.toDouble)
      )
  // lazy to ensure metrics get registered only if the limiter is actually used
  private lazy val reportedMaxLimit: Unit =
    if (reportMaxLimit) {
      metrics.recordMaxLimit(config.ratePerSecond)(metricsContext)
    }

  def markRun(): Boolean =
    if (config.enabled) {
      reportedMaxLimit
      val canRun =
        !rejectAll && limiter.forall(_.tryAcquire()) && sustainedLimiter.forall(_.tryAcquire())
      if (canRun) {
        metrics.meter.mark()(
          metricsContext.merge(MetricsContext("result" -> "accepted"))
        )
      } else {
        metrics.meter.mark()(
          metricsContext.merge(MetricsContext("result" -> "rejected"))
        )
      }
      canRun
    } else true

  def runWithLimit[T](f: => Future[T]): Future[T] =
    if (markRun()) {
      f
    } else {
      Future.failed(
        io.grpc.Status.RESOURCE_EXHAUSTED
          .withDescription("Rate limit exceeded")
          .asRuntimeException()
      )
    }

}
