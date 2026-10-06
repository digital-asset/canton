// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricQualification.Saturation
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricsContext}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.TracedLogger

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Ported from `SpliceRateLimitMetrics` in splice's
  * `apps/common/src/main/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiter.scala`,
  * renamed for a splice-agnostic home. The meters, gauge, and their labels are unchanged so a
  * dashboard built against one can be built the same way against the other.
  *
  * `prefix` defaults to canton's own `MetricName.Daml` (splice's original hardcodes its own
  * `SpliceMetrics.MetricsPrefix` instead), so a future splice caller can pass that in and keep
  * emitting `splice.*`-prefixed metrics unchanged after migrating onto this class.
  */
final case class RateLimitMetrics(
    otelFactory: LabeledMetricsFactory,
    private val logger: TracedLogger,
    prefix: MetricName = MetricName.Daml,
)(implicit
    mc: MetricsContext
) extends AutoCloseable {

  private val gaugesToClose = Collections.synchronizedList(new util.ArrayList[AutoCloseable]())

  val meter: MetricHandle.Meter = otelFactory.meter(
    MetricInfo(
      prefix :+ "rate_limiting",
      "Rate limits applied in the node",
      Saturation,
    )
  )

  val unknownAttributeNotLimited: MetricHandle.Meter = otelFactory.meter(
    MetricInfo(
      prefix :+ "rate_limiting_unknown_attribute_not_limited",
      "Number of requests not rate limited by a per-attribute limiter because the attribute value is unknown",
      Saturation,
    )
  )

  def recordUnknownAttributeNotLimited()(implicit extraMc: MetricsContext): Unit =
    unknownAttributeNotLimited.mark()(mc.merge(extraMc))

  /*we need to pass the full context when we create it to avoid duplicate values warnings*/
  def recordMaxLimit(limit: Double)(implicit extraMc: MetricsContext): Unit = {
    val createdGauge = otelFactory.gauge[Double](
      MetricInfo(
        prefix :+ "rate_limiting_max_limit_per_second",
        "Max allowed rate per second",
        Saturation,
      ),
      limit,
    )(mc.merge(extraMc))
    gaugesToClose.add(createdGauge).discard
  }

  override def close(): Unit = {
    val gaugesThatWillBeClosed = gaugesToClose.asScala.toSeq
    gaugesToClose.clear()
    LifeCycle.close(gaugesThatWillBeClosed*)(logger)
  }

}
