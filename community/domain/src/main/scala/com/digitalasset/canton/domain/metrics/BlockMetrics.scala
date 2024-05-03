// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.api.MetricHandle.{Gauge, Histogram, Meter}
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory

/** Metrics produced by the block update generator */
class BlockMetrics(
    parent: MetricName,
    val openTelemetryMetricsFactory: CantonLabeledMetricsFactory,
) {

  private val prefix: MetricName = parent :+ "block"

  val height: Gauge[Long] =
    openTelemetryMetricsFactory.gauge(prefix :+ "height", 0L)(MetricsContext.Empty)
  val blockEvents: Meter = openTelemetryMetricsFactory.meter(prefix :+ "events")
  val blockEventBytes: Meter =
    openTelemetryMetricsFactory.meter(prefix :+ s"event-${Histogram.Bytes}")

}
