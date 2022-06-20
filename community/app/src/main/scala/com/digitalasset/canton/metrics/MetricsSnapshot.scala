// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics

import scala.jdk.CollectionConverters._

case class MetricsSnapshot(
    timers: Map[String, metrics.Timer],
    counters: Map[String, metrics.Counter],
    gauges: Map[String, metrics.Gauge[_]],
    histograms: Map[String, metrics.Histogram],
    meters: Map[String, metrics.Meter],
)

object MetricsSnapshot {

  def apply(registry: metrics.MetricRegistry): MetricsSnapshot = {
    MetricsSnapshot(
      timers = registry.getTimers.asScala.toMap,
      counters = registry.getCounters.asScala.toMap,
      gauges = registry.getGauges.asScala.toMap,
      histograms = registry.getHistograms.asScala.toMap,
      meters = registry.getMeters.asScala.toMap,
    )
  }
}
