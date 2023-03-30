// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics
import io.opentelemetry.exporter.prometheus.OtelPrometheusAdapter
import io.prometheus.client.Collector.MetricFamilySamples

import scala.jdk.CollectionConverters.*

final case class MetricsSnapshot(
    timers: Map[String, metrics.Timer],
    counters: Map[String, metrics.Counter],
    gauges: Map[String, metrics.Gauge[_]],
    histograms: Map[String, metrics.Histogram],
    meters: Map[String, metrics.Meter],
    prometheusMetrics: Seq[MetricFamilySamples],
)

object MetricsSnapshot {

  def apply(registry: metrics.MetricRegistry, reader: OnDemandMetricsReader): MetricsSnapshot = {
    val metrics = reader.read()
    MetricsSnapshot(
      timers = registry.getTimers.asScala.toMap,
      counters = registry.getCounters.asScala.toMap,
      gauges = registry.getGauges.asScala.toMap,
      histograms = registry.getHistograms.asScala.toMap,
      meters = registry.getMeters.asScala.toMap,
      prometheusMetrics = OtelPrometheusAdapter.openTelemetryMetricsToPrometheus(metrics),
    )
  }
}
