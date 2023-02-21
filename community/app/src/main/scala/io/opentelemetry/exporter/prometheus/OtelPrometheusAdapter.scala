// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.opentelemetry.exporter.prometheus

import io.opentelemetry.sdk.metrics.data.MetricData
import io.prometheus.client.Collector

/*
 * Required to access the internal conversion API
 * */
object OtelPrometheusAdapter {

  def openTelemetryMetricsToPrometheus(
      metrics: Seq[MetricData]
  ): Seq[Collector.MetricFamilySamples] =
    metrics.map(MetricAdapter.toMetricFamilySamples)

}
