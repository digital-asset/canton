// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

class SigningHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "signing"

  private[metrics] val signingLatency: Item = Item(
    prefix :+ "latency",
    summary = "Latency of signing requests.",
    description = "Measures the latency of signing operations.",
    qualification = MetricQualification.Latency,
  )
}

class SigningMetrics(
    histograms: SigningHistograms,
    labeledMetricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  val prefix: MetricName = histograms.prefix
  val signingLatency: Timer = labeledMetricsFactory.timer(histograms.signingLatency.info)
}
