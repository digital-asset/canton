// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

class DecryptionHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "decryption"

  private[metrics] val decryptLatency: Item = Item(
    prefix :+ "latency",
    summary = "Latency of decryption requests.",
    description = "Measures the latency of decryption operations.",
    qualification = MetricQualification.Latency,
  )
}

class DecryptionMetrics(
    histograms: DecryptionHistograms,
    labeledMetricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  val prefix: MetricName = histograms.prefix
  val decryptLatency: Timer = labeledMetricsFactory.timer(histograms.decryptLatency.info)
}
