// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}

object TestReassignmentMetrics {
  def apply(name: String = "test"): ReassignmentMetrics = {
    val metricsFactory = new InMemoryMetricsFactory
    val histogramInventory = new HistogramInventory
    new ReassignmentMetrics(
      new ReassignmentHistograms(MetricName(name))(histogramInventory),
      metricsFactory,
    )(MetricsContext.Empty)
  }
}
