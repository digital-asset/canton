// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.testing.InMemoryMetricsFactory.InMemoryCounter
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import org.scalactic.source.Position
import org.scalatest.Inside

object TestCommitmentMetrics {
  def apply(name: String = "test"): CommitmentMetrics = {
    val metricsFactory = new InMemoryMetricsFactory
    val histogramInventory = new HistogramInventory
    new CommitmentMetrics(
      new CommitmentHistograms(MetricName(name))(histogramInventory),
      metricsFactory,
    )(MetricsContext.Empty)
  }

  def counterValue(
      counter: Counter
  )(implicit mc: MetricsContext = MetricsContext.Empty, pos: Position): Long = {
    import Inside.*
    inside(counter) { case inMemoryCounter: InMemoryCounter =>
      inMemoryCounter.markers.get(inMemoryCounter.initialContext.merge(mc)).fold(0L)(_.get())
    }
  }

}
