// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

class TransactionViewHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private val prefix = parent :+ "transaction-view"

  val subViews: Item =
    Item(
      name = prefix :+ "sub-views",
      summary = "The number of sub-views in the transaction view",
      description = "The number of sub-views in the transaction view.",
      qualification = MetricQualification.Saturation,
    )

  val treeDepth: Item =
    Item(
      name = prefix :+ "tree-depth",
      summary = "The depth of a transaction view tree",
      description = "The parse depth of a transaction view tree",
      qualification = MetricQualification.Saturation,
    )
}

class TransactionViewMetrics(
    histograms: TransactionViewHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {
  val subViews: Gauge[Int] = factory.gauge[Int](histograms.subViews.info, 0)

  val treeDepth: Gauge[Int] = factory.gauge[Int](histograms.treeDepth.info, 0)
}
