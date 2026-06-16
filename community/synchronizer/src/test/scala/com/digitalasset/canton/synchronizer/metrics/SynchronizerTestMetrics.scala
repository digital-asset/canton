// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}

final case class SequencerTestMetrics(testName: String)
    extends SequencerMetrics(
      new SequencerHistograms(MetricName(testName))(new HistogramInventory),
      NoOpMetricsFactory,
    )

final case class MediatorTestMetrics(testName: String)
    extends MediatorMetrics(
      new MediatorHistograms(MetricName(testName))(new HistogramInventory),
      NoOpMetricsFactory,
    )
