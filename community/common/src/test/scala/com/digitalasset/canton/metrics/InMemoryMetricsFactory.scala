// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.testing.InMemoryMetricsFactory as DamlInMemoryMetricsFactory
import com.daml.metrics.api.{MetricHandle, MetricName}
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory

import scala.concurrent.duration.FiniteDuration

class InMemoryMetricsFactory extends DamlInMemoryMetricsFactory with MetricsFactory {

  override def registry: MetricRegistry = new MetricRegistry

  override def loadGauge(
      name: MetricName,
      interval: FiniteDuration,
      timer: MetricHandle.Timer,
  ): TimedLoadGauge = new TimedLoadGauge(name, interval, timer)

}
