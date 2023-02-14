// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.noop.NoOpMetricsFactory as DamlNoOpMetricsFactory
import com.daml.metrics.api.{MetricHandle as DamlMetricHandle, MetricName}

import scala.concurrent.duration.FiniteDuration

object MetricHandle {

  trait MetricsFactory extends DamlMetricHandle.MetricsFactory {

    def registry: MetricRegistry

    def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): TimedLoadGauge

  }

  class CantonDropwizardMetricsFactory(registry: MetricRegistry)
      extends DropwizardMetricsFactory(registry)
      with MetricsFactory {

    override def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): TimedLoadGauge =
      reRegisterGauge[Double, TimedLoadGauge](name, new TimedLoadGauge(name, interval, timer))

  }

  object NoOpMetricsFactory
      extends DamlNoOpMetricsFactory
      with MetricsFactory
      with LabeledMetricsFactory {

    override val registry = new MetricRegistry

    override def loadGauge(
        name: MetricName,
        interval: FiniteDuration,
        timer: Timer,
    ): TimedLoadGauge = new TimedLoadGauge(name, interval, timer)

  }

}
