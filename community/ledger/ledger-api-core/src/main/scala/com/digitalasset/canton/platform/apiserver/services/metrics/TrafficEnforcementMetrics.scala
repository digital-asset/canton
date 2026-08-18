// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.metrics

import com.daml.metrics.api.MetricHandle.Meter
import com.daml.metrics.api.MetricQualification.Traffic
import com.daml.metrics.api.{MetricHandle, MetricInfo, MetricName, MetricsContext}

class TrafficEnforcementMetrics(
    parent: MetricName,
    metricsFactory: MetricHandle.LabeledMetricsFactory,
)(implicit val metricsContext: MetricsContext) {
  val prefix: MetricName = parent :+ "traffic-enforcement"

  val balanceLookupsName: MetricName = prefix :+ "balance-lookups"
  val balanceLookups: Meter = metricsFactory.meter(
    MetricInfo(
      name = balanceLookupsName,
      summary = "Number of balance lookups performed",
      description = "The number of times the system has looked up an account balance.",
      qualification = Traffic,
    )
  )

  val notEnoughTrafficName: MetricName = prefix :+ "not-enough-traffic"
  val notEnoughTraffic: Meter = metricsFactory.meter(
    MetricInfo(
      name = notEnoughTrafficName,
      summary = "Number of times an account did not have enough traffic to perform an action",
      description =
        "The number of times the system has rejected an action due to insufficient traffic balance.",
      qualification = Traffic,
    )
  )
}
