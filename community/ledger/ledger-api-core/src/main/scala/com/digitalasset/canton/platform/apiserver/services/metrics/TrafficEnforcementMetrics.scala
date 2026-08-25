// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, Meter, Timer}
import com.daml.metrics.api.MetricQualification.{Errors, Latency, Traffic}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricsContext,
}

class TrafficEnforcementMetrics(
    inventory: TrafficEnforcementInventory,
    metricsFactory: MetricHandle.LabeledMetricsFactory,
)(implicit val metricsContext: MetricsContext) {
  val prefix: MetricName = inventory.prefix

  val balanceLookups: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "balance-lookups",
      summary = "Number of balance lookups performed",
      description = "The number of times the system has looked up an account balance.",
      qualification = Traffic,
    )
  )

  val insufficientBalanceRejections: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "insufficient-balance-rejections",
      summary = "Number of times an account did not have enough traffic to perform an action",
      description =
        "The number of times the system has rejected an action due to insufficient traffic balance.",
      qualification = Traffic,
    )
  )

  val allowedSubmissionOnLookupFailures: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "allowed-submission-on-lookup-failures",
      summary =
        "Number of times a submission was allowed, but traffic information couldn't be fetched",
      description =
        "The number of times the system has allowed a submission to proceed despite a failure to fetch traffic information.",
      qualification = Errors,
    )
  )

  val enforcementCheckDuration: Timer = metricsFactory.timer(inventory.enforcementCheckItem.info)

  val projectionTimestamp: Gauge[Long] = metricsFactory.gauge[Long](
    MetricInfo(
      name = prefix :+ "projection-timestamp",
      summary = "The consumed timestamp of the traffic enforcement projection",
      description =
        "The consumed timestamp of the traffic enforcement projection, indicating how far the system has processed traffic enforcement events.",
      qualification = Latency,
    ),
    0L,
  )

  val projectionOffset: Gauge[Long] = metricsFactory.gauge[Long](
    MetricInfo(
      name = prefix :+ "projection-offset",
      summary = "The saved offset of the traffic enforcement projection",
      description = "The saved offset of the traffic enforcement projection.",
      qualification = Latency,
    ),
    0L,
  )
}

class TrafficEnforcementInventory(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "traffic-enforcement"
  val enforcementCheckItem = Item(
    name = prefix :+ "enforcement-check-duration",
    summary = "Duration of traffic enforcement checks",
    description =
      "The time taken to perform traffic enforcement checks, including balance lookups and decision making.",
    qualification = Latency,
  )
}
