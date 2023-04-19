// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.daml.metrics.api.reporters.MetricsReporter
import com.digitalasset.canton.platform.config.MetricsConfig.MetricRegistryType

import scala.concurrent.duration.{Duration, *}

final case class MetricsConfig(
    enabled: Boolean = false,
    reporter: MetricsReporter = MetricsReporter.Console,
    reportingInterval: Duration = 10.seconds,
    registryType: MetricRegistryType = MetricRegistryType.JvmShared,
)

object MetricsConfig {
  sealed trait MetricRegistryType
  object MetricRegistryType {
    final case object JvmShared extends MetricRegistryType
    final case object New extends MetricRegistryType
  }
  val DefaultMetricsConfig: MetricsConfig = MetricsConfig()
}