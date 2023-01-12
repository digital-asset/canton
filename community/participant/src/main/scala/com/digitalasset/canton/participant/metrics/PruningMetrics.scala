// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Factory, Timer}
import com.daml.metrics.api.{MetricDoc, MetricName}

class PruningMetrics(val prefix: MetricName, metricsFactory: Factory) {

  object commitments {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "commitments")

    @MetricDoc.Tag(
      summary = "Time spent on commitment computations.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric
        |exposes the time spent on each computation. If the time to compute the metrics
        |starts to exceed the commitment intervals, this likely indicates a problem.""",
      qualification = Debug,
    )
    val compute: Timer = metricsFactory.timer(prefix :+ "compute")
  }

  object prune {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "prune")

    @MetricDoc.Tag(
      summary = "Duration of prune operations.",
      description =
        """This timer exposes the duration of pruning requests from the Canton portion of the ledger.""",
      qualification = Debug,
    )
    val overall: Timer = metricsFactory.timer(prefix)

  }
}
