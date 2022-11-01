// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.{MetricDoc, MetricName}
import com.digitalasset.canton.metrics.MetricHandle

class PruningMetrics(override val prefix: MetricName, val registry: MetricRegistry)
    extends MetricHandle.Factory {

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
    val compute: Timer = timer(prefix :+ "compute")
  }

  object prune {
    private val prefix: MetricName = MetricName(PruningMetrics.this.prefix :+ "prune")

    @MetricDoc.Tag(
      summary = "Duration of prune operations.",
      description =
        """This timer exposes the duration of pruning requests from the Canton portion of the ledger.""",
      qualification = Debug,
    )
    val overall: Timer = timer(prefix)

  }
}
