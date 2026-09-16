// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.topology.ParticipantId

object ParticipantTestMetrics
    extends ParticipantMetrics(
      new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
      new NoOpMetricsFactory,
    ) {

  val participantId = ParticipantId("test-participant")

  val synchronizer: ConnectedSynchronizerMetrics =
    this.connectedSynchronizerMetrics(SynchronizerAlias.tryCreate("test"), participantId)
}
