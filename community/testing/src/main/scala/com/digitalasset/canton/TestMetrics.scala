// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.testing.InMemoryMetricsFactory

/** Metric instances that can be used for testing.
  * Also check:
  * - [[com.digitalasset.canton.participant.metrics.ParticipantTestMetrics]]
  * - [[com.digitalasset.canton.domain.metrics.SequencerTestMetrics]]
  * - [[com.digitalasset.canton.domain.metrics.MediatorTestMetrics]]*
  */
trait TestMetrics {

  val executorServiceMetrics = new ExecutorServiceMetrics(InMemoryMetricsFactory)

}
