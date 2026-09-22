// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.daml.metrics.api.MetricHandle.Counter
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.metrics.{CommitmentSenderMetrics, TestCommitmentMetrics}
import com.digitalasset.canton.participant.store.AcsCommitmentSenderWatermarkStore
import com.digitalasset.canton.tracing.TraceContext
import org.scalactic.source.Position
import org.scalatest.Assertion

trait BaseAcsCommitmentSenderTest extends BaseTest {
  protected def assertInitialEmptySendMetricValues(
      metrics: CommitmentSenderMetrics
  )(implicit pos: Position): Assertion = {
    assertCounterMetricValue(metrics.sentBatchCount, 0)
    assertCounterMetricValue(metrics.sentCommitmentCount, 0)
    assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
    assertCounterMetricValue(metrics.sendFailureCount, 0)
    assertCounterMetricValue(metrics.sendAttemptCount, 0)
  }

  protected def assertWatermarkValue(
      watermarkStore: AcsCommitmentSenderWatermarkStore,
      expectedTimepoint: Option[Timepoint],
  )(implicit traceContext: TraceContext): Assertion =
    watermarkStore.lookupWatermark().futureValueUS.map(_.tupled) shouldBe expectedTimepoint.map(
      _.tupled
    )

  protected def assertInitialEmptyMetricValues(
      metrics: CommitmentSenderMetrics
  )(implicit pos: Position): Assertion = {
    assertWatermarkMetricsValue(metrics, None)
    assertInitialEmptySendMetricValues(metrics)
  }

  protected def assertWatermarkMetricsValue(
      metrics: CommitmentSenderMetrics,
      expectedTimepoint: Option[Timepoint],
  ): Assertion = {
    metrics.watermarkOffset.getValue shouldBe expectedTimepoint.fold(0L)(_.offset.unwrap)
    metrics.watermarkTimestamp.getValue shouldBe expectedTimepoint.fold(
      CantonTimestamp.MinValue.toMicros
    )(_.recordTime.toMicros)
  }

  protected def assertCounterMetricValue(counter: Counter, expectedValue: Long)(implicit
      pos: Position
  ): Assertion =
    TestCommitmentMetrics.counterValue(counter)(
      AcsCommitmentSender.metricsContext,
      pos,
    ) shouldBe expectedValue
}
