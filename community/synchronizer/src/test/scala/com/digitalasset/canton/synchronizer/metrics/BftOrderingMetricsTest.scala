// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.api.opentelemetry.OpenTelemetryTimer
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.BaseTest
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration

class BftOrderingMetricsTest extends AnyWordSpec with BaseTest {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty
  private val timerInfo =
    MetricInfo(MetricName("test"), "Test timer", MetricQualification.Latency)
  private val maxDuration = Duration.ofNanos(Long.MaxValue)

  "updateTimer" should {
    "record non-negative durations up to the nanosecond conversion limit" in {
      Seq(Duration.ZERO, Duration.ofSeconds(1), maxDuration).foreach { duration =>
        val histogram = mock[DoubleHistogram]
        val timer = OpenTelemetryTimer(timerInfo, histogram, metricsContext)

        BftOrderingMetrics.updateTimer(timer, duration)

        verify(histogram).record(eqTo(duration.toNanos.toDouble / 1e9), any[Attributes])
      }
    }

    "skip negative durations and durations above the nanosecond conversion limit" in {
      Seq(Duration.ofNanos(-1), maxDuration.plusNanos(1), Duration.ofSeconds(Long.MaxValue))
        .foreach { duration =>
          val histogram = mock[DoubleHistogram]
          val timer = OpenTelemetryTimer(timerInfo, histogram, metricsContext)

          BftOrderingMetrics.updateTimer(timer, duration)

          verify(histogram, never).record(any[Double], any[Attributes])
        }
    }
  }
}
