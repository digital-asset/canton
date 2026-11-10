// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.HistogramDefinition
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.api.{
  HistogramInventory,
  MetricName,
  MetricQualification,
  MetricsContext,
  MetricsInfoFilter,
}
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

class OpenTelemetryViewsSpec extends AnyWordSpec with Matchers {

  private val prefix = MetricName("test")
  private val buckets = Seq(0.1d, 0.2d, 0.3d)

  /** Records one value on the timer and on the byte histogram of the inventory, and returns the
    * bucket boundaries each instrument ended up with, keyed by instrument name.
    */
  private def boundariesOf(
      histogramConfigs: Seq[HistogramDefinition]
  ): Map[String, Seq[Double]] = {
    val inventory = new HistogramInventory()
    val timerItem =
      Item(prefix :+ "latency", "a timer", MetricQualification.Latency)(inventory)
    val byteItem =
      Item(prefix :+ Histogram.Bytes, "a byte histogram", MetricQualification.Traffic)(inventory)

    val reader = InMemoryMetricReader.create()
    val provider = OpenTelemetryViews
      .addViewsToProvider(
        SdkMeterProvider.builder.registerMetricReader(reader),
        testingSupportAdhocMetrics = false,
        inventory,
        new MetricsInfoFilter(Seq.empty, MetricQualification.All.toSet),
        histogramConfigs,
      )
      .build()

    val factory = new OpenTelemetryMetricsFactory(
      provider.get("test"),
      inventory.registered().map(_.name.toString).toSet,
      onlyLogMissingHistograms = None,
    )
    factory
      .timer(timerItem.info)(MetricsContext.Empty)
      .update(1L, TimeUnit.SECONDS)
    factory.histogram(byteItem.info)(MetricsContext.Empty).update(1L)(MetricsContext.Empty)

    reader
      .collectAllMetrics()
      .asScala
      .map(metric =>
        metric.getName -> metric.getHistogramData.getPoints.asScala.head.getBoundaries.asScala
          .map(_.doubleValue())
          .toSeq
      )
      .toMap
  }

  private val timerInstrument = "test.latency.duration.seconds"
  private val byteInstrument = "test.bytes"

  "the registered views" should {

    "give a timer the boundaries configured for its metric name" in {
      val collected = boundariesOf(
        Seq(HistogramDefinition(prefix :+ "latency", HistogramDefinition.Buckets(buckets)))
      )
      collected(timerInstrument) shouldBe buckets
    }

    "give a timer the boundaries of a wildcard configuration, as it gives a histogram" in {
      val collected =
        boundariesOf(Seq(HistogramDefinition("*", HistogramDefinition.Buckets(buckets))))
      collected(timerInstrument) shouldBe buckets
      collected(byteInstrument) shouldBe buckets
    }

    "fall back to Canton's own seconds-scaled boundaries for a timer" in {
      val collected = boundariesOf(Seq.empty)
      // The default of `addViewsToProvider`, not the millisecond-scaled boundaries of the SDK.
      collected(timerInstrument) should contain allOf (0.01d, 10d)
      collected(timerInstrument).count(_ < 1d) should be > 1
    }

    "still give a byte histogram its configured boundaries" in {
      val collected =
        boundariesOf(
          Seq(HistogramDefinition(prefix :+ Histogram.Bytes, HistogramDefinition.Buckets(buckets)))
        )
      collected(byteInstrument) shouldBe buckets
    }
  }

}
