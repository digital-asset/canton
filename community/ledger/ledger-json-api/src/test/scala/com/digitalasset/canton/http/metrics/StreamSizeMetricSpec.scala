// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise}
import scala.util.Try

class StreamSizeMetricSpec
    extends AsyncWordSpec
    with PekkoBeforeAndAfterAll
    with Matchers
    with MetricValues {

  private val mc: MetricsContext = MetricsContext.Empty

  private def newHistogram(): Histogram =
    InMemoryMetricsFactory.histogram(
      MetricInfo(MetricName("test", "bytes"), "", MetricQualification.Debug)
    )(mc)

  // Runs the source and returns the values of the histogram as seen by the sink
  // at the moment the stream terminates, i.e. before the materialized future completes.
  private def valuesAtTermination(
      source: Source[String, ?],
      histogram: Histogram,
  ): Future[Seq[Long]] = {
    val valuesAtTermination = Promise[Seq[Long]]()
    StreamSizeMetric
      .recordTotalSize(source, histogram)(_.length.toLong)(mc)
      .runWith(Sink.onComplete(_ => valuesAtTermination.complete(Try(histogram.values))))
    valuesAtTermination.future
  }

  "recordTotalSize" should {
    "pass the elements through unchanged" in {
      StreamSizeMetric
        .recordTotalSize(Source(List("ab", "cde")), newHistogram())(_.length.toLong)(mc)
        .runWith(Sink.seq)
        .map(_ shouldBe Seq("ab", "cde"))
    }

    "record the total size before the stream completes downstream" in {
      val histogram = newHistogram()
      valuesAtTermination(Source(List("ab", "cde")), histogram).map(_ shouldBe Seq(5L))
    }

    "record the size transferred before the stream fails" in {
      val histogram = newHistogram()
      valuesAtTermination(
        Source(List("ab", "cde")).concat(Source.failed(new RuntimeException("test failure"))),
        histogram,
      ).map(_ shouldBe Seq(5L))
    }
  }
}
