// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import java.util
import java.util.concurrent.LinkedBlockingQueue

import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.trace.data.{EventData, SpanData}

class TestTelemetrySetup() extends AutoCloseable {
  private lazy val testExporter = new TestTelemetry.TestExporter()
  private val tracerProvider = new ReportingTracerProvider(testExporter, "test")

  val tracer: Tracer = tracerProvider.tracer
  def reportedSpans(): List[SpanData] = testExporter.allSpans()

  override def close(): Unit = tracerProvider.close()
}

object TestTelemetry {
  class TestExporter extends SpanExporter {
    val queue = new LinkedBlockingQueue[SpanData]
    override def `export`(spans: util.Collection[SpanData]): CompletableResultCode = {
      queue.addAll(spans)
      CompletableResultCode.ofSuccess()
    }
    def allSpans(): List[SpanData] = {
      import scala.jdk.CollectionConverters._
      queue.iterator().asScala.toList.reverse
    }
    override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
    override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
  }

  def eventsOrderedByTime(spans: SpanData*): List[EventData] = {
    import scala.jdk.CollectionConverters._
    spans.toList
      .flatMap(_.getEvents.asScala)
      .map(ev => (ev, ev.getEpochNanos))
      .sortBy(_._2)
      .map(_._1)
  }
}
