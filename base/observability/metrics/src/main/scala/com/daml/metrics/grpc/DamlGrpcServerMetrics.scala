// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.grpc

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, LabeledMetricsFactory}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

import scala.collection.concurrent.TrieMap

class DamlGrpcServerHistograms(implicit
    inventory: HistogramInventory
) {
  private[grpc] val prefix = MetricName.Daml :+ "grpc" :+ "server"

  val damlGrpcServerCallTimer: Item = Item(
    MetricName.Daml :+ "grpc",
    MetricName("server"),
    summary = "Distribution of the durations of serving gRPC requests.",
    qualification = MetricQualification.Latency,
  )
  val damlGrpcServerReceived: Item = Item(
    prefix :+ "messages" :+ "received",
    Histogram.Bytes,
    summary = "Distribution of payload sizes in gRPC messages received (both unary and streaming).",
    qualification = MetricQualification.Traffic,
  )
  val damlGrpcServerSent: Item = Item(
    prefix :+ "messages" :+ "sent",
    Histogram.Bytes,
    summary = "Distribution of payload sizes in gRPC messages sent (both unary and streaming).",
    qualification = MetricQualification.Traffic,
  )

}

class DamlGrpcServerMetrics(
    metricsFactory: LabeledMetricsFactory,
    service: String,
)(implicit
    baseMetricsContext: MetricsContext = MetricsContext.Empty
) extends GrpcServerMetrics {

  private val histograms = new DamlGrpcServerHistograms()(new HistogramInventory)

  private implicit val grpcMetricsContext: MetricsContext = MetricsContext(
    Map("service" -> service)
  )

  private val activeRequestsPrefix = MetricName.Daml :+ "grpc" :+ "server" :+ "requests"
  private val activeRequestsLabels = Map(
    "method" -> "The method / service name limited.",
    "service" -> "The API the method belongs to",
    "api" -> "The API the method belongs to",
  )

  private val activeForDocs: Gauge[Int] = NoOpGauge(
    MetricInfo(
      activeRequestsPrefix :+ "active",
      summary = "Currently active requests.",
      description = """Currently pending GRPC requests. These can be streams or unary requests.""",
      qualification = MetricQualification.Traffic,
      labelsWithDescription = activeRequestsLabels,
    ),
    0,
  )

  private val limitForDocs: Gauge[Int] = NoOpGauge(
    MetricInfo(
      activeRequestsPrefix :+ "limit",
      summary = "Limit for concurrent requests per method.",
      description = """Limits for concurrent requests. These can be streams or unary requests.""",
      qualification = MetricQualification.Traffic,
      labelsWithDescription = activeRequestsLabels,
    ),
    0,
  )

  val rejections: Counter = metricsFactory.counter(
    MetricInfo(
      activeRequestsPrefix :+ "rejections",
      summary = "Number of rejected requests due to active request limits.",
      description =
        "Counts the number of requests rejected because the active request limit was reached.",
      qualification = MetricQualification.Saturation,
      labelsWithDescription = activeRequestsLabels,
    )
  )(baseMetricsContext)

  def mkContext(api: String, methodName: String): MetricsContext =
    baseMetricsContext.withExtraLabels("api" -> api, "method" -> methodName, "service" -> service)

  private val activeAndLimitGauges = new TrieMap[String, (Gauge[Int], Gauge[Int])]()

  def getActiveAndLimitGauge(api: String, methodName: String): (Gauge[Int], Gauge[Int]) =
    activeAndLimitGauges.getOrElseUpdate(
      api + "::" + methodName, {
        val mc = mkContext(api, methodName)
        (
          metricsFactory.gauge(activeForDocs.info, 0)(mc),
          metricsFactory.gauge(limitForDocs.info, 0)(mc),
        )
      },
    )

  override val callTimer: MetricHandle.Timer =
    metricsFactory.timer(histograms.damlGrpcServerCallTimer.info)(grpcMetricsContext)
  override val messagesSent: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "messages" :+ "sent",
      summary = "Total number of gRPC messages sent (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )(grpcMetricsContext)
  override val messagesReceived: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "messages" :+ "received",
      summary = "Total number of gRPC messages received (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )(grpcMetricsContext)
  override val messagesSentSize: MetricHandle.Histogram = metricsFactory.histogram(
    histograms.damlGrpcServerSent.info
  )(grpcMetricsContext)
  override val messagesReceivedSize: MetricHandle.Histogram = metricsFactory.histogram(
    histograms.damlGrpcServerReceived.info
  )(grpcMetricsContext)
  override val callsStarted: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "started",
      summary = "Total number of started gRPC requests (on either type of connection).",
      qualification = MetricQualification.Traffic,
    )
  )(grpcMetricsContext)
  override val callsHandled: MetricHandle.Meter = metricsFactory.meter(
    MetricInfo(
      histograms.prefix :+ "handled",
      summary = "Total number of handled gRPC requests.",
      qualification = MetricQualification.Traffic,
    )
  )(grpcMetricsContext)

}
