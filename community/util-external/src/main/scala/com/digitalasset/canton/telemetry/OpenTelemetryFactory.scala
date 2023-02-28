// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.telemetry.OpenTelemetryOwner.addViewsToProvider
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.metrics.OpenTelemetryOnDemandMetricsReader
import com.digitalasset.canton.tracing.{NoopSpanExporter, TracingConfig}
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder
import io.opentelemetry.sdk.trace.`export`.{BatchSpanProcessor, SpanExporter}
import io.opentelemetry.sdk.trace.samplers.Sampler

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn

object OpenTelemetryFactory {

  @nowarn("msg=deprecated")
  def initializeOpenTelemetry(
      setGlobal: Boolean,
      metricsEnabled: Boolean,
      config: TracingConfig.Tracer,
  ): ConfiguredOpenTelemetry = {
    val autoconfigureBuilder =
      new AtomicReference[Option[SdkTracerProviderBuilder]](None)
    val configuredThroughSystemProps = sys.props.contains("otel.traces.exporter")
    // if no metrics exporter is configured then default to none instead of the oltp default used by the library
    sys.props.getOrElseUpdate("otel.metrics.exporter", "none"): Unit
    // if no trace exporter is configured then default to none instead of the oltp default used by the library
    sys.props.getOrElseUpdate("otel.traces.exporter", "none"): Unit
    // set default propagator, otherwise the ledger-api-client interceptor won't propagate any information
    sys.props.getOrElseUpdate("otel.propagators", "tracecontext"): Unit
    val onDemandMetricReader = new OpenTelemetryOnDemandMetricsReader
    val configuredSdk = AutoConfiguredOpenTelemetrySdk
      .builder()
      .addSpanExporterCustomizer { (exporter, _) =>
        // if the exporter was set through system props do nothing
        // if the exporter was not configured then return the compose exporter, which is treated as a noop exporter and ignored
        // this allows us to use the code provided exporter
        if (configuredThroughSystemProps) exporter else SpanExporter.composite()
      }
      .addTracerProviderCustomizer {
        (tracerBuilder: SdkTracerProviderBuilder, u: ConfigProperties) =>
          val exporter = createExporter(config.exporter)
          val sampler = createSampler(config.sampler)
          val builder = {
            if (!configuredThroughSystemProps)
              // important to use batch span processor instead of simple span processor here because otherwise problems appear
              // with spans that are created inside grpc interceptors
              tracerBuilder
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .setSampler(sampler)
            else {
              tracerBuilder
            }
          }
          autoconfigureBuilder.set(Some(builder))
          builder
      }
      .addMeterProviderCustomizer { case (builder, _) =>
        val meterProviderBuilder = addViewsToProvider(builder)
        /* To integrate with prometheus we're using the deprecated [[PrometheusCollector]].
         * More details about the deprecation here: https://github.com/open-telemetry/opentelemetry-java/issues/4284
         * This forces us to keep the current OpenTelemetry version (see ticket for potential paths forward).
         */
        if (metricsEnabled) {
          meterProviderBuilder
            .registerMetricReader(PrometheusCollector.create())
            .registerMetricReader(onDemandMetricReader)
        } else meterProviderBuilder
      }
      .setResultAsGlobal(setGlobal)
      // Cleaned up during environment shutdown
      .registerShutdownHook(false)
      .build()
      .getOpenTelemetrySdk
    val tracerProviderBuilder = autoconfigureBuilder
      .get()
      .getOrElse(
        sys.error(
          "Attempted to create OpenTelemetry tracer using Autoconfiguration but the expected provider has not been set. Likely due to service provider error."
        )
      )
    ConfiguredOpenTelemetry(
      openTelemetry = configuredSdk,
      tracerProviderBuilder = tracerProviderBuilder,
      onDemandMetricsReader =
        if (metricsEnabled) onDemandMetricReader else NoOpOnDemandMetricsReader$,
    )
  }

  private def createExporter(config: TracingConfig.Exporter): SpanExporter = config match {
    case TracingConfig.Exporter.Jaeger(address, port) =>
      JaegerGrpcSpanExporter.builder
        .setEndpoint(s"http://$address:$port")
        .setTimeout(30, TimeUnit.SECONDS)
        .build
    case TracingConfig.Exporter.Zipkin(address, port) =>
      val httpUrl = s"http://$address:$port/api/v2/spans"
      ZipkinSpanExporter.builder.setEndpoint(httpUrl).build
    case TracingConfig.Exporter.Disabled =>
      NoopSpanExporter
  }

  private def createSampler(config: TracingConfig.Sampler): Sampler = {
    val sampler = config match {
      case TracingConfig.Sampler.AlwaysOn(_) =>
        Sampler.alwaysOn()
      case TracingConfig.Sampler.AlwaysOff(_) =>
        Sampler.alwaysOff()
      case TracingConfig.Sampler.TraceIdRatio(ratio, _) =>
        Sampler.traceIdRatioBased(ratio)
    }
    if (config.parentBased) Sampler.parentBased(sampler) else sampler
  }

}
