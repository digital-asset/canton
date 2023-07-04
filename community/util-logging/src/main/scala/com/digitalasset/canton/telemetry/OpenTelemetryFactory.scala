// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.daml.metrics.HistogramDefinition
import com.daml.telemetry.OpenTelemetryOwner.addViewsToProvider
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.metrics.OpenTelemetryOnDemandMetricsReader
import com.digitalasset.canton.tracing.{NoopSpanExporter, TraceContext, TracingConfig}
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.exporter.prometheus.PrometheusCollector
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder
import io.opentelemetry.sdk.trace.`export`.{
  BatchSpanProcessor,
  BatchSpanProcessorBuilder,
  SpanExporter,
}
import io.opentelemetry.sdk.trace.samplers.Sampler

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.concurrent.blocking
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.chaining.scalaUtilChainingOps

object OpenTelemetryFactory {

  private def withSysProps(createOpenTelemetry: Boolean => OpenTelemetrySdk): OpenTelemetrySdk = {
    final case class WriteWithRevert(key: String, newValue: String) {
      val oldValue = sys.props.get(key)
      sys.props.getOrElseUpdate(key, newValue): Unit
      def revert() = {
        sys.props.updateWith(key)(_ => oldValue): Unit
      }
    }

    // TODO (i13757)
    // The reliance on system properties or config to setup otel is very brittle
    // It breaks down in unit tests where suites are run in parallel but may require different settings
    // We implement here a mechanism that prohibits reentrancy and cleans up sys.props when done
    blocking(synchronized {
      val configuredThroughSystemProps = sys.props.contains("otel.traces.exporter")
      // if no metrics exporter is configured then default to none instead of the oltp default used by the library
      val metricsExporter = WriteWithRevert("otel.metrics.exporter", "none")
      // if no trace exporter is configured then default to none instead of the oltp default used by the library
      val tracesExporter = WriteWithRevert("otel.traces.exporter", "none")
      // set default propagator, otherwise the ledger-api-client interceptor won't propagate any information
      val otelPropagator = WriteWithRevert("otel.propagators", "tracecontext")
      val openTelemetry = createOpenTelemetry(configuredThroughSystemProps)
      metricsExporter.revert()
      tracesExporter.revert()
      otelPropagator.revert()
      openTelemetry
    })
  }

  @nowarn("msg=deprecated")
  def initializeOpenTelemetry(
      setGlobal: Boolean,
      metricsEnabled: Boolean,
      config: TracingConfig.Tracer,
      histograms: Seq[HistogramDefinition],
      loggerFactory: NamedLoggerFactory,
  ): ConfiguredOpenTelemetry = {
    val logger: TracedLogger = loggerFactory.getTracedLogger(getClass)
    val autoconfigureBuilder =
      new AtomicReference[Option[SdkTracerProviderBuilder]](None)
    val onDemandMetricReader = new OpenTelemetryOnDemandMetricsReader
    val configuredSdk = withSysProps { configuredThroughSystemProps =>
      val suffix = if (configuredThroughSystemProps) "through sys.props" else ""
      logger.info(s"Initializing open telemetry $suffix")(TraceContext.empty)
      AutoConfiguredOpenTelemetrySdk
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

            def setBatchSize(
                batchSize: Int
            ): BatchSpanProcessorBuilder => BatchSpanProcessorBuilder =
              builder =>
                if (batchSize != 0)
                  builder.setMaxExportBatchSize(batchSize)
                else
                  builder
            def setScheduleDelay(
                scheduleDelay: FiniteDuration
            ): BatchSpanProcessorBuilder => BatchSpanProcessorBuilder = builder =>
              if (scheduleDelay != 0.millis)
                builder.setScheduleDelay(scheduleDelay.toJava)
              else
                builder

            val builder = {
              if (!configuredThroughSystemProps) {
                // important to use batch span processor instead of simple span processor here because otherwise problems appear
                // with spans that are created inside grpc interceptors
                tracerBuilder
                  .addSpanProcessor(
                    BatchSpanProcessor
                      .builder(exporter)
                      .pipe(setBatchSize(config.batchSpanProcessor.batchSize))
                      .pipe(setScheduleDelay(config.batchSpanProcessor.scheduleDelay))
                      .build()
                  )
                  .setSampler(sampler)
              } else {
                tracerBuilder
              }
            }
            autoconfigureBuilder.set(Some(builder))
            builder
        }
        .addMeterProviderCustomizer { case (builder, _) =>
          val meterProviderBuilder = addViewsToProvider(builder, histograms)
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
    }
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
    case TracingConfig.Exporter.Otlp(address, port) =>
      val httpUrl = s"http://$address:$port"
      OtlpGrpcSpanExporter.builder.setEndpoint(httpUrl).build
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
