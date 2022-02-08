// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.OpenTelemetrySdkAutoConfiguration
import io.opentelemetry.sdk.autoconfigure.spi.SdkTracerProviderConfigurer
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.export.{BatchSpanProcessor, SimpleSpanProcessor}
import io.opentelemetry.sdk.trace.samplers.Sampler
import io.opentelemetry.sdk.trace.{SdkTracerProvider, SdkTracerProviderBuilder}
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/** Provides tracer for span reporting and takes care of closing resources
  */
trait TracerProvider extends AutoCloseable {
  def tracer: Tracer
  def openTelemetry: OpenTelemetry
}

/** Generates traces and reports using given exporter
  */
private[tracing] class ReportingTracerProvider(
    exporter: SpanExporter,
    name: String,
    attributes: Map[String, String] = Map(),
) extends TracerProviderWithBuilder(
      SdkTracerProvider.builder
        .addSpanProcessor(SimpleSpanProcessor.create(exporter)),
      name,
      attributes,
    )

private[tracing] class TracerProviderWithBuilder(
    builder: SdkTracerProviderBuilder,
    name: String,
    attributes: Map[String, String] = Map(),
) extends TracerProvider {
  private val tracerProvider = {
    val attrs = attributes
      .foldRight(Attributes.builder()) { case ((key, value), builder) =>
        builder.put(s"canton.$key", value)
      }
      .put(ResourceAttributes.SERVICE_NAME, name)
      .build()
    val serviceNameResource = Resource.create(attrs)
    builder
      .setResource(Resource.getDefault.merge(serviceNameResource))
      .build
  }

  override val openTelemetry: OpenTelemetry =
    OpenTelemetrySdk.builder
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .setTracerProvider(tracerProvider)
      .build

  override val tracer: Tracer = openTelemetry.getTracer(getClass.getName)

  override def close(): Unit = tracerProvider.close()
}

/** Generates traces but does not report
  */
object NoReportingTracerProvider extends ReportingTracerProvider(NoopSpanExporter, "no-reporting") {
  override def close(): Unit = ()
}

private object NoopSpanExporter extends SpanExporter {
  override def `export`(spans: util.Collection[SpanData]): CompletableResultCode =
    CompletableResultCode.ofSuccess()
  override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()
}

/** This implements a service provider interface (SPI) such that the configure method gets called
  * by OpenTelemetrySdkAutoConfiguration.initialize() and passes the tracer provider builder that contains all the
  * system properties as described under at https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure
  * We capture this builder so that we can reuse it multiple times with different service names instead of only using the
  * global one (GlobalOpenTelemetry.get()).
  * Notice that for this to be picked up, there is a file that contains this class's fully qualified name under
  * resources/META-INF/services.
  */
private class TracerProviderConfigurer() extends SdkTracerProviderConfigurer {
  override def configure(tracerProvider: SdkTracerProviderBuilder): Unit = {
    Autoconfigure.autoconfigureBuilder.set(Some(tracerProvider))
  }
}

private object Autoconfigure {
  // the TracerProviderConfigurer above is responsible for making sure this gets set once OpenTelemetrySdkAutoConfiguration.initialize() gets run
  val autoconfigureBuilder = new AtomicReference[Option[SdkTracerProviderBuilder]](None)
  val isEnabled: Boolean = sys.props.contains("otel.traces.exporter")
  if (isEnabled) {
    // set default propagator, otherwise the ledger-api-client interceptor won't propagate any information
    sys.props.getOrElseUpdate("otel.propagators", "tracecontext")
    OpenTelemetrySdkAutoConfiguration.initialize()
  } else
    GlobalOpenTelemetry.set(
      OpenTelemetrySdk.builder
        // also set default propagator here for the same reason as above
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .build()
    )
  private lazy val builder: SdkTracerProviderBuilder =
    autoconfigureBuilder
      .get()
      .getOrElse(
        sys.error(
          "Attempted to create OpenTelemetry tracer using Autoconfiguration but the expected provider has not been set. Likely due to service provider error."
        )
      )

  class TracerProvider(name: String) extends TracerProviderWithBuilder(builder, name)
}

object TracerProvider {
  object Factory {
    def apply(config: TracingConfig.Tracer, name: String): TracerProvider =
      if (Autoconfigure.isEnabled) new Autoconfigure.TracerProvider(name)
      else {
        val exporter = createExporter(config.exporter)
        val sampler = createSampler(config.sampler)
        val builder =
          // important to use batch span processor instead of simple span processor here because otherwise problems appear
          // with spans that are created inside grpc interceptors
          SdkTracerProvider.builder
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .setSampler(sampler)
        new TracerProviderWithBuilder(builder, name)
      }

    private def createExporter(config: TracingConfig.Exporter): SpanExporter = config match {
      case TracingConfig.Exporter.Jaeger(address, port) =>
        val jaegerChannel: ManagedChannel =
          ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        JaegerGrpcSpanExporter.builder
          .setChannel(jaegerChannel)
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
}
