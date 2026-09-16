// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsReporterConfig.Otlp
import com.digitalasset.canton.metrics.OtlpAuth.OauthClientCredentials
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.SingleUseCell
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.common.`export`.MemoryMode
import io.opentelemetry.sdk.metrics.`export`.MetricExporter
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.{Aggregation, InstrumentType}
import org.apache.pekko.actor.ActorSystem

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

/** An otel metrics reporter that exports metrics to a remote OTLP service.
  */
object OtlpReporter {

  final class ExporterFactory {

    private val pending = ListBuffer.empty[(DeferredMetricExporter, OauthClientCredentials)]

    def createExporter(config: Otlp, loggerFactory: NamedLoggerFactory): MetricExporter =
      config.auth match {
        case None => createDelegate(config, headers = None)
        case Some(auth: OauthClientCredentials) =>
          val tokenProvider =
            new DeferredValue[TokenProvider]("OTLP token provider")
          val headers = () =>
            Map("Authorization" -> tokenProvider.getOrFail.authorizationHeader()).asJava
          val exporter = new DeferredMetricExporter(
            delegate = createDelegate(config, Some(headers)),
            tokenProvider = tokenProvider,
            loggerFactory,
          )
          pending += exporter -> auth

          exporter
      }

    def initialize(
        clock: Clock
    )(implicit actorSystem: ActorSystem, executionContext: ExecutionContext): Unit =
      pending.foreach { case (exporter, auth) =>
        exporter.initialize(
          new ClientCredentialsTokenProvider(auth, clock)
        )
      }

    private def createDelegate(
        config: Otlp,
        headers: Option[() => util.Map[String, String]],
    ): MetricExporter =
      config.protocol match {
        case OtlpProtocol.Grpc =>
          val builder = OtlpGrpcMetricExporter
            .builder()
            .setEndpoint(config.endpoint)
          headers.foreach { headers =>
            builder.setHeaders(() => headers())
          }
          builder.build()

        case OtlpProtocol.HttpProtobuf =>
          val builder = OtlpHttpMetricExporter
            .builder()
            .setEndpoint(config.endpoint)
          headers.foreach { headers =>
            builder.setHeaders(() => headers())
          }
          builder.build()
      }
  }

  /** Wraps MetricExporter to allow adding authorization onto the outgoing metric exports.
    *
    * @param delegate
    * @param tokenProvider
    *   a deferred token provider (Deferred because this exporter gets created early on, even before
    *   canton's custom ExecutionContext is created)
    * @param loggerFactory
    */
  private[metrics] final class DeferredMetricExporter(
      delegate: MetricExporter,
      tokenProvider: DeferredValue[TokenProvider],
      override protected val loggerFactory: NamedLoggerFactory,
  ) extends MetricExporter
      with NamedLogging
      with NoTracing {

    private val executionContext =
      new DeferredValue[ExecutionContext]("OTLP exporter execution context")

    private val closed = new AtomicBoolean(false)

    private[metrics] def initialize(
        provider: TokenProvider
    )(implicit executionContext: ExecutionContext): Unit = {
      tokenProvider.initialize(provider)
      this.executionContext.initialize(executionContext)
    }

    override def `export`(metrics: util.Collection[MetricData]): CompletableResultCode =
      if (closed.get()) {
        CompletableResultCode.ofFailure()
      } else {
        tokenProvider.get match {
          case None =>
            CompletableResultCode.ofFailure()

          case Some(provider) =>
            val result = new CompletableResultCode()

            provider
              .ensureToken()
              .onComplete {
                case Success(_) =>
                  completeFrom(delegate.export(metrics), result)

                case Failure(exception) =>
                  logger.warn(
                    "Failed to obtain OAuth2 access token for OTLP metrics export",
                    exception,
                  )
                  result.failExceptionally(exception)
              }(executionContext.getOrFail)

            result
        }
      }

    override def flush(): CompletableResultCode =
      delegate.flush()

    override def shutdown(): CompletableResultCode = {
      closed.set(true)
      delegate.shutdown()
    }

    override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
      delegate.getAggregationTemporality(instrumentType)

    override def getDefaultAggregation(instrumentType: InstrumentType): Aggregation =
      delegate.getDefaultAggregation(instrumentType)

    override def getMemoryMode: MemoryMode = delegate.getMemoryMode

    private def completeFrom(source: CompletableResultCode, target: CompletableResultCode): Unit = {
      val _ = source.whenComplete { () =>
        if (source.isSuccess) {
          target.succeed()
        } else {
          Option(source.getFailureThrowable) match {
            case Some(exception) =>
              target.failExceptionally(exception)

            case None =>
              target.fail()
          }
        }

        ()
      }
    }
  }

  private[metrics] final class DeferredValue[A](name: String) {
    private val cell = new SingleUseCell[A]

    def initialize(value: A): Unit = require(
      cell.putIfAbsent(value).isEmpty,
      s"$name has already been initialized",
    )

    def get: Option[A] = cell.get

    def getOrFail: A = cell.getOrElse(
      throw new IllegalStateException(s"$name has not been initialized")
    )
  }
}
