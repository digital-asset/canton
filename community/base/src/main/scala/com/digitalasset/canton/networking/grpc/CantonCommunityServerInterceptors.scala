// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.daml.metrics.grpc.GrpcMetricsServerInterceptor
import com.digitalasset.canton.config.{ActiveRequestLimitsConfig, ApiLoggingConfig}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.ActiveRequestsMetrics.GrpcServerMetricsX
import com.digitalasset.canton.networking.grpc.ratelimiting.ActiveRequestCounterInterceptor
import com.digitalasset.canton.tracing.{TraceContextGrpc, TracingConfig}
import io.grpc.ServerInterceptors.intercept
import io.grpc.{ServerInterceptor, ServerServiceDefinition}

import scala.util.chaining.*

trait CantonServerInterceptors {
  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition

  def activeRequestCounter: Option[ActiveRequestCounterInterceptor]
}

class CantonCommunityServerInterceptors(
    api: String,
    tracingConfig: TracingConfig,
    apiLoggingConfig: ApiLoggingConfig,
    loggerFactory: NamedLoggerFactory,
    grpcMetrics: GrpcServerMetricsX,
    additionalInterceptors: Seq[ServerInterceptor] = Seq.empty,
    requestLimits: Option[ActiveRequestLimitsConfig],
) extends CantonServerInterceptors {

  override val activeRequestCounter: Option[ActiveRequestCounterInterceptor] = requestLimits.map {
    limits =>
      val (_, requestMetrics) = grpcMetrics
      new ActiveRequestCounterInterceptor(
        api,
        limits.active,
        limits.warnOnUndefinedLimits,
        limits.throttleLoggingRatePerSecond,
        requestMetrics,
        loggerFactory,
      )
  }

  private def interceptForLogging(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    if (withLogging) {
      intercept(service, new GrpcRequestLoggingInterceptor(loggerFactory, apiLoggingConfig))
    } else {
      service
    }

  private def addTraceContextInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition =
    tracingConfig.propagation match {
      case TracingConfig.Propagation.Disabled => service
      case TracingConfig.Propagation.Enabled =>
        intercept(service, TraceContextGrpc.serverInterceptor)
    }

  private def addMetricsInterceptor(
      service: ServerServiceDefinition
  ): ServerServiceDefinition =
    intercept(service, new GrpcMetricsServerInterceptor(grpcMetrics._1))

  private def addLimitInterceptor(service: ServerServiceDefinition): ServerServiceDefinition =
    activeRequestCounter.fold(service) { limits =>
      intercept(service, limits)
    }

  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    service
      .pipe(interceptForLogging(_, withLogging))
      .pipe(addTraceContextInterceptor)
      .pipe(addMetricsInterceptor)
      .pipe(addLimitInterceptor)
      .pipe(s => additionalInterceptors.foldLeft(s)((acc, i) => intercept(acc, i)))
}
