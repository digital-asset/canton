// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.{TraceContextGrpc, TracingConfig}
import io.grpc.ServerInterceptors.intercept
import io.grpc.ServerServiceDefinition

import scala.util.chaining._

trait CantonServerInterceptors {
  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition
}

class CantonCommunityServerInterceptors(
    tracingConfig: TracingConfig,
    logMessagePayloads: Boolean,
    loggerFactory: NamedLoggerFactory,
) extends CantonServerInterceptors {
  private def interceptForLogging(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    if (withLogging) {
      intercept(service, new ApiRequestLogger(loggerFactory, logMessagePayloads))
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

  def addAllInterceptors(
      service: ServerServiceDefinition,
      withLogging: Boolean,
  ): ServerServiceDefinition =
    service
      .pipe(interceptForLogging(_, withLogging))
      .pipe(addTraceContextInterceptor)
}
