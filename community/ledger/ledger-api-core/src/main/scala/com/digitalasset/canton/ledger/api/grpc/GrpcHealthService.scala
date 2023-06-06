// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.api.grpc.GrpcHealthService.*
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.error.DamlContextualizedErrorLogger
import io.grpc.ServerServiceDefinition
import io.grpc.health.v1.health.{HealthCheckRequest, HealthCheckResponse, HealthGrpc}
import io.grpc.stub.StreamObserver

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class GrpcHealthService(
    healthChecks: HealthChecks,
    maximumWatchFrequency: FiniteDuration = 1.second,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends HealthGrpc.Health
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(getClass)
  protected val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def bindService(): ServerServiceDefinition =
    HealthGrpc.bindService(this, executionContext)

  override def check(request: HealthCheckRequest): Future[HealthCheckResponse] =
    Future.fromTry(matchResponse(serviceFrom(request)))

  override def watch(
      request: HealthCheckRequest,
      responseObserver: StreamObserver[HealthCheckResponse],
  ): Unit = registerStream(responseObserver) {
    Source
      .fromIterator(() =>
        Iterator.continually(matchResponse(serviceFrom(request)).fold(throw _, identity))
      )
      .throttle(1, per = maximumWatchFrequency)
      .via(DropRepeated())
  }

  private def matchResponse(componentName: Option[String]): Try[HealthCheckResponse] =
    componentName
      .collect {
        case component if !healthChecks.hasComponent(component) =>
          Failure(
            invalidArgument(s"Component $component does not exist.")(contextualizedErrorLogger)
          )
      }
      .getOrElse {
        if (healthChecks.isHealthy(componentName)) Success(servingResponse)
        else Success(notServingResponse)
      }
}

object GrpcHealthService {
  private[grpc] val servingResponse =
    HealthCheckResponse(HealthCheckResponse.ServingStatus.SERVING)

  private[grpc] val notServingResponse =
    HealthCheckResponse(HealthCheckResponse.ServingStatus.NOT_SERVING)

  private def serviceFrom(request: HealthCheckRequest): Option[String] = {
    Option(request.service).filter(_.nonEmpty)
  }
}
