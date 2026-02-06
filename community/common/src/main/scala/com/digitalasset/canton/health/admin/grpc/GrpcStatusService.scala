// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health.admin.grpc

import better.files.*
import com.digitalasset.canton.admin.health.v30
import com.digitalasset.canton.admin.health.v30.{HealthDumpRequest, HealthDumpResponse}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NodeLoggingUtil}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.GrpcStreamingUtils
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class GrpcStatusService(
    healthDump: File => Future[Unit],
    processingTimeout: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.StatusServiceGrpc.StatusService
    with NamedLogging {

  override def healthDump(
      request: HealthDumpRequest,
      responseObserver: StreamObserver[HealthDumpResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    logger.info(s"Streaming health dump with chunk size = ${request.chunkSize}")
    GrpcStreamingUtils.streamToClientFromFile(
      (file: File) => healthDump(file),
      responseObserver,
      byteString => HealthDumpResponse(byteString),
      processingTimeout.unbounded.duration,
    )
  }

  override def setLogLevel(request: v30.SetLogLevelRequest): Future[v30.SetLogLevelResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    logger.info(s"Changing log level to ${request.level}")
    NodeLoggingUtil.setLevel(level = request.level)
    Future.successful(v30.SetLogLevelResponse())
  }
}
