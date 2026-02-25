// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.sampleservice.implementations

import com.daml.grpc.sampleservice.HelloServiceResponding
import com.daml.platform.hello.HelloServiceGrpc.HelloService
import com.daml.platform.hello.{HelloRequest, HelloResponse, HelloServiceGrpc}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition, Status, StatusRuntimeException}

import scala.concurrent.ExecutionContext

class HelloServiceReferenceImplementation
    extends HelloService
    with HelloServiceResponding
    with BindableService
    with AutoCloseable {

  override def close(): Unit = ()

  @SuppressWarnings(Array("org.wartremover.warts.GlobalExecutionContext"))
  override def bindService(): ServerServiceDefinition =
    HelloServiceGrpc.bindService(this, ExecutionContext.global)

  override def serverStreaming(
      request: HelloRequest,
      responseObserver: StreamObserver[HelloResponse],
  ): Unit =
    validateRequest(request) match {
      case Left(err) =>
        responseObserver.onError(err)
      case Right(_) =>
        for (i <- 1.to(request.reqInt)) responseObserver.onNext(HelloResponse(i))
        responseObserver.onCompleted()
    }

  private def validateRequest(request: HelloRequest): Either[StatusRuntimeException, Unit] =
    Either.cond(
      request.reqInt >= 0,
      (),
      Status.INVALID_ARGUMENT
        .withDescription("request cannot be negative")
        .asRuntimeException(),
    )

}
