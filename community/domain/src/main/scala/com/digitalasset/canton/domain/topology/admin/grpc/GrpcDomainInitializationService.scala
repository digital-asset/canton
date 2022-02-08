// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.admin.grpc

import cats.data.EitherT
import cats.data.EitherT.fromEither
import com.digitalasset.canton.domain.admin.v0.DomainInitializationServiceGrpc.DomainInitializationService
import com.digitalasset.canton.domain.admin.v0.DomainInitRequest
import com.digitalasset.canton.domain.config.store.DomainNodeSequencerConfig
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainInitializationService(
    manager: DomainTopologyManager,
    start: DomainNodeSequencerConfig => EitherT[Future, String, Unit],
)(implicit executionContext: ExecutionContext)
    extends DomainInitializationService {
  override def init(request: DomainInitRequest): Future[Empty] = fromGrpcContext {
    implicit traceContext =>
      for {
        managerInitialized <- manager.isInitialized
        _ <-
          if (!managerInitialized) {
            Future.failed(
              Status.FAILED_PRECONDITION
                .withDescription(
                  "Cannot initialize domain before there is at least one signing key authorized for each type of domain entity"
                )
                .asRuntimeException
            )
          } else
            EitherTUtil.toFuture(for {
              config <- fromEither[Future](for {
                configP <- ProtoConverter.required("config", request.config)
                config <- DomainNodeSequencerConfig.fromProtoV0(configP)
              } yield config)
                .leftMap(err => s"Failed to deserialize request: $err")
                .leftMap(Status.INVALID_ARGUMENT.withDescription)
                .leftMap(_.asException())
              _ <- start(config).leftMap(error =>
                Status.INTERNAL.withDescription(error).asException()
              )
            } yield ())
      } yield Empty()
  }
}
