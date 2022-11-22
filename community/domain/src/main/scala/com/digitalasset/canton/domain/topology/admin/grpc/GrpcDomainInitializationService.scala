// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.admin.grpc

import cats.data.EitherT
import com.digitalasset.canton.domain.admin.v0.DomainInitRequest
import com.digitalasset.canton.domain.admin.v0.DomainInitializationServiceGrpc.DomainInitializationService
import com.digitalasset.canton.domain.config.store.DomainNodeSequencerConfig
import com.digitalasset.canton.domain.topology.DomainTopologyManager
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainInitializationService(
    manager: DomainTopologyManager,
    start: DomainNodeSequencerConfig => EitherT[Future, String, Unit],
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends DomainInitializationService
    with NamedLogging {
  override def init(request: DomainInitRequest): Future[Empty] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      _ <- mapErr(manager.isInitializedET(mustHaveActiveMediator = false))
      configP <- mapErr(ProtoConverter.required("config", request.config))
      config <- mapErrNew(wrapErr(DomainNodeSequencerConfig.fromProtoV0(configP)))
      _ <- mapErr(start(config))
    } yield Empty()
    EitherTUtil.toFuture(res)
  }
}
