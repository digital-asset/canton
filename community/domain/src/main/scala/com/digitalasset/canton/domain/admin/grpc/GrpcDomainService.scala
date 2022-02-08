// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.admin.grpc

import cats.syntax.traverse._
import com.digitalasset.canton.domain.admin.{v0 => adminproto}
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.{StaticDomainParameters, v0}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty

import scala.concurrent.{ExecutionContext, Future}

class GrpcDomainService(
    staticDomainParameters: StaticDomainParameters,
    agreementManager: Option[ServiceAgreementManager],
)(implicit val ec: ExecutionContext)
    extends adminproto.DomainServiceGrpc.DomainService {

  override def listServiceAgreementAcceptances(
      request: Empty
  ): Future[adminproto.ServiceAgreementAcceptances] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      agreementManager
        .traverse { manager =>
          for {
            acceptances <- EitherTUtil.toFuture(CantonGrpcUtil.mapErr(manager.listAcceptances()))
          } yield adminproto.ServiceAgreementAcceptances(acceptances = acceptances.map(_.toProtoV0))
        }
        .map(_.getOrElse(adminproto.ServiceAgreementAcceptances(Seq())))
    }

  override def getDomainParameters(request: Empty): Future[v0.StaticDomainParameters] =
    Future.successful(staticDomainParameters.toProtoV0)
}
