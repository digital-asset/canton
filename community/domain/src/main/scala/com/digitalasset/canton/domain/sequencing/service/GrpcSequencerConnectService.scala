// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v0.SequencerConnect.{GetDomainId, GetDomainParameters}
import com.digitalasset.canton.domain.api.v0.{
  SequencerConnectServiceGrpc,
  SequencerConnect => proto,
}
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.VerifyActiveResponse
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    domainId: DomainId,
    staticDomainParameters: StaticDomainParameters,
    cryptoApi: DomainSyncCryptoClient,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerConnectServiceGrpc.SequencerConnectService
    with GrpcHandshakeService
    with NamedLogging {

  protected val serverVersion = staticDomainParameters.protocolVersion

  def getDomainId(request: GetDomainId.Request): Future[GetDomainId.Response] =
    Future.successful(proto.GetDomainId.Response(domainId.toProtoPrimitive))

  def getDomainParameters(
      request: GetDomainParameters.Request
  ): Future[GetDomainParameters.Response] =
    Future.successful(proto.GetDomainParameters.Response(Option(staticDomainParameters.toProtoV0)))

  def verifyActive(request: proto.VerifyActive.Request): Future[proto.VerifyActive.Response] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val resultF = for {
        participant <- EitherT.fromEither[Future](getParticipantFromGrpcContext())
        isActive <- EitherT(
          cryptoApi.ips.currentSnapshotApproximation
            .isParticipantActive(participant)
            .map(_.asRight[String])
        )
      } yield VerifyActiveResponse.Success(isActive)

      resultF
        .fold[proto.VerifyActive.Response.Value](
          reason => proto.VerifyActive.Response.Value.Failure(proto.VerifyActive.Failure(reason)),
          success =>
            proto.VerifyActive.Response.Value.Success(proto.VerifyActive.Success(success.isActive)),
        )
        .map(proto.VerifyActive.Response(_))
    }

  /*
   Note: we only get the participantId from the context; we have no idea
   whether the member is authenticated or not.
   */
  private def getParticipantFromGrpcContext(): Either[String, ParticipantId] =
    IdentityContextHelper.getCurrentStoredMember
      .toRight("Unable to find participant id in gRPC context")
      .flatMap {
        case participantId: ParticipantId => Right(participantId)
        case member => Left(s"Expecting participantId ; found $member")
      }
}
