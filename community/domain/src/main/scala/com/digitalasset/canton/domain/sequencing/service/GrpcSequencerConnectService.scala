// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v0.SequencerConnect.{GetDomainId, GetDomainParameters}
import com.digitalasset.canton.domain.api.{v0 => proto}
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.protocol.v0.{ServiceAgreement => protoServiceAgreement}
import com.digitalasset.canton.sequencing.protocol.VerifyActiveResponse
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Sequencer connect service on gRPC
  */
class GrpcSequencerConnectService(
    domainId: DomainId,
    staticDomainParameters: StaticDomainParameters,
    cryptoApi: DomainSyncCryptoClient,
    agreementManager: Option[ServiceAgreementManager],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends proto.SequencerConnectServiceGrpc.SequencerConnectService
    with GrpcHandshakeService
    with NamedLogging {

  protected val serverVersion = staticDomainParameters.protocolVersion

  def getDomainId(request: GetDomainId.Request): Future[GetDomainId.Response] =
    Future.successful(GetDomainId.Response(domainId.toProtoPrimitive))

  def getDomainParameters(
      request: GetDomainParameters.Request
  ): Future[GetDomainParameters.Response] =
    Future.successful(GetDomainParameters.Response(Option(staticDomainParameters.toProtoV0)))

  override def getServiceAgreement(
      request: proto.GetServiceAgreementRequest
  ): Future[proto.GetServiceAgreementResponse] = {
    val agreement =
      agreementManager.map(manager =>
        protoServiceAgreement(
          manager.agreement.id.toProtoPrimitive,
          manager.agreement.text.toProtoPrimitive,
        )
      )
    Future.successful(proto.GetServiceAgreementResponse(agreement))
  }

  def verifyActive(
      request: proto.SequencerConnect.VerifyActive.Request
  ): Future[proto.SequencerConnect.VerifyActive.Response] = {
    import proto.SequencerConnect.VerifyActive

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
        .fold[VerifyActive.Response.Value](
          reason => VerifyActive.Response.Value.Failure(VerifyActive.Failure(reason)),
          success => VerifyActive.Response.Value.Success(VerifyActive.Success(success.isActive)),
        )
        .map(VerifyActive.Response(_))
    }
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
