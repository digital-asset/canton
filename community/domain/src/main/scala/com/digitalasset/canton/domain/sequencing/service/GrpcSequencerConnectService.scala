// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.api.v0.SequencerConnect.{GetDomainId, GetDomainParameters}
import com.digitalasset.canton.domain.api.v0.{
  SequencerConnectServiceGrpc,
  SequencerConnect => proto,
}
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.domain.service.HandshakeValidator
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.VerifyActiveResponse
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

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
    with NamedLogging {

  private val currentVersion = ProtocolVersion.current
  private val handshakeValidator = new HandshakeValidator(currentVersion, loggerFactory)

  /** The handshake will check whether the client's version is compatible with the one of this domain.
    * This should be called before attempting to connect to the domain to make sure they can operate together.
    */
  def handshake(
      request: com.digitalasset.canton.protocol.v0.Handshake.Request
  ): Future[com.digitalasset.canton.protocol.v0.Handshake.Response] = {
    import com.digitalasset.canton.protocol.v0

    val response = handshakeValidation(request)
      .fold[v0.Handshake.Response.Value](
        failure => v0.Handshake.Response.Value.Failure(v0.Handshake.Failure(failure)),
        _ => v0.Handshake.Response.Value.Success(v0.Handshake.Success()),
      )
    Future.successful(v0.Handshake.Response(serverVersion = currentVersion.fullVersion, response))
  }

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

  private def handshakeValidation(
      request: com.digitalasset.canton.protocol.v0.Handshake.Request
  ): Either[String, Unit] =
    handshakeValidator.clientIsCompatible(
      request.clientProtocolVersions,
      request.minimumProtocolVersion,
    )

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
