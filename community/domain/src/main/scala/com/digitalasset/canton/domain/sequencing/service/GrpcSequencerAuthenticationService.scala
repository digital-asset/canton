// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either._
import com.daml.error.{ErrorCategory, ErrorCode, Explanation}
import com.digitalasset.canton.crypto.{Nonce, Signature}
import com.digitalasset.canton.domain.Domain.GrpcSequencerAuthenticationErrorGroup
import com.digitalasset.canton.domain.api.v0.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.domain.api.v0.{Authentication, Challenge}
import com.digitalasset.canton.domain.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerAuthenticationService.{
  SequencerAuthenticationFailure,
  SequencerAuthenticationFaultyOrMalicious,
}
import com.digitalasset.canton.domain.service.HandshakeValidator
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.AuthenticationError
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAuthenticationService(
    authenticationService: MemberAuthenticationService,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerAuthenticationService
    with NamedLogging {

  private val handshakeValidator = new HandshakeValidator(protocolVersion, loggerFactory)

  /** This will complete the participant authentication process using the challenge information and returning a token
    * to be used for further authentication.
    */
  override def authenticate(request: Authentication.Request): Future[Authentication.Response] =
    fromGrpcContext { implicit traceContext =>
      (for {
        member <- eitherT(deserializeMember(request.member))
        signature <- eitherT(
          ProtoConverter
            .parseRequired(Signature.fromProtoV0, "signature", request.signature)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )
        providedNonce <- eitherT(
          Nonce
            .fromProtoPrimitive(request.nonce)
            .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.toString))
        )
        tokenAndExpiry <- authenticationService
          .validateSignature(member, signature, providedNonce)
          .leftMap(handleAuthError)
      } yield tokenAndExpiry)
        .fold[Authentication.Response.Value](
          error => {
            val sensitive =
              if (
                error.getCode == Status.Code.INTERNAL || error.getCode == Status.Code.INVALID_ARGUMENT
              ) {
                // create error message to appropriately log this incident
                SequencerAuthenticationFaultyOrMalicious
                  .AuthenticationFailure(request.member, error)
                true
              } else {
                // create error message to appropriate log this incident
                SequencerAuthenticationFailure
                  .AuthenticationFailure(request.member, error)
                false
              }
            Authentication.Response.Value.Failure(
              Authentication.Failure(
                code = error.getCode.value(),
                reason = if (sensitive) "Bad authentication request" else error.getDescription,
              )
            )
          },
          { case AuthenticationTokenWithExpiry(token, expiry) =>
            Authentication.Response.Value.Success(
              Authentication
                .Success(token = token.toProtoPrimitive, expiresAt = Some(expiry.toProtoPrimitive))
            )
          },
        )
        .map(Authentication.Response(_))
    }

  /** This is will return a random number (nonce) plus the fingerprint of the key the participant needs to use to complete
    * the authentication process with this domain.
    * A handshake check is also done here to make sure that no participant can start authenticating without doing this check.
    * While the pure handshake can be called without any prior setup, this endpoint will only work after topology state
    * for the participant has been pushed to this domain.
    */
  override def challenge(request: Challenge.Request): Future[Challenge.Response] = fromGrpcContext {
    implicit traceContext =>
      (for {
        _ <- eitherT(handshakeValidation(request))
        member <- eitherT(deserializeMember(request.member))
        result <- authenticationService
          .generateNonce(member)
          .leftMap(handleAuthError)
      } yield result)
        .fold[Challenge.Response.Value](
          error => {
            val sensitive =
              if (
                error.getCode == Status.Code.INTERNAL || error.getCode == Status.Code.INVALID_ARGUMENT
              ) {
                SequencerAuthenticationFaultyOrMalicious.ChallengeFailure(
                  request.member,
                  request.memberProtocolVersions,
                  error,
                )
                true
              } else {
                SequencerAuthenticationFailure.ChallengeFailure(
                  request.member,
                  request.memberProtocolVersions,
                  error,
                )
                false
              }
            Challenge.Response.Value.Failure(
              Challenge.Failure(
                code = error.getCode.value(),
                reason = if (sensitive) "Bad challenge request" else error.getDescription,
              )
            )
          },
          { case (nonce, fingerprints) =>
            Challenge.Response.Value.Success(
              Challenge.Success(
                protocolVersion.fullVersion,
                nonce.toProtoPrimitive,
                fingerprints.map(_.unwrap).toList,
              )
            )
          },
        )
        .map(Challenge.Response(_))
  }

  private def handleAuthError(err: AuthenticationError): Status = {
    def maliciousOrFaulty(): Status =
      Status.INTERNAL.withDescription(err.reason)
    err match {
      case MemberAuthentication.ParticipantDisabled(_) =>
        Status.PERMISSION_DENIED.withDescription(err.reason)
      case MemberAuthentication.ServiceAgreementAcceptanceError(_, _) =>
        Status.FAILED_PRECONDITION.withDescription(err.reason)
      case MemberAuthentication.NonMatchingDomainId(_, _) =>
        Status.FAILED_PRECONDITION.withDescription(err.reason)
      case MemberAuthentication.NoKeysRegistered(_) => maliciousOrFaulty()
      case MemberAuthentication.FailedToSign(_, _) => maliciousOrFaulty()
      case MemberAuthentication.MissingNonce(_) => maliciousOrFaulty()
      case MemberAuthentication.InvalidSignature(_) => maliciousOrFaulty()
      case MemberAuthentication.MissingToken(_) => maliciousOrFaulty()
      case MemberAuthentication.TokenVerificationException(_) => maliciousOrFaulty()
      case MemberAuthentication.AuthenticationNotSupportedForMember(_) => maliciousOrFaulty()
    }
  }

  private def eitherT[A, B](value: Either[A, B]) = EitherT.fromEither[Future](value)

  private def deserializeMember(
      memberPO: String
  ): Either[Status, Member] =
    Member
      .fromProtoPrimitive(memberPO, "member")
      .leftMap(err =>
        Status.INVALID_ARGUMENT.withDescription(s"Failed to deserialize member: $err")
      )

  private def handshakeValidation(request: Challenge.Request): Either[Status, Unit] =
    handshakeValidator
      .clientIsCompatible(request.memberProtocolVersions, minClientVersionP = None)
      .leftMap(err => Status.FAILED_PRECONDITION.withDescription(err))

}

object GrpcSequencerAuthenticationService extends GrpcSequencerAuthenticationErrorGroup {

  @Explanation(
    """This error indicates that a client failed to authenticate with the sequencer. The message is logged 
      |on the server in order to support an operator to provide explanations to clients struggling to connect."""
  )
  object SequencerAuthenticationFailure
      extends ErrorCode(
        id = "CLIENT_AUTHENTICATION_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    override protected def exposedViaApi: Boolean = false

    case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[String],
        response: Status,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

    case class AuthenticationFailure(member: String, response: Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

  }
  @Explanation(
    """This error indicates that a client failed to authenticate with the sequencer due to a reason possibly
      |pointing out to faulty or malicious behaviour. The message is logged on the server in order to support an 
      |operator to provide explanations to clients struggling to connect."""
  )
  object SequencerAuthenticationFaultyOrMalicious
      extends ErrorCode(
        id = "CLIENT_AUTHENTICATION_FAULTY",
        ErrorCategory.MaliciousOrFaultyBehaviour,
      ) {

    override protected def exposedViaApi: Boolean = false

    case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[String],
        response: Status,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Faulty or malicious challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

    case class AuthenticationFailure(member: String, response: Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Faulty or malicious authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with CantonError

  }

}
