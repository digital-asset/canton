// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.base.error.{Alarm, AlarmErrorCode, ErrorCategory, ErrorCode, Explanation}
import com.digitalasset.canton.crypto.{Nonce, Signature}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcFUSExtended
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.{
  AuthenticateRequest,
  AuthenticateResponse,
  ChallengeRequest,
  ChallengeResponse,
  LogoutRequest,
  LogoutResponse,
}
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.sequencer.api.v31.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService as VerifierBoundAuthenticationService
import com.digitalasset.canton.sequencer.api.v31.{
  SequencerAuthenticateRequest,
  SequencerAuthenticateResponse,
  SequencerChallengeRequest,
  SequencerChallengeResponse,
}
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  LogoutTokenDoesNotExist,
}
import com.digitalasset.canton.sequencing.authentication.grpc.AuthenticationTokenWithExpiry
import com.digitalasset.canton.sequencing.authentication.{AuthenticationToken, MemberAuthentication}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.Synchronizer.GrpcSequencerAuthenticationErrorGroup
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerLimits
import com.digitalasset.canton.synchronizer.sequencing.authentication.MemberAuthenticationService
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAuthenticationService.{
  SequencerAuthenticationFailure,
  SequencerAuthenticationFaultyOrMalicious,
}
import com.digitalasset.canton.synchronizer.service.HandshakeValidator
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.{ProtoUnvalidatedString, ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import io.grpc.Status
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerAuthenticationService(
    authenticationService: MemberAuthenticationService,
    protocolVersion: ProtocolVersion,
    disableReleaseVersionHandshakeCheck: Boolean,
    sequencerLimits: SequencerLimits,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerAuthenticationService
    with NamedLogging {

  private val pvv = ProtocolVersionValidation(protocolVersion)

  /** Bind this service to the local sequencer identity and configured external P2P endpoint. The
    * challenge discloses our identity to the signer; redemption reconstructs the signed hash using
    * this same binding. Neither value may come from the authentication request, otherwise a relay
    * could choose the verifier against which its captured signature is checked.
    */
  def forVerifier(
      verifier: MemberAuthentication.Verifier
  ): VerifierBoundAuthenticationService =
    // The service is not bound if PV <= 37, but it rejects requests even if bound in that case.
    new VerifierBoundAuthenticationService {
      private def whenSupported[A](operation: => Future[A]): Future[A] =
        if (MemberAuthentication.requiresVerifier(verifier.sequencerId, protocolVersion)) operation
        else
          Future.failed(
            Status.UNIMPLEMENTED
              .withDescription("Verifier-bound authentication requires protocol version 38")
              .asRuntimeException()
          )

      override def sequencerAuthenticate(
          request: SequencerAuthenticateRequest
      ): Future[SequencerAuthenticateResponse] =
        whenSupported(
          authenticateInternal(
            AuthenticateRequest(request.member, request.signature, request.nonce),
            Some(verifier),
          ).map(response => SequencerAuthenticateResponse(response.token, response.expiresAt))
        )

      override def sequencerChallenge(
          request: SequencerChallengeRequest
      ): Future[SequencerChallengeResponse] =
        whenSupported(
          challengeInternal(
            ChallengeRequest(request.member, request.memberProtocolVersions, request.clientVersion),
            verifierBound = true,
          ).map { challenge =>
            SequencerChallengeResponse(Some(challenge), verifier.sequencerId.toProtoPrimitive)
          }
        )
    }

  /** Redeem a member challenge response. Participants and mediators retain this RPC at every
    * protocol version; sequencers may use it only below PV38.
    */
  override def authenticate(request: AuthenticateRequest): Future[AuthenticateResponse] =
    authenticateInternal(request, None)

  private def authenticateInternal(
      request: AuthenticateRequest,
      verifierO: Option[MemberAuthentication.Verifier],
  ): Future[AuthenticateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      memberStr <- eitherT(
        ProtoValidation
          .validate(request.member, "member", pvv)
          .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), true))
      )
      member <- eitherT(deserializeMember(memberStr))
      _ <- eitherT(checkAuthenticationMechanism(member, verifierO.nonEmpty))
      signature <- eitherT(
        ProtoConverter
          .parseRequired(Signature.fromProtoV30(pvv, _), "signature", request.signature)
          .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), true))
      )
      providedNonce <- eitherT(
        Nonce
          .fromProtoPrimitive(request.nonce)
          .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), true))
      )
      tokenAndExpiry <- authenticationService
        .validateSignature(member, signature, providedNonce, verifierO)
        .leftMap(handleAuthError)
    } yield {
      val AuthenticationTokenWithExpiry(token, expiry) = tokenAndExpiry
      AuthenticateResponse(
        token = token.toProtoPrimitive,
        expiresAt = Some(expiry.toProtoTimestamp),
      )
    }).valueOr { case (error, isSensitive) =>
      // create error message to appropriately log this error
      val loggedMember = memberForLogging(request.member)
      val redactedError =
        if (isSensitive) {
          SequencerAuthenticationFaultyOrMalicious
            .AuthenticationFailure(
              loggedMember,
              error,
              if (error.getCode != Status.INTERNAL.getCode) Some(Level.INFO)
              else None,
            )
            .discard
          error.withDescription("Bad authentication request")
        } else {
          SequencerAuthenticationFailure
            .AuthenticationFailure(loggedMember, error)
            .discard
          error
        }
      throw redactedError.asRuntimeException()
    }.asGrpcResponse
  }

  /** Issue a member challenge after protocol compatibility and topology checks. Requesting a nonce
    * does not prove ownership of the claimed member identity; only redeeming its signature does.
    * PV38 sequencers must use the verifier-bound challenge RPC instead.
    */
  override def challenge(request: ChallengeRequest): Future[ChallengeResponse] =
    challengeInternal(request, verifierBound = false)

  // Enforce the choice at both challenge issuance and redemption, so PV38 sequencers cannot
  //  bypass verifier binding by calling the still-available participant/mediator RPCs.
  private def checkAuthenticationMechanism(
      member: Member,
      verifierBound: Boolean,
  ): Either[(Status, Boolean), Unit] =
    Either.cond(
      verifierBound == MemberAuthentication.requiresVerifier(member, protocolVersion),
      (),
      (
        Status.FAILED_PRECONDITION.withDescription(
          "Incorrect authentication mechanism for member and protocol version"
        ),
        false,
      ),
    )

  private def challengeInternal(
      request: ChallengeRequest,
      verifierBound: Boolean,
  ): Future[ChallengeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    (for {
      _ <- eitherT(handshakeValidation(request))
      memberStr <- eitherT(
        ProtoValidation
          .validate(request.member, "member", pvv)
          .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), true))
      )
      member <- eitherT(deserializeMember(memberStr))
      _ <- eitherT(checkAuthenticationMechanism(member, verifierBound))
      result <- authenticationService
        .generateChallenge(member)
        .leftMap(handleAuthError)
    } yield {
      val (nonce, fingerprints) = result
      ChallengeResponse(
        nonce.toProtoPrimitive,
        fingerprints.map(_.unwrap.toProtoUnvalidated).toList,
      )
    }).valueOr { case (error, isSensitive) =>
      val loggedMember = memberForLogging(request.member)
      // logging only, so an over-long list degrades to empty rather than failing the report
      val loggedMemberProtocolVersions = ProtoValidation
        .validateLength(
          request.memberProtocolVersions,
          "member_protocol_versions",
          pvv,
          ValidationParam(ProtocolVersion.minimum -> sequencerLimits.maxMemberProtocolVersions),
        )
        .getOrElse(Seq.empty)
      val redactedError =
        if (isSensitive) {
          SequencerAuthenticationFaultyOrMalicious
            .ChallengeFailure(
              loggedMember,
              loggedMemberProtocolVersions,
              error,
              if (error.getCode != Status.INTERNAL.getCode) Some(Level.INFO)
              else None,
            )
            .discard
          error.withDescription("Bad challenge request")
        } else {
          SequencerAuthenticationFailure
            .ChallengeFailure(
              loggedMember,
              loggedMemberProtocolVersions,
              error,
            )
            .discard
          error
        }
      throw redactedError.asRuntimeException()
    }.asGrpcResponse
  }

  private def handleAuthError(err: AuthenticationError): (Status, Boolean) =
    err match {
      case MemberAuthentication.MemberAccessDisabled(_) =>
        (Status.PERMISSION_DENIED.withDescription(err.reason), false)
      case MemberAuthentication.NonMatchingSynchronizerId(_, _) =>
        (Status.INVALID_ARGUMENT.withDescription(err.reason), false)
      case MemberAuthentication.NoKeysWithCorrectUsageRegistered(_, _) =>
        (Status.FAILED_PRECONDITION.withDescription(err.reason), true)
      case MemberAuthentication.FailedToSign(_, _) =>
        (Status.INTERNAL.withDescription(err.reason), true)
      case MemberAuthentication.MissingNonce(_) =>
        (Status.UNAUTHENTICATED.withDescription(err.reason), true)
      case MemberAuthentication.ExpiredNonce(_, _, _, _) =>
        (Status.UNAUTHENTICATED.withDescription(err.reason), true)
      case MemberAuthentication.InvalidSignature(_) =>
        (Status.INVALID_ARGUMENT.withDescription(err.reason), true)
      case MemberAuthentication.MissingToken(_) =>
        (Status.INVALID_ARGUMENT.withDescription(err.reason), true)
      case MemberAuthentication.TokenVerificationException(_) =>
        (Status.INTERNAL.withDescription(err.reason), true)
      case MemberAuthentication.LogoutTokenDoesNotExist =>
        (Status.INVALID_ARGUMENT.withDescription(err.reason), true)
    }

  private def eitherT[A, B](value: Either[A, B]) = EitherT.fromEither[FutureUnlessShutdown](value)

  private def deserializeMember(
      memberPO: String
  ): Either[(Status, Boolean), Member] =
    Member
      .fromProtoPrimitive(memberPO, "member")
      .leftMap(err =>
        (Status.INVALID_ARGUMENT.withDescription(s"Failed to deserialize member: $err"), true)
      )

  // A member string that fails content validation must not be echoed verbatim into logs; fall back
  // to "invalid" (the request has already been rejected in the for-comprehension by then).
  private def memberForLogging(member: ProtoUnvalidatedString): String =
    ProtoValidation
      .validate(member, "member", pvv)
      .getOrElse("invalid")

  /** Validates the handshake request.
    *
    * Returns on handshake validation failure as a Left that contains the Status error and a boolean
    * indicating if the error is security sensitive.
    */
  private def handshakeValidation(request: ChallengeRequest): Either[(Status, Boolean), Unit] =
    for {
      memberProtocolVersions <- ProtoValidation
        .validateLength(
          request.memberProtocolVersions,
          "member_protocol_versions",
          pvv,
          ValidationParam(ProtocolVersion.minimum -> sequencerLimits.maxMemberProtocolVersions),
        )
        .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), false))
      clientVersion <- ProtoValidation
        .validate(
          request.clientVersion,
          "client_version",
          pvv,
        )
        .leftMap(err => (Status.INVALID_ARGUMENT.withDescription(err.toString), false))
      _ <- HandshakeValidator
        .clientIsCompatible(
          protocolVersion,
          memberProtocolVersions,
          minClientVersionP = None,
          clientBinaryVersion = OptionUtil.emptyStringAsNone(clientVersion),
          disableReleaseVersionHandshakeCheck = disableReleaseVersionHandshakeCheck,
        )
        .leftMap((_, false))
    } yield ()

  /** Unconditionally revoke a member's authentication tokens and disconnect it
    */
  override def logout(request: LogoutRequest): Future[LogoutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      providedToken <- AuthenticationToken.fromProtoPrimitive(request.token) match {
        case Right(token) => FutureUnlessShutdown.pure(token)
        case Left(err) =>
          FutureUnlessShutdown.failed(
            Status.INVALID_ARGUMENT
              .withDescription(s"Failed to deserialize token: $err")
              .asRuntimeException()
          )
      }
      logoutResult <- authenticationService.invalidateMemberWithToken(providedToken)
      _ <- logoutResult match {
        case Right(()) => FutureUnlessShutdown.unit
        case Left(err @ LogoutTokenDoesNotExist) =>
          FutureUnlessShutdown.failed(
            Status.FAILED_PRECONDITION
              .withDescription(s"Failed to logout: $err")
              .asRuntimeException()
          )
      }
    } yield ()

    result.map(_ => LogoutResponse()).asGrpcResponse
  }
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

    final case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[Int],
        response: Status,
    )(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with ContextualizedCantonError

    final case class AuthenticationFailure(member: String, response: Status)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with ContextualizedCantonError
  }

  @Explanation(
    """This error indicates that a client failed to authenticate with the sequencer due to a reason possibly
      |pointing out to faulty or malicious behaviour. The message is logged on the server in order to support an
      |operator to provide explanations to clients struggling to connect."""
  )
  object SequencerAuthenticationFaultyOrMalicious
      extends AlarmErrorCode(id = "CLIENT_AUTHENTICATION_FAULTY") {

    override protected def exposedViaApi: Boolean = false

    final case class ChallengeFailure(
        member: String,
        supportedProtocol: Seq[Int],
        response: Status,
        override val overrideLogLevel: Option[Level] = None,
    )(implicit override val loggingContext: ErrorLoggingContext)
        extends Alarm(
          cause =
            s"Faulty or malicious challenge for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with ContextualizedCantonError

    final case class AuthenticationFailure(
        member: String,
        response: Status,
        override val overrideLogLevel: Option[Level] = None,
    )(implicit
        override val loggingContext: ErrorLoggingContext
    ) extends Alarm(
          cause =
            s"Faulty or malicious authentication for $member rejected with ${response.getCode}/${response.getDescription}"
        )
        with ContextualizedCantonError
  }
}
