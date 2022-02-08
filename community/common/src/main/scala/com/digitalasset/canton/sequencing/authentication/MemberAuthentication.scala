// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.data.{EitherT, NonEmptyList}
import cats.syntax.traverseFilter._
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.{
  AuthenticationError,
  FailedToSign,
  NoKeysRegistered,
}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

sealed trait MemberAuthentication {

  def hashDomainNonce(
      nonce: Nonce,
      domainId: DomainId,
      agreementId: Option[ServiceAgreementId],
      pureCrypto: CryptoPureApi,
  ): Hash

  /** Participant concatenates the nonce with the domain's id and signs it (step 3)
    */
  def signDomainNonce(
      member: Member,
      nonce: Nonce,
      domainId: DomainId,
      possibleSigningKeys: NonEmptyList[Fingerprint],
      agreementId: Option[ServiceAgreementId],
      crypto: Crypto,
  )(implicit ec: ExecutionContext): EitherT[Future, AuthenticationError, Signature] = {
    val hash = hashDomainNonce(nonce, domainId, agreementId, crypto.pureCrypto)

    for {
      // see if we have any of the possible keys that could be used to sign
      availableSigningKey <- possibleSigningKeys.toList
        .filterA(key => crypto.cryptoPrivateStore.existsSigningKey(key)(TraceContext.empty))
        .map(_.headOption) // the first we find is as good as any
        .leftMap(_ => NoKeysRegistered(member))
        .subflatMap(_.toRight[AuthenticationError](NoKeysRegistered(member)))
      sig <- crypto.privateCrypto
        .sign(hash, availableSigningKey)
        .leftMap[AuthenticationError](FailedToSign(member, _))
    } yield sig
  }

  /** Hash the common fields of the nonce.
    * Implementations of MemberAuthentication can then add their own fields as appropriate.
    */
  protected def commonNonce(pureApi: CryptoPureApi, nonce: Nonce, domainId: DomainId): HashBuilder =
    pureApi
      .build(HashPurpose.AuthenticationToken)
      .addWithoutLengthPrefix(
        nonce.getCryptographicEvidence
      ) // Nonces have a fixed length so it's fine to not add a length prefix
      .add(domainId.toProtoPrimitive)
}

object ParticipantAuthentication extends MemberAuthentication {
  def hashDomainNonce(
      nonce: Nonce,
      domainId: DomainId,
      agreementId: Option[ServiceAgreementId],
      pureApi: CryptoPureApi,
  ): Hash = {
    val builder = commonNonce(pureApi, nonce, domainId)
    agreementId.foreach(ag => builder.add(ag.unwrap))
    builder.finish()
  }
}

object DomainEntityAuthentication extends MemberAuthentication {
  override def hashDomainNonce(
      nonce: Nonce,
      domainId: DomainId,
      agreementId: Option[ServiceAgreementId],
      pureApi: CryptoPureApi,
  ): Hash =
    // we don't expect domain entities to use the agreement-id, so just exclude it
    commonNonce(pureApi, nonce, domainId).finish()
}

object MemberAuthentication {

  import com.digitalasset.canton.util.ShowUtil._

  def apply(member: Member): Either[AuthenticationError, MemberAuthentication] = member match {
    case _: ParticipantId => Right(ParticipantAuthentication)
    case _: MediatorId => Right(DomainEntityAuthentication)
    case _: DomainTopologyManagerId => Right(DomainEntityAuthentication)
    case _: SequencerId => Left(AuthenticationNotSupportedForMember(member))
    case _: UnauthenticatedMemberId => Left(AuthenticationNotSupportedForMember(member))
  }

  sealed abstract class AuthenticationError(val reason: String, val code: String)
  case class NoKeysRegistered(member: Member)
      extends AuthenticationError(s"Member $member has no keys registered", "NoKeysRegistered")
  case class FailedToSign(member: Member, error: SigningError)
      extends AuthenticationError("Failed to sign nonce", "FailedToSign")
  case class MissingNonce(member: Member)
      extends AuthenticationError(
        s"Member $member has not been previously assigned a handshake nonce",
        "MissingNonce",
      )
  case class InvalidSignature(member: Member)
      extends AuthenticationError(
        s"Given signature for member $member is invalid",
        "InvalidSignature",
      )
  case class MissingToken(member: Member)
      extends AuthenticationError(
        s"Authentication token for member $member has expired. Please reauthenticate.",
        "MissingToken",
      )
  case class NonMatchingDomainId(member: Member, domainId: DomainId)
      extends AuthenticationError(
        show"Domain id $domainId provided by member $member does not match the domain id of the domain the participant is trying to connect to",
        "NonMatchingDomainId",
      )
  case class ParticipantDisabled(participantId: ParticipantId)
      extends AuthenticationError(s"Participant $participantId is disabled", "ParticipantDisabled")
  case class TokenVerificationException(member: String)
      extends AuthenticationError(
        s"Due to an internal error, the server side token lookup for member $member failed",
        "VerifyTokenTimeout",
      )
  case class ServiceAgreementAcceptanceError(member: Member, error: String)
      extends AuthenticationError(reason = error, code = "ServiceAgreementAcceptanceError")
  case class AuthenticationNotSupportedForMember(member: Member)
      extends AuthenticationError(
        reason = s"Authentication for member type is not supported: $member",
        code = "UnsupportedMember",
      )

}
