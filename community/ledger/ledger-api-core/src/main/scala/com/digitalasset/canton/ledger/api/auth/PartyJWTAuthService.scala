// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm as Auth0Algorithm
import com.auth0.jwt.interfaces.DecodedJWT as Auth0DecodedJWT
import com.daml.jwt.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  Error as JwtError,
  Jwt,
  JwtDecoder,
  JwtException,
  JwtFromBearerHeader,
  JwtVerifier,
  Leeway,
  PartyJWTPayload,
}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.auth.{AuthService, ClaimSet, UninitializedPartyJWTAuthService}
import com.digitalasset.canton.crypto.{
  SignatureFormat,
  SignatureWithoutSigner,
  SigningAlgorithmSpec,
  SigningKeyUsage,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.user.IdentityProviderId
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.circe.parser
import org.bouncycastle.asn1
import org.bouncycastle.util.BigIntegers

import java.util.Base64
import scala.concurrent.{ExecutionContext, Future}

class PartyJWTAuthService(
    config: UninitializedPartyJWTAuthService,
    participantId: Ref.ParticipantId,
    userManagementStore: UserManagementStore,
    lookupSynchronizerCryptoClient: SynchronizerId => Option[SynchronizerCryptoClient],
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends AuthService
    with NamedLogging {

  def decodeToken(
      authToken: Option[String],
      serviceName: String,
  )(implicit _traceContext: TraceContext): Future[AuthService.Result] =
    authToken match {
      case None => Future.successful(AuthService.Result(ClaimSet.Unauthenticated))
      case Some(header) =>
        parseHeader(header).map(AuthService.Result(_)).recover { case error =>
          AuthService.Result(
            claimSet = ClaimSet.Unauthenticated,
            deferredWarning = Some(error.getMessage),
          )
        }
    }

  private val parseHeaderSymbol = Symbol("PartyJWTAuthService.parseHeader")

  private def fromEitherJwtError[T](e: Either[JwtError, T]): Future[T] =
    e.fold(err => Future.failed(new JwtException(err)), Future.successful)

  private def fromEitherString[T](e: Either[String, T]): Future[T] =
    fromEitherJwtError(e.left.map(msg => JwtError(parseHeaderSymbol, msg)))

  private def fromCond(b: Boolean, msg: String): Future[Unit] =
    fromEitherString(Either.cond(b, (), msg))

  private def parseHeader(
      header: String
  )(implicit traceContext: TraceContext): Future[ClaimSet] = {
    implicit val loggingContext = LoggingContextWithTrace(traceContext)(LoggingContext.empty)

    // We currently can only support users that have the default identity provider set.
    //
    // Explicitly passing another IDP in the JWT would be weird, because
    // conceptually the party is the issuer and IDP of the JWT.
    //
    // TODO(i34173)
    val userIdentityProviderId: IdentityProviderId = IdentityProviderId.Default

    for {
      token <- fromEitherJwtError(JwtFromBearerHeader(header))
      baseJWT <- Future(JWT.decode(token))

      // Create a verifier (checking alg) and check kty, in essence checking the header.
      // After the signature check, we will also use the verifier to check the timestamps.
      _ <- fromCond(baseJWT.getType() == "JWT", "Invalid kty")
      verifier <- fromEitherString(Verifier.get(baseJWT.getAlgorithm()))

      // Parse the payload to an AuthServiceJWTPayload, then more specifically
      // a PartyJWTPayload
      decodedJwt <- fromEitherJwtError(JwtDecoder.decode(Jwt(token)))
      payload <- Future(parse(decodedJwt.payload))
      partyPayload <- payload match {
        case p: PartyJWTPayload => Future.successful(p)
        case _ => Future.failed(JwtException(JwtError(parseHeaderSymbol, "Expected Party JWT")))
      }

      synchronizerId <- fromEitherString(SynchronizerId.fromString(partyPayload.synchronizerId))
      lfPartyId <- fromEitherString[Ref.Party](Ref.Party.fromString(partyPayload.partyId))
      partyId <- fromEitherString[PartyId](PartyId.fromLfParty(lfPartyId))

      // Verify participant (stored in the audience field of Party JWTs)
      _ <- fromCond(
        partyPayload.participantId == participantId,
        "Invalid participant ID",
      )

      // Verify the user exists, has the right primary party as well as
      // primaryPartyAuthentication enabled.
      lfUserId <- fromEitherString(Ref.UserId.fromString(partyPayload.userId))
      user <- userManagementStore
        .getUser(lfUserId, userIdentityProviderId)
        .flatMap(e => fromEitherString(e.left.map(_ => "Could not retrieve user")))
      _ <- fromCond(
        user.primaryPartyAuthentication == true,
        "primaryPartyAuthentication is not enabled",
      )
      _ <- fromCond(user.primaryParty.contains(lfPartyId), "primaryParty mismatch")

      // Parse the signature bytes
      (signedBytes, signatureBase64) <- token.lastIndexOf('.') match {
        case idx if idx >= 0 =>
          Future.successful((token.substring(0, idx), token.substring(idx + 1)))
        case _ => Future.failed(JwtException(JwtError(parseHeaderSymbol, "Parsing JWT failed")))
      }
      signatureBytes = Base64.getUrlDecoder().decode(signatureBase64)

      // Verify the actual signature
      synchronizerCryptoClient <- fromEitherString(
        lookupSynchronizerCryptoClient(synchronizerId).toRight("Synchronizer not found")
      )
      snapshotApproximation <- synchronizerCryptoClient.currentSnapshotApproximation
        .failOnShutdownToAbortException("PartyJWTAuthService.currentSnapshotApproximation")
      signature <- fromEitherString(verifier.fromJwtSignature(signatureBytes))
      verification <- snapshotApproximation
        .verifyPartyJwtSignature(
          bytes = ByteString.copyFromUtf8(signedBytes),
          signer = partyId,
          signature = signature,
          // Currently, we allow Party JWTs to be signed with the same keys that are used to sign protocol messages.
          // TODO(i32231): Introduce a dedicated key usage for this, but keep Protocol for backwards-compatibility with older keys.
          usage = SigningKeyUsage.ProtocolOnly,
        )
        .value
        .failOnShutdownToAbortException("verifyPartySignature")
      _ <- fromEitherString(verification.left.map(_.toString()))

      // Check for expiry
      _ <- fromEitherJwtError(verifier.timeVerifier.verify(com.daml.jwt.Jwt(token)))
    } yield ClaimSet.AuthenticatedUser(
      participantId = Some(partyPayload.participantId),
      identityProviderId = None,
      userId = partyPayload.userId,
      expiration = partyPayload.exp,
    )
  }

  private def parse(jwtPayload: String): AuthServiceJWTPayload = {
    import AuthServiceJWTCodec.JsonImplicits.*
    parser
      .decode(jwtPayload)
      .fold(
        err => throw new RuntimeException("Failed to decode JWT JSON payload", err),
        identity,
      )
  }

  /** We want to use the existing JWT infrastructure to validate timestamps and other base
    * properties of the JWT.
    *
    * This requires us to construct a JwtVerifier with an Algorithm (implementing the auth0
    * interface). The algorithm instance itself doesn't do anything, as we use the canton
    * CryptoPureApi to verify signatures. However, the name must exactly match the one in the JWT.
    *
    * For the second part, signature verification, we need to convert to JWT signature to a Canton
    * Signature.
    */
  private trait Verifier extends Leeway {
    def alg: String

    lazy val timeVerifier = new JwtVerifier(
      getVerifier(
        new Auth0Algorithm(alg, s"Stub $alg verification") {
          override def verify(jwt: Auth0DecodedJWT): Unit = ()
          override def sign(content: Array[Byte]): Array[Byte] =
            throw new UnsupportedOperationException("Signing not supported")
        },
        config.jwtTimestampLeeway,
      ),
      config.maxTokenLife,
    )

    def fromJwtSignature(signatureBytes: Array[Byte]): Either[String, SignatureWithoutSigner]
  }

  private object Verifier {
    private object EdDSA extends Verifier {
      def alg = "EdDSA"

      def fromJwtSignature(signatureBytes: Array[Byte]): Either[String, SignatureWithoutSigner] =
        Right(
          SignatureWithoutSigner(
            format = SignatureFormat.Concat,
            signingAlgorithmSpec = SigningAlgorithmSpec.Ed25519,
            signature = ByteString.copyFrom(signatureBytes),
          )
        )
    }

    private object ES256 extends Verifier {
      def alg = "ES256"

      def fromJwtSignature(signatureBytes: Array[Byte]): Either[String, SignatureWithoutSigner] =
        concatToDer(64, signatureBytes).map { der =>
          SignatureWithoutSigner(
            format = SignatureFormat.Der,
            signingAlgorithmSpec = SigningAlgorithmSpec.EcDsaSha256,
            signature = ByteString.copyFrom(der),
          )
        }
    }

    private object ES384 extends Verifier {
      def alg = "ES384"

      def fromJwtSignature(signatureBytes: Array[Byte]): Either[String, SignatureWithoutSigner] =
        concatToDer(96, signatureBytes).map { der =>
          SignatureWithoutSigner(
            format = SignatureFormat.Der,
            signingAlgorithmSpec = SigningAlgorithmSpec.EcDsaSha384,
            signature = ByteString.copyFrom(der),
          )
        }
    }

    /** Coversion of concat-based signature format (used by some JWT signature algorithms) to
      * DER-based signature format. Note that this does not change the "value" of the signature,
      * only its wire encoding.
      */
    private def concatToDer(expectedBytes: Int, bytes: Array[Byte]): Either[String, Array[Byte]] =
      if (expectedBytes != bytes.length)
        Left(s"Invalid signature size: expected $expectedBytes bytes but got ${bytes.length}")
      else {
        val rBytes = bytes.take(bytes.length / 2)
        val sBytes = bytes.drop(bytes.length / 2)
        val r = BigIntegers.fromUnsignedByteArray(rBytes)
        val s = BigIntegers.fromUnsignedByteArray(sBytes)
        val vec = new asn1.ASN1EncodableVector()
        vec.add(new asn1.ASN1Integer(r))
        vec.add(new asn1.ASN1Integer(s))
        val seq = new asn1.DERSequence(vec)
        Right(seq.getEncoded(asn1.ASN1Encoding.DER))
      }

    private val supported = List(EdDSA, ES256, ES384)

    def get(alg: String): Either[String, Verifier] =
      supported.find(_.alg == alg).toRight(s"no verifier for alg=$alg")
  }
}
