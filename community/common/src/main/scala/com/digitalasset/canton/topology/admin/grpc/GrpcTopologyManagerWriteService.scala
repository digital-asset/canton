// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{CertificateId, Fingerprint, PublicKey, SigningPublicKey}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.admin.v0.DomainParametersChangeAuthorization.Parameters
import com.digitalasset.canton.topology.admin.v0.SignedLegalIdentityClaimGeneration.X509CertificateClaim
import com.digitalasset.canton.topology.admin.v0._
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.LegalIdentityClaimEvidence.X509Cert
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPackageId, ProtoDeserializationError}

import scala.concurrent.{ExecutionContext, Future}

class GrpcTopologyManagerWriteService[T <: CantonError](
    manager: TopologyManager[T],
    store: TopologyStore[TopologyStoreId.AuthorizedStore],
    cryptoPublicStore: CryptoPublicStore,
    protocolVersion: ProtocolVersion,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TopologyManagerWriteServiceGrpc.TopologyManagerWriteService
    with NamedLogging {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil._

  private def process(
      authDataPO: Option[AuthorizationData],
      elementE: Either[CantonError, TopologyMapping],
  )(implicit traceContext: TraceContext): Future[AuthorizationSuccess] = {
    val authDataE = for {
      authDataP <- ProtoConverter.required("authorization", authDataPO)
      AuthorizationData(changeP, signedByP, replaceExistingP, forceChangeP) = authDataP
      change <- TopologyChangeOp.fromProtoV0(changeP)
      fingerprint <- (if (signedByP.isEmpty) None
                      else Some(Fingerprint.fromProtoPrimitive(signedByP))).sequence
    } yield (change, fingerprint, replaceExistingP, forceChangeP)

    val authorizationSuccess = for {
      authData <- EitherT.fromEither[Future](
        authDataE.leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      element <- EitherT.fromEither[Future](elementE)
      (op, fingerprint, replace, force) = authData
      tx <- manager.genTransaction(op, element, protocolVersion).leftWiden[CantonError]
      success <- manager
        .authorize(tx, fingerprint, protocolVersion, force = force, replaceExisting = replace)
        .leftWiden[CantonError]
    } yield new AuthorizationSuccess(success.getCryptographicEvidence)

    EitherTUtil.toFuture(mapErrNew(authorizationSuccess))
  }

  override def authorizePartyToParticipant(
      request: PartyToParticipantAuthorization
  ): Future[AuthorizationSuccess] =
    fromGrpcContext { implicit traceContext =>
      val item = for {
        party <- UniqueIdentifier.fromProtoPrimitive(request.party, "party")
        side <- RequestSide.fromProtoEnum(request.side)
        participantId <- ParticipantId
          .fromProtoPrimitive(request.participant, "participant")
        permission <- ParticipantPermission
          .fromProtoEnum(request.permission)
      } yield PartyToParticipant(side, PartyId(party), participantId, permission)
      process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
    }

  override def authorizeOwnerToKeyMapping(
      request: OwnerToKeyMappingAuthorization
  ): Future[AuthorizationSuccess] =
    fromGrpcContext { implicit traceContext =>
      val itemEitherT: EitherT[Future, CantonError, OwnerToKeyMapping] = for {
        owner <- KeyOwner
          .fromProtoPrimitive(request.keyOwner, "keyOwner")
          .leftMap(ProtoDeserializationFailure.Wrap(_))
          .toEitherT[Future]
        keyId <- EitherT.fromEither[Future](
          Fingerprint
            .fromProtoPrimitive(request.fingerprintOfKey)
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
        key <- parseKeyInStoreResponse[PublicKey](keyId, cryptoPublicStore.publicKey(keyId))
      } yield OwnerToKeyMapping(owner, key)
      for {
        item <- itemEitherT.value
        result <- process(request.authorization, item)
      } yield result
    }

  private def parseKeyInStoreResponse[K <: PublicKey](
      fp: Fingerprint,
      result: EitherT[Future, CryptoPublicStoreError, Option[K]],
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, K] = {
    result
      .leftMap(TopologyManagerError.InternalError.CryptoPublicError(_))
      .subflatMap(_.toRight(TopologyManagerError.PublicKeyNotInStore.Failure(fp): CantonError))
  }

  private def getSigningPublicKey(
      fingerprint: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, SigningPublicKey] = {
    parseKeyInStoreResponse(fingerprint, cryptoPublicStore.signingKey(fingerprint))
  }

  private def parseFingerprint(
      proto: String
  )(implicit traceContext: TraceContext): EitherT[Future, CantonError, Fingerprint] =
    EitherT
      .fromEither[Future](Fingerprint.fromProtoPrimitive(proto))
      .leftMap(ProtoDeserializationFailure.Wrap(_))

  override def authorizeNamespaceDelegation(
      request: NamespaceDelegationAuthorization
  ): Future[AuthorizationSuccess] =
    fromGrpcContext { implicit traceContext =>
      val item = for {
        fp <- parseFingerprint(request.fingerprintOfAuthorizedKey)
        key <- getSigningPublicKey(fp)
        namespaceFingerprint <- parseFingerprint(request.namespace)
      } yield NamespaceDelegation(
        Namespace(namespaceFingerprint),
        key,
        request.isRootDelegation || request.namespace == request.fingerprintOfAuthorizedKey,
      )
      item.value.flatMap(itemE => process(request.authorization, itemE))
    }

  override def authorizeIdentifierDelegation(
      request: IdentifierDelegationAuthorization
  ): Future[AuthorizationSuccess] =
    fromGrpcContext { implicit traceContext =>
      val item = for {
        uid <- UniqueIdentifier
          .fromProtoPrimitive(request.identifier, "identifier")
          .leftMap(ProtoDeserializationFailure.Wrap(_))
          .toEitherT[Future]
        fp <- parseFingerprint(request.fingerprintOfAuthorizedKey)
        key <- getSigningPublicKey(fp)
      } yield IdentifierDelegation(uid, key)
      item.value.flatMap(itemE => process(request.authorization, itemE))
    }

  /** Adds a signed topology transaction to the Authorized store
    */
  override def addSignedTopologyTransaction(
      request: SignedTopologyTransactionAddition
  ): Future[AdditionSuccess] =
    fromGrpcContext { implicit traceContext =>
      val item = for {
        parsed <- mapErrNew(
          EitherT
            .fromEither[Future](SignedTopologyTransaction.fromByteString(request.serialized))
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
        _ <- mapErrNew(
          manager.add(parsed, force = true, replaceExisting = true, allowDuplicateMappings = true)
        )
      } yield AdditionSuccess()
      EitherTUtil.toFuture(item)
    }

  /** Authorizes a new signed legal identity
    */
  override def authorizeSignedLegalIdentityClaim(
      request: SignedLegalIdentityClaimAuthorization
  ): Future[AuthorizationSuccess] = fromGrpcContext { implicit traceContext =>
    val item = for {
      claimP <- ProtoConverter
        .required("claim", request.claim)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
      signedClaim <- SignedLegalIdentityClaim
        .fromProtoV0(claimP)
        .leftMap(ProtoDeserializationFailure.Wrap(_))
    } yield signedClaim
    process(request.authorization, item)
  }

  /** Generates a legal identity claim
    */
  override def generateSignedLegalIdentityClaim(
      request: SignedLegalIdentityClaimGeneration
  ): Future[v0.SignedLegalIdentityClaim] = fromGrpcContext { implicit traceContext =>
    import SignedLegalIdentityClaimGeneration.Request
    request.request match {
      case Request.LegalIdentityClaim(bytes) =>
        val result = for {
          parsed <- mapErrNew(
            LegalIdentityClaim.fromByteString(bytes).leftMap(ProtoDeserializationFailure.Wrap(_))
          )
          generated <- mapErrNew(manager.generate(parsed))
        } yield generated.toProtoV0
        EitherTUtil.toFuture(result)
      case Request.Certificate(X509CertificateClaim(uniqueIdentifier, certificateIdProto)) =>
        val result = for {
          uid <- mapErr(UniqueIdentifier.fromProtoPrimitive(uniqueIdentifier, "unique_identifier"))
          certs <- mapErr(cryptoPublicStore.listCertificates())
          certificateId <- mapErr(CertificateId.fromProtoPrimitive(certificateIdProto))
          certificate <- mapErr(
            certs
              .find(_.id == certificateId)
              .toRight(s"Can not find certificate with id ${certificateId}")
          )
          pem <- mapErr(certificate.toPem)
          generated <- mapErr(
            manager.generate(LegalIdentityClaim.create(uid, X509Cert(pem), protocolVersion))
          )
        } yield generated.toProtoV0
        EitherTUtil.toFuture(result)

      case Request.Empty => Future.failed(CantonGrpcUtil.invalidArgument("request is empty"))
    }
  }

  override def authorizeParticipantDomainState(
      request: ParticipantDomainStateAuthorization
  ): Future[AuthorizationSuccess] = fromGrpcContext { implicit traceContext =>
    val item = for {
      side <- RequestSide.fromProtoEnum(request.side)
      domain <- DomainId.fromProtoPrimitive(request.domain, "domain")
      participant <- ParticipantId
        .fromProtoPrimitive(request.participant, "participant")
      permission <- ParticipantPermission.fromProtoEnum(request.permission)
      trustLevel <- TrustLevel.fromProtoEnum(request.trustLevel)
    } yield ParticipantState(side, domain, participant, permission, trustLevel)
    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

  override def authorizeMediatorDomainState(
      request: MediatorDomainStateAuthorization
  ): Future[AuthorizationSuccess] = fromGrpcContext { implicit traceContext =>
    val item = for {
      side <- RequestSide.fromProtoEnum(request.side)
      domain <- DomainId.fromProtoPrimitive(request.domain, "domain")
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.mediator, "mediator")
    } yield MediatorDomainState(side, domain, MediatorId(uid))
    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

  /** Authorizes a new package vetting transaction */
  override def authorizeVettedPackages(
      request: VettedPackagesAuthorization
  ): Future[AuthorizationSuccess] =
    fromGrpcContext { implicit traceContext =>
      val item = for {
        uid <- UniqueIdentifier
          .fromProtoPrimitive(request.participant, "participant")
          .leftMap(ProtoDeserializationFailure.Wrap(_))
        packageIds <- request.packageIds
          .traverse(LfPackageId.fromString)
          .leftMap(err =>
            ProtoDeserializationFailure.Wrap(
              ProtoDeserializationError.ValueConversionError("package_ids", err)
            )
          )
      } yield VettedPackages(ParticipantId(uid), packageIds)
      process(request.authorization, item)
    }

  /** Authorizes a new domain parameters change transaction */
  override def authorizeDomainParametersChange(
      request: DomainParametersChangeAuthorization
  ): Future[AuthorizationSuccess] = fromGrpcContext { implicit traceContext =>
    val item = for {
      uid <- UniqueIdentifier
        .fromProtoPrimitive(request.domain, "domain")

      domainParameters <- request.parameters match {
        case Parameters.Empty => Left(ProtoDeserializationError.FieldNotSet("domainParameters"))
        case Parameters.ParametersV0(parametersV0) =>
          DynamicDomainParameters.fromProtoV0(parametersV0)
        case Parameters.ParametersV1(parametersV1) =>
          DynamicDomainParameters.fromProtoV1(parametersV1)
      }

    } yield DomainParametersChange(DomainId(uid), domainParameters)

    process(request.authorization, item.leftMap(ProtoDeserializationFailure.Wrap(_)))
  }

}
