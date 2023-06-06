// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.{v0 as cryptoproto, *}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.google.protobuf.empty.Empty
import org.bouncycastle.asn1.x500.X500Name

import scala.concurrent.{ExecutionContext, Future}

class GrpcVaultService(
    crypto: Crypto,
    certificateGenerator: X509CertificateGenerator,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v0.VaultServiceGrpc.VaultService
    with NamedLogging {

  private def listPublicKeys(
      request: v0.ListKeysRequest,
      pool: Iterable[PublicKeyWithName],
  ): Seq[PublicKeyWithName] =
    pool
      .filter(entry =>
        entry.publicKey.fingerprint.unwrap.startsWith(request.filterFingerprint)
          && entry.name.map(_.unwrap).getOrElse("").contains(request.filterName)
          && request.filterPurpose.forall(_ == entry.publicKey.purpose.toProtoEnum)
      )
      .toSeq

  // returns public keys of which we have private keys
  override def listMyKeys(request: v0.ListKeysRequest): Future[v0.ListMyKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      keys <- EitherTUtil.toFuture(
        mapErr(
          crypto.cryptoPublicStore.publicKeysWithName.leftMap(err =>
            s"Failed to retrieve public keys: $err"
          )
        )
      )
      publicKeys <- EitherTUtil.toFuture(
        mapErr(
          keys.toList.parFilterA(pk =>
            crypto.cryptoPrivateStore
              .existsPrivateKey(pk.publicKey.id)
              .leftMap[String](err => s"Failed to check key ${pk.publicKey.id}'s existence: $err")
          )
        )
      )
      filteredPublicKeys = listPublicKeys(request, publicKeys)
      keysMetadata <- EitherTUtil.toFuture(
        mapErr(
          filteredPublicKeys.parTraverse { pk =>
            (crypto.cryptoPrivateStore.toExtended match {
              case Some(extended) =>
                extended
                  .encrypted(pk.publicKey.id)
                  .leftMap[String](err =>
                    s"Failed to retrieve encrypted status for key ${pk.publicKey.id}: $err"
                  )
              case None => EitherT.rightT[Future, String](None)
            }).map(encrypted => PrivateKeyMetadata(pk, encrypted).toProtoV0)
          }
        )
      )
    } yield v0.ListMyKeysResponse(keysMetadata)
  }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v0.ImportPublicKeyRequest
  ): Future[v0.ImportPublicKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      publicKey <- mapErr(
        ProtoConverter
          .parse(
            cryptoproto.PublicKey.parseFrom,
            PublicKey.fromProtoPublicKeyV0,
            request.publicKey,
          )
          .leftMap(err => s"Failed to parse public key from protobuf: $err")
          .toEitherT[Future]
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      _ <- mapErr(
        crypto.cryptoPublicStore
          .storePublicKey(publicKey, name.emptyStringAsNone)
          .leftMap(err => s"Failed to store public key: $err")
      )
    } yield v0.ImportPublicKeyResponse(fingerprint = publicKey.fingerprint.unwrap)

    EitherTUtil.toFuture(res)
  }

  override def listPublicKeys(request: v0.ListKeysRequest): Future[v0.ListKeysResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    EitherTUtil.toFuture(
      mapErr(
        crypto.cryptoPublicStore.publicKeysWithName
          .map(keys => v0.ListKeysResponse(listPublicKeys(request, keys).map(_.toProtoV0)))
          .leftMap(err => s"Failed to retrieve public keys: $err")
      )
    )
  }

  override def generateSigningKey(
      request: v0.GenerateSigningKeyRequest
  ): Future[v0.GenerateSigningKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      scheme <- mapErr(
        if (request.keyScheme.isMissingSigningKeyScheme)
          Right(crypto.privateCrypto.defaultSigningKeyScheme)
        else
          SigningKeyScheme.fromProtoEnum("key_scheme", request.keyScheme)
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      key <- mapErr(crypto.generateSigningKey(scheme, name.emptyStringAsNone))
    } yield v0.GenerateSigningKeyResponse(publicKey = Some(key.toProtoV0))
    EitherTUtil.toFuture(res)
  }

  override def generateEncryptionKey(
      request: v0.GenerateEncryptionKeyRequest
  ): Future[v0.GenerateEncryptionKeyResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      scheme <- mapErr(
        if (request.keyScheme.isMissingEncryptionKeyScheme)
          Right(crypto.privateCrypto.defaultEncryptionKeyScheme)
        else
          EncryptionKeyScheme.fromProtoEnum("key_scheme", request.keyScheme)
      )
      name <- mapErr(KeyName.fromProtoPrimitive(request.name))
      key <- mapErr(crypto.generateEncryptionKey(scheme, name.emptyStringAsNone))
    } yield v0.GenerateEncryptionKeyResponse(publicKey = Some(key.toProtoV0))
    EitherTUtil.toFuture(res)
  }

  override def importCertificate(
      request: v0.ImportCertificateRequest
  ): Future[v0.ImportCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      pem <- mapErr(X509CertificatePem.fromString(request.x509Cert))
      certificate <- mapErr(X509Certificate.fromPem(pem))
      _ <- mapErr(crypto.cryptoPublicStore.storeCertificate(certificate))
    } yield v0.ImportCertificateResponse(certificateId = certificate.id.unwrap)
    EitherTUtil.toFuture(res)
  }

  override def generateCertificate(
      request: v0.GenerateCertificateRequest
  ): Future[v0.GenerateCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      uid <- mapErr(
        UniqueIdentifier.fromProtoPrimitive(request.uniqueIdentifier, "unique_identifier")
      )
      signingKeyId <- mapErr(Fingerprint.fromProtoPrimitive(request.certificateKey))
      commonName = s"CN=${uid.toProtoPrimitive}"
      additionalSubject = OptionUtil.emptyStringAsNone(request.additionalSubject).toList
      subject = new X500Name((commonName +: additionalSubject).mkString(","))
      certificate <- mapErr(
        certificateGenerator
          .generate(subject, signingKeyId, request.subjectAlternativeNames)
      )
      _ <- mapErr(crypto.cryptoPublicStore.storeCertificate(certificate))
      pem <- mapErr(certificate.toPem)
    } yield v0.GenerateCertificateResponse(x509Cert = pem.toString)
    EitherTUtil.toFuture(res)
  }

  override def listCertificates(
      request: v0.ListCertificateRequest
  ): Future[v0.ListCertificateResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      certs <- mapErr(crypto.cryptoPublicStore.listCertificates())
      converted <- mapErr(certs.toList.traverseFilter { certificate =>
        for {
          pem <- certificate.toPem
          cn <- certificate.subjectCommonName
        } yield {
          if (cn.startsWith(request.filterUid))
            Some(pem)
          else None
        }
      })
    } yield v0.ListCertificateResponse(
      results = converted.map(pem => v0.ListCertificateResponse.Result(x509Cert = pem.toString))
    )
    EitherTUtil.toFuture(res)
  }

  override def rotateWrapperKey(
      request: v0.RotateWrapperKeyRequest
  ): Future[Empty] = {
    Future.failed[Empty](StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException())
  }

  override def getWrapperKeyId(
      request: v0.GetWrapperKeyIdRequest
  ): Future[v0.GetWrapperKeyIdResponse] =
    Future.failed[v0.GetWrapperKeyIdResponse](
      StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException()
    )

}

object GrpcVaultService {
  trait GrpcVaultServiceFactory {
    def create(
        crypto: Crypto,
        certificateGenerator: X509CertificateGenerator,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): GrpcVaultService
  }

  class CommunityGrpcVaultServiceFactory extends GrpcVaultServiceFactory {
    override def create(
        crypto: Crypto,
        certificateGenerator: X509CertificateGenerator,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): GrpcVaultService =
      new GrpcVaultService(crypto, certificateGenerator, loggerFactory)
  }
}

final case class PrivateKeyMetadata(
    publicKeyWithName: PublicKeyWithName,
    wrapperKeyId: Option[String300],
) {

  def id: Fingerprint = publicKey.id

  def publicKey: PublicKey = publicKeyWithName.publicKey

  def name: Option[KeyName] = publicKeyWithName.name

  def purpose: KeyPurpose = publicKey.purpose

  def encrypted: Boolean = wrapperKeyId.isDefined

  def toProtoV0: v0.PrivateKeyMetadata =
    v0.PrivateKeyMetadata(
      publicKeyWithName = Some(publicKeyWithName.toProtoV0),
      wrapperKeyId = OptionUtil.noneAsEmptyString(wrapperKeyId.map(_.toProtoPrimitive)),
    )
}

object PrivateKeyMetadata {

  def fromProtoV0(key: v0.PrivateKeyMetadata): ParsingResult[PrivateKeyMetadata] =
    for {
      publicKeyWithName <- ProtoConverter.parseRequired(
        PublicKeyWithName.fromProtoV0,
        "public_key_with_name",
        key.publicKeyWithName,
      )
      wrapperKeyId <- OptionUtil
        .emptyStringAsNone(key.wrapperKeyId)
        .traverse(keyId => String300.fromProtoPrimitive(keyId, "wrapper_key_id"))
    } yield PrivateKeyMetadata(
      publicKeyWithName,
      wrapperKeyId,
    )
}
