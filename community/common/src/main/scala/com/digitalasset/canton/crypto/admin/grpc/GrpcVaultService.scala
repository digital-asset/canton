// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.admin.grpc

import cats.syntax.either._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.admin.v0.{RotateHmacSecretRequest, RotateHmacSecretResponse}
import com.digitalasset.canton.crypto.{v0 => cryptoproto, _}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil._
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import org.bouncycastle.asn1.x500.X500Name

import scala.concurrent.{ExecutionContext, Future}

class GrpcVaultService(
    crypto: Crypto,
    certificateGenerator: X509CertificateGenerator,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends v0.VaultServiceGrpc.VaultService
    with NamedLogging {

  private def listKeysResponse(
      request: v0.ListKeysRequest,
      pool: Iterable[PublicKeyWithName],
  ): v0.ListKeysResponse =
    v0.ListKeysResponse(
      pool
        .filter(entry =>
          entry.publicKey.fingerprint.unwrap.startsWith(request.filterFingerprint)
            && entry.name.map(_.unwrap).getOrElse("").contains(request.filterName)
            && request.filterPurpose.forall(_ == entry.publicKey.purpose.toProtoEnum)
        )
        .map(_.toProtoV0)
        .toSeq
    )

  // returns public keys of which we have private keys
  override def listMyKeys(request: v0.ListKeysRequest): Future[v0.ListKeysResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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
            keys.toList.filterA(pk =>
              crypto.cryptoPrivateStore
                .existsPrivateKey(pk.publicKey.id)
                .leftMap[String](err => s"Failed to check key ${pk.publicKey.id}'s existence: $err")
            )
          )
        )
      } yield listKeysResponse(request, publicKeys)
    }

  // allows to import public keys into the key store
  override def importPublicKey(
      request: v0.ImportPublicKeyRequest
  ): Future[v0.ImportPublicKeyResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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

  override def listPublicKeys(request: v0.ListKeysRequest): Future[v0.ListKeysResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      EitherTUtil.toFuture(
        mapErr(
          crypto.cryptoPublicStore.publicKeysWithName
            .map(keys => listKeysResponse(request, keys))
            .leftMap(err => s"Failed to retrieve public keys: $err")
        )
      )
    }

  override def generateSigningKey(
      request: v0.GenerateSigningKeyRequest
  ): Future[v0.GenerateSigningKeyResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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
  ): Future[v0.GenerateEncryptionKeyResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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
  ): Future[v0.ImportCertificateResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      val res = for {
        pem <- mapErr(X509CertificatePem.fromString(request.x509Cert))
        certificate <- mapErr(X509Certificate.fromPem(pem))
        _ <- mapErr(crypto.cryptoPublicStore.storeCertificate(certificate))
      } yield v0.ImportCertificateResponse(certificateId = certificate.id.unwrap)
      EitherTUtil.toFuture(res)
    }

  override def generateCertificate(
      request: v0.GenerateCertificateRequest
  ): Future[v0.GenerateCertificateResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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
  ): Future[v0.ListCertificateResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
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

  override def rotateHmacSecret(
      request: RotateHmacSecretRequest
  ): Future[RotateHmacSecretResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      EitherTUtil
        .toFuture(mapErr(crypto.privateCrypto.rotateHmacSecret(request.length)))
        .map(_ => RotateHmacSecretResponse())
    }
}
