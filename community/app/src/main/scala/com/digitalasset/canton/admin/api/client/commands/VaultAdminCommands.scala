// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.CertificateResult
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.crypto.admin.v0
import com.digitalasset.canton.crypto.admin.v0.VaultServiceGrpc.VaultServiceStub
import com.digitalasset.canton.crypto.{PublicKeyWithName, v0 as cryptoproto, *}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

object VaultAdminCommands {

  abstract class BaseVaultAdminCommand[Req, Res, Result]
      extends GrpcAdminCommand[Req, Res, Result] {
    override type Svc = VaultServiceStub
    override def createService(channel: ManagedChannel): VaultServiceStub =
      v0.VaultServiceGrpc.stub(channel)
  }

  abstract class ListKeys[R, T](
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends BaseVaultAdminCommand[v0.ListKeysRequest, R, Seq[T]] {

    override def createRequest(): Either[String, v0.ListKeysRequest] =
      Right(
        v0.ListKeysRequest(
          filterFingerprint = filterFingerprint,
          filterName = filterName,
          filterPurpose = filterPurpose.map(_.toProtoEnum).toSeq,
        )
      )
  }

  // list keys in my key vault
  case class ListMyKeys(
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends ListKeys[v0.ListMyKeysResponse, PrivateKeyMetadata](
        filterFingerprint,
        filterName,
        filterPurpose,
      ) {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ListKeysRequest,
    ): Future[v0.ListMyKeysResponse] =
      service.listMyKeys(request)

    override def handleResponse(
        response: v0.ListMyKeysResponse
    ): Either[String, Seq[PrivateKeyMetadata]] =
      response.privateKeysMetadata.traverse(PrivateKeyMetadata.fromProtoV0).leftMap(_.toString)
  }

  // list public keys in key registry
  case class ListPublicKeys(
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
  ) extends ListKeys[v0.ListKeysResponse, PublicKeyWithName](
        filterFingerprint,
        filterName,
        filterPurpose,
      ) {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ListKeysRequest,
    ): Future[v0.ListKeysResponse] =
      service.listPublicKeys(request)

    override def handleResponse(
        response: v0.ListKeysResponse
    ): Either[String, Seq[PublicKeyWithName]] =
      response.publicKeys.traverse(PublicKeyWithName.fromProtoV0).leftMap(_.toString)
  }

  abstract class BaseImportPublicKey
      extends BaseVaultAdminCommand[
        v0.ImportPublicKeyRequest,
        v0.ImportPublicKeyResponse,
        Fingerprint,
      ] {

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ImportPublicKeyRequest,
    ): Future[v0.ImportPublicKeyResponse] =
      service.importPublicKey(request)

    override def handleResponse(response: v0.ImportPublicKeyResponse): Either[String, Fingerprint] =
      Fingerprint.fromProtoPrimitive(response.fingerprint).leftMap(_.toString)
  }

  // upload a public key into the key registry
  case class ImportPublicKey(publicKey: ByteString, name: Option[String])
      extends BaseImportPublicKey {

    override def createRequest(): Either[String, v0.ImportPublicKeyRequest] =
      Right(v0.ImportPublicKeyRequest(publicKey = publicKey, name = name.getOrElse("")))
  }

  case class GenerateSigningKey(name: String, scheme: Option[SigningKeyScheme])
      extends BaseVaultAdminCommand[
        v0.GenerateSigningKeyRequest,
        v0.GenerateSigningKeyResponse,
        SigningPublicKey,
      ] {

    override def createRequest(): Either[String, v0.GenerateSigningKeyRequest] =
      Right(
        v0.GenerateSigningKeyRequest(
          name = name,
          keyScheme = scheme.fold[cryptoproto.SigningKeyScheme](
            cryptoproto.SigningKeyScheme.MissingSigningKeyScheme
          )(_.toProtoEnum),
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GenerateSigningKeyRequest,
    ): Future[v0.GenerateSigningKeyResponse] = {
      service.generateSigningKey(request)
    }

    override def handleResponse(
        response: v0.GenerateSigningKeyResponse
    ): Either[String, SigningPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => SigningPublicKey.fromProtoV0(k).leftMap(_.toString))

    // may take some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  case class GenerateEncryptionKey(name: String, scheme: Option[EncryptionKeyScheme])
      extends BaseVaultAdminCommand[
        v0.GenerateEncryptionKeyRequest,
        v0.GenerateEncryptionKeyResponse,
        EncryptionPublicKey,
      ] {

    override def createRequest(): Either[String, v0.GenerateEncryptionKeyRequest] =
      Right(
        v0.GenerateEncryptionKeyRequest(
          name = name,
          keyScheme = scheme.fold[cryptoproto.EncryptionKeyScheme](
            cryptoproto.EncryptionKeyScheme.MissingEncryptionKeyScheme
          )(_.toProtoEnum),
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GenerateEncryptionKeyRequest,
    ): Future[v0.GenerateEncryptionKeyResponse] = {
      service.generateEncryptionKey(request)
    }

    override def handleResponse(
        response: v0.GenerateEncryptionKeyResponse
    ): Either[String, EncryptionPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => EncryptionPublicKey.fromProtoV0(k).leftMap(_.toString))

    // may time some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  case class RotateWrapperKey(newWrapperKeyId: String)
      extends BaseVaultAdminCommand[
        v0.RotateWrapperKeyRequest,
        Empty,
        Unit,
      ] {

    override def createRequest(): Either[String, v0.RotateWrapperKeyRequest] =
      Right(
        v0.RotateWrapperKeyRequest(
          newWrapperKeyId = newWrapperKeyId
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.RotateWrapperKeyRequest,
    ): Future[Empty] = {
      service.rotateWrapperKey(request)
    }

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

  }

  case class GetWrapperKeyId()
      extends BaseVaultAdminCommand[
        v0.GetWrapperKeyIdRequest,
        v0.GetWrapperKeyIdResponse,
        String,
      ] {

    override def createRequest(): Either[String, v0.GetWrapperKeyIdRequest] =
      Right(
        v0.GetWrapperKeyIdRequest()
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GetWrapperKeyIdRequest,
    ): Future[v0.GetWrapperKeyIdResponse] = {
      service.getWrapperKeyId(request)
    }

    override def handleResponse(
        response: v0.GetWrapperKeyIdResponse
    ): Either[String, String] =
      Right(response.wrapperKeyId)

  }

  case class GenerateCertificate(
      uid: UniqueIdentifier,
      certificateKey: Fingerprint,
      additionalSubject: String,
      subjectAlternativeNames: Seq[String],
  ) extends BaseVaultAdminCommand[
        v0.GenerateCertificateRequest,
        v0.GenerateCertificateResponse,
        CertificateResult,
      ] {

    override def createRequest(): Either[String, v0.GenerateCertificateRequest] =
      Right(
        v0.GenerateCertificateRequest(
          uniqueIdentifier = uid.toProtoPrimitive,
          certificateKey = certificateKey.toProtoPrimitive,
          additionalSubject = additionalSubject,
          subjectAlternativeNames = subjectAlternativeNames,
        )
      )

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.GenerateCertificateRequest,
    ): Future[v0.GenerateCertificateResponse] =
      service.generateCertificate(request)

    override def handleResponse(
        response: v0.GenerateCertificateResponse
    ): Either[String, CertificateResult] =
      CertificateResult.fromPem(response.x509Cert).leftMap(_.toString)
  }

  case class ImportCertificate(x509Pem: String)
      extends BaseVaultAdminCommand[
        v0.ImportCertificateRequest,
        v0.ImportCertificateResponse,
        String,
      ] {

    override def createRequest(): Either[String, v0.ImportCertificateRequest] =
      Right(v0.ImportCertificateRequest(x509Cert = x509Pem))

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ImportCertificateRequest,
    ): Future[v0.ImportCertificateResponse] =
      service.importCertificate(request)

    override def handleResponse(response: v0.ImportCertificateResponse): Either[String, String] =
      Right(response.certificateId)
  }

  case class ListCertificates(filterUid: String)
      extends BaseVaultAdminCommand[v0.ListCertificateRequest, v0.ListCertificateResponse, List[
        CertificateResult
      ]] {

    override def createRequest(): Either[String, v0.ListCertificateRequest] =
      Right(v0.ListCertificateRequest(filterUid = filterUid))

    override def submitRequest(
        service: VaultServiceStub,
        request: v0.ListCertificateRequest,
    ): Future[v0.ListCertificateResponse] =
      service.listCertificates(request)

    override def handleResponse(
        response: v0.ListCertificateResponse
    ): Either[String, List[CertificateResult]] =
      response.results.toList.traverse(x =>
        CertificateResult.fromPem(x.x509Cert).leftMap(_.toString)
      )
  }

}
