// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.crypto.admin.grpc.{BaseVaultRequest, PrivateKeyMetadata}
import com.digitalasset.canton.crypto.admin.v30
import com.digitalasset.canton.crypto.admin.v30.ListPublicKeysRequest
import com.digitalasset.canton.crypto.admin.v30.VaultServiceGrpc.VaultServiceStub
import com.digitalasset.canton.crypto.{PublicKeyWithName, v30 as cryptoprotoV30, *}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel

import scala.concurrent.Future

object VaultAdminCommands {

  abstract class BaseVaultAdminCommand[Req, Res, Result]
      extends GrpcAdminCommand[Req, Res, Result] {
    override type Svc = VaultServiceStub
    override def createService(channel: ManagedChannel): VaultServiceStub =
      v30.VaultServiceGrpc.stub(channel)
  }

  // list keys in my key vault
  final case class ListMyKeys(
      baseRequest: BaseVaultRequest,
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
      filterUsage: Set[SigningKeyUsage] = Set.empty,
      serverVersion: Option[ReleaseVersion],
  ) extends BaseVaultAdminCommand[
        v30.ListMyKeysRequest,
        v30.ListMyKeysResponse,
        Seq[PrivateKeyMetadata],
      ] {

    override protected def createRequest(): Either[String, v30.ListMyKeysRequest] =
      Right(
        v30.ListMyKeysRequest(
          baseRequest = Some(baseRequest.toProtoV30),
          filters = Some(
            v30.ListKeysFilters(
              fingerprint = filterFingerprint,
              name = filterName,
              purpose = filterPurpose.map(_.toProtoEnum).toSeq,
              usageV30 =
                if (ReleaseVersion.Feature.signingKeyUsageProtoV31.supported(serverVersion))
                  Seq()
                else
                  filterUsage.map(_.toProtoEnumV30).toSeq,
            )
          ),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.ListMyKeysRequest,
    ): Future[v30.ListMyKeysResponse] =
      service.listMyKeys(request)

    override protected def handleResponse(
        response: v30.ListMyKeysResponse
    ): Either[String, Seq[PrivateKeyMetadata]] =
      response.privateKeysMetadata.traverse(PrivateKeyMetadata.fromProtoV30).leftMap(_.toString)
  }

  // list public keys in key registry
  final case class ListPublicKeys(
      baseRequest: BaseVaultRequest,
      filterFingerprint: String,
      filterName: String,
      filterPurpose: Set[KeyPurpose] = Set.empty,
      filterUsage: Set[SigningKeyUsage] = Set.empty,
      serverVersion: Option[ReleaseVersion] = None,
  ) extends BaseVaultAdminCommand[
        v30.ListPublicKeysRequest,
        v30.ListPublicKeysResponse,
        Seq[PublicKeyWithName],
      ] {

    override protected def createRequest(): Either[String, ListPublicKeysRequest] =
      Right(
        v30.ListPublicKeysRequest(
          baseRequest = Some(baseRequest.toProtoV30),
          filters = Some(
            v30.ListKeysFilters(
              fingerprint = filterFingerprint,
              name = filterName,
              purpose = filterPurpose.map(_.toProtoEnum).toSeq,
              usageV30 =
                if (ReleaseVersion.Feature.signingKeyUsageProtoV31.supported(serverVersion))
                  Seq()
                else
                  filterUsage.map(_.toProtoEnumV30).toSeq,
            )
          ),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.ListPublicKeysRequest,
    ): Future[v30.ListPublicKeysResponse] =
      service.listPublicKeys(request)

    override protected def handleResponse(
        response: v30.ListPublicKeysResponse
    ): Either[String, Seq[PublicKeyWithName]] =
      response.publicKeysV30.traverse(PublicKeyWithName.fromProto30).leftMap(_.toString)
  }

  abstract class BaseImportPublicKey
      extends BaseVaultAdminCommand[
        v30.ImportPublicKeyRequest,
        v30.ImportPublicKeyResponse,
        Fingerprint,
      ] {

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.ImportPublicKeyRequest,
    ): Future[v30.ImportPublicKeyResponse] =
      service.importPublicKey(request)

    override protected def handleResponse(
        response: v30.ImportPublicKeyResponse
    ): Either[String, Fingerprint] =
      Fingerprint.fromProtoPrimitive(response.fingerprint).leftMap(_.toString)
  }

  // upload a public key into the key registry
  final case class ImportPublicKey(publicKey: ByteString, name: Option[String])
      extends BaseImportPublicKey {

    override protected def createRequest(): Either[String, v30.ImportPublicKeyRequest] =
      Right(v30.ImportPublicKeyRequest(publicKey = publicKey, name = name.getOrElse("")))
  }

  final case class GenerateSigningKey(
      baseRequest: BaseVaultRequest,
      name: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keySpec: Option[SigningKeySpec],
      serverVersion: Option[ReleaseVersion],
  ) extends BaseVaultAdminCommand[
        v30.GenerateSigningKeyRequest,
        v30.GenerateSigningKeyResponse,
        SigningPublicKey,
      ] {

    override protected def createRequest(): Either[String, v30.GenerateSigningKeyRequest] =
      Right(
        v30.GenerateSigningKeyRequest(
          baseRequest = Some(baseRequest.toProtoV30),
          name = name,
          usageV30 =
            if (ReleaseVersion.Feature.signingKeyUsageProtoV31.supported(serverVersion))
              Seq()
            else
              usage.map(_.toProtoEnumV30).toSeq,
          keySpec = keySpec.fold[cryptoprotoV30.SigningKeySpec](
            cryptoprotoV30.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED
          )(_.toProtoEnum),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.GenerateSigningKeyRequest,
    ): Future[v30.GenerateSigningKeyResponse] =
      service.generateSigningKey(request)

    override protected def handleResponse(
        response: v30.GenerateSigningKeyResponse
    ): Either[String, SigningPublicKey] =
      response.publicKey match {
        case v30.GenerateSigningKeyResponse.PublicKey.V30(k) =>
          SigningPublicKey.fromProtoV30(k).leftMap(_.toString)
        case _ => Left("No public key returned")
      }

    // may take some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class GenerateEncryptionKey(name: String, keySpecO: Option[EncryptionKeySpec])
      extends BaseVaultAdminCommand[
        v30.GenerateEncryptionKeyRequest,
        v30.GenerateEncryptionKeyResponse,
        EncryptionPublicKey,
      ] {

    override protected def createRequest(): Either[String, v30.GenerateEncryptionKeyRequest] =
      Right(
        v30.GenerateEncryptionKeyRequest(
          name = name,
          keySpec = keySpecO.fold[cryptoprotoV30.EncryptionKeySpec](
            cryptoprotoV30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_UNSPECIFIED
          )(_.toProtoEnum),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.GenerateEncryptionKeyRequest,
    ): Future[v30.GenerateEncryptionKeyResponse] =
      service.generateEncryptionKey(request)

    override protected def handleResponse(
        response: v30.GenerateEncryptionKeyResponse
    ): Either[String, EncryptionPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => EncryptionPublicKey.fromProtoV30(k).leftMap(_.toString))

    // may time some time if we need to wait for entropy
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class RegisterKmsSigningKey(
      baseRequest: BaseVaultRequest,
      kmsKeyId: String,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: String,
      serverVersion: Option[ReleaseVersion],
  ) extends BaseVaultAdminCommand[
        v30.RegisterKmsSigningKeyRequest,
        v30.RegisterKmsSigningKeyResponse,
        SigningPublicKey,
      ] {

    override protected def createRequest(): Either[String, v30.RegisterKmsSigningKeyRequest] =
      Right(
        v30.RegisterKmsSigningKeyRequest(
          baseRequest = Some(baseRequest.toProtoV30),
          kmsKeyId = kmsKeyId,
          usageV30 =
            if (ReleaseVersion.Feature.signingKeyUsageProtoV31.supported(serverVersion))
              Seq()
            else
              usage.map(_.toProtoEnumV30).toSeq,
          name = name,
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.RegisterKmsSigningKeyRequest,
    ): Future[v30.RegisterKmsSigningKeyResponse] =
      service.registerKmsSigningKey(request)

    override protected def handleResponse(
        response: v30.RegisterKmsSigningKeyResponse
    ): Either[String, SigningPublicKey] =
      response.publicKey match {
        case v30.RegisterKmsSigningKeyResponse.PublicKey.V30(k) =>
          SigningPublicKey.fromProtoV30(k).leftMap(_.toString)
        case _ => Left("No public key returned")
      }

  }

  final case class RegisterKmsEncryptionKey(kmsKeyId: String, name: String)
      extends BaseVaultAdminCommand[
        v30.RegisterKmsEncryptionKeyRequest,
        v30.RegisterKmsEncryptionKeyResponse,
        EncryptionPublicKey,
      ] {

    override protected def createRequest(): Either[String, v30.RegisterKmsEncryptionKeyRequest] =
      Right(
        v30.RegisterKmsEncryptionKeyRequest(
          kmsKeyId = kmsKeyId,
          name = name,
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.RegisterKmsEncryptionKeyRequest,
    ): Future[v30.RegisterKmsEncryptionKeyResponse] =
      service.registerKmsEncryptionKey(request)

    override protected def handleResponse(
        response: v30.RegisterKmsEncryptionKeyResponse
    ): Either[String, EncryptionPublicKey] =
      response.publicKey
        .toRight("No public key returned")
        .flatMap(k => EncryptionPublicKey.fromProtoV30(k).leftMap(_.toString))

  }

  final case class RotateWrapperKey(newWrapperKeyId: String)
      extends BaseVaultAdminCommand[
        v30.RotateWrapperKeyRequest,
        v30.RotateWrapperKeyResponse,
        Unit,
      ] {

    override protected def createRequest(): Either[String, v30.RotateWrapperKeyRequest] =
      Right(
        v30.RotateWrapperKeyRequest(
          newWrapperKeyId = newWrapperKeyId
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.RotateWrapperKeyRequest,
    ): Future[v30.RotateWrapperKeyResponse] =
      service.rotateWrapperKey(request)

    override protected def handleResponse(
        response: v30.RotateWrapperKeyResponse
    ): Either[String, Unit] =
      Either.unit

  }

  final case class GetWrapperKeyId()
      extends BaseVaultAdminCommand[
        v30.GetWrapperKeyIdRequest,
        v30.GetWrapperKeyIdResponse,
        String,
      ] {

    override protected def createRequest(): Either[String, v30.GetWrapperKeyIdRequest] =
      Right(
        v30.GetWrapperKeyIdRequest()
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.GetWrapperKeyIdRequest,
    ): Future[v30.GetWrapperKeyIdResponse] =
      service.getWrapperKeyId(request)

    override protected def handleResponse(
        response: v30.GetWrapperKeyIdResponse
    ): Either[String, String] =
      Right(response.wrapperKeyId)

  }

  final case class ImportKeyPair(
      keyPair: ByteString,
      name: Option[String],
      password: Option[String],
  ) extends BaseVaultAdminCommand[
        v30.ImportKeyPairRequest,
        v30.ImportKeyPairResponse,
        Unit,
      ] {

    override protected def createRequest(): Either[String, v30.ImportKeyPairRequest] =
      Right(
        v30.ImportKeyPairRequest(
          keyPair = keyPair,
          name = OptionUtil.noneAsEmptyString(name),
          password = OptionUtil.noneAsEmptyString(password),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.ImportKeyPairRequest,
    ): Future[v30.ImportKeyPairResponse] =
      service.importKeyPair(request)

    override protected def handleResponse(
        response: v30.ImportKeyPairResponse
    ): Either[String, Unit] = Either.unit
  }

  final case class ExportKeyPair(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion,
      password: Option[String],
  ) extends BaseVaultAdminCommand[
        v30.ExportKeyPairRequest,
        v30.ExportKeyPairResponse,
        ByteString,
      ] {

    override protected def createRequest(): Either[String, v30.ExportKeyPairRequest] =
      Right(
        v30.ExportKeyPairRequest(
          fingerprint = fingerprint.toProtoPrimitive,
          protocolVersion = protocolVersion.toProtoPrimitive,
          password = OptionUtil.noneAsEmptyString(password),
        )
      )

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.ExportKeyPairRequest,
    ): Future[v30.ExportKeyPairResponse] =
      service.exportKeyPair(request)

    override protected def handleResponse(
        response: v30.ExportKeyPairResponse
    ): Either[String, ByteString] =
      Right(response.keyPair)
  }

  final case class DeleteKeyPair(fingerprint: Fingerprint)
      extends BaseVaultAdminCommand[
        v30.DeleteKeyPairRequest,
        v30.DeleteKeyPairResponse,
        Unit,
      ] {

    override protected def createRequest(): Either[String, v30.DeleteKeyPairRequest] =
      Right(v30.DeleteKeyPairRequest(fingerprint = fingerprint.toProtoPrimitive))

    override protected def submitRequest(
        service: VaultServiceStub,
        request: v30.DeleteKeyPairRequest,
    ): Future[v30.DeleteKeyPairResponse] =
      service.deleteKeyPair(request)

    override protected def handleResponse(
        response: v30.DeleteKeyPairResponse
    ): Either[String, Unit] = Either.unit
  }
}
