// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.gcp

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.KmsError.*
import com.digitalasset.canton.crypto.kms.gcp.audit.GcpRequestResponseLogger
import com.digitalasset.canton.crypto.kms.{
  Kms,
  KmsEncryptionPublicKey,
  KmsError,
  KmsKeyId,
  KmsSigningPublicKey,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.google.api.core.ApiFunction
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.api.gax.rpc.{ApiException, ResourceExhaustedException}
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.kms.v1 as gcp
import com.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm
import com.google.cloud.kms.v1.{AsymmetricSignRequest, CryptoKeyVersion}
import com.google.protobuf.ByteString
import io.grpc.ManagedChannelBuilder
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.openssl.PEMParser

import java.io.{IOException, StringReader}
import java.util.UUID
import scala.annotation.unused
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.*

/** Stands for Google Cloud Platform - Key Management Service and is an internal KMS implementation
  * that wraps the necessary cryptographic functions from the GCP SDK.
  */
class GcpKms(
    val config: KmsConfig.Gcp,
    private val location: gcp.LocationName,
    private val kmsClient: gcp.KeyManagementServiceClient,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends Kms
    with NamedLogging {

  override type Config = KmsConfig.Gcp

  override def name: String = "gcp-kms"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()

  private lazy val loggerKms = new GcpRequestResponseLogger(config.auditLogging, loggerFactory)

  /** Cache of cryptoKey id -> resolved cryptoKey version id, populated lazily on first
    * successful resolution to an `ENABLED` version. This avoids issuing a
    * `listCryptoKeyVersions` call for every KMS operation while still picking up the right
    * version for cryptoKeys whose key material was imported into GCP KMS (where the version
    * holding the imported material is not necessarily `"1"`).
    */
  private val resolvedKeyVersions: TrieMap[String, String] = TrieMap.empty

  private def cryptoKeyName(keyId: KmsKeyId): gcp.CryptoKeyName =
    // Symmetric keys: GCP picks the primary version on encrypt and recovers it from the
    // ciphertext on decrypt, so no per-key version is needed here.
    gcp.CryptoKeyName.of(config.projectId, config.locationId, config.keyRingId, keyId.unwrap)

  private def cryptoKeyVersionName(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, gcp.CryptoKeyVersionName] =
    resolveLatestKeyVersion(keyId).map { version =>
      gcp.CryptoKeyVersionName.of(
        config.projectId,
        config.locationId,
        config.keyRingId,
        keyId.unwrap,
        version,
      )
    }

  /** Resolves the cryptoKey version Canton should use for the given [[KmsKeyId]]. We pick the
    * highest-numbered usable (i.e. non-destroyed) version of the cryptoKey. Once an `ENABLED`
    * version is observed, it is cached for the lifetime of this client. Callers that need to
    * react to state changes (e.g. `keyExistsAndIsActive`) should go through
    * [[retrieveKeyMetadata]] which always re-issues the lookup.
    */
  private def resolveLatestKeyVersion(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, String] =
    resolvedKeyVersions.get(keyId.unwrap) match {
      case Some(cached) =>
        EitherT.rightT[FutureUnlessShutdown, KmsError](cached)
      case None =>
        listLatestCryptoKeyVersion(keyId).map { version =>
          val versionId = parseKeyVersionId(version.getName)
          // Only memoize ENABLED versions: while a version is in PENDING_GENERATION (or some
          // other transient state) we want subsequent calls to re-issue the lookup so the
          // upstream retry logic keeps making progress.
          if (version.getState == CryptoKeyVersion.CryptoKeyVersionState.ENABLED) {
            resolvedKeyVersions.put(keyId.unwrap, versionId).discard
          }
          versionId
        }
    }

  private def parseKeyVersionId(cryptoKeyVersionName: String): String =
    cryptoKeyVersionName.substring(cryptoKeyVersionName.lastIndexOf('/') + 1)

  private def listLatestCryptoKeyVersion(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, gcp.CryptoKeyVersion] = {
    val parent =
      gcp.CryptoKeyName.of(config.projectId, config.locationId, config.keyRingId, keyId.unwrap)
    loggerKms.withLogging[gcp.CryptoKeyVersion](
      loggerKms.retrieveKeyMetadataRequestMsg(keyId.unwrap),
      keyMetadata =>
        loggerKms.retrieveKeyMetadataResponseMsg(
          keyId.unwrap,
          keyMetadata.getAlgorithm.name,
          keyMetadata.getState.name,
        ),
    )(
      wrapKmsCall(
        kmsErrorGen = (errStr, retryable) => KmsRetrieveKeyMetadataError(keyId, errStr, retryable),
        functionName = functionFullName,
      ) {
        kmsClient.listCryptoKeyVersions(parent).iterateAll().asScala.toSeq
      }.subflatMap { versions =>
        val usable = versions.filter { v =>
          val state = v.getState
          state != CryptoKeyVersion.CryptoKeyVersionState.DESTROYED &&
          state != CryptoKeyVersion.CryptoKeyVersionState.DESTROY_SCHEDULED
        }
        // Pick the highest version id (last segment of the name parses to an integer) so that
        // both Canton-generated keys (always a single version "1") and imported keys (where
        // each import adds a new version) work transparently. We sort numerically, not
        // lexicographically, so version "10" comes after "9".
        usable
          .maxByOption(v => parseKeyVersionId(v.getName).toIntOption.getOrElse(-1))
          .toRight[KmsError](
            // Retryable so we don't hard-fail right after creation while the only version is
            // still PENDING_GENERATION; the upstream retry loop will re-list and converge.
            KmsRetrieveKeyMetadataError(
              keyId,
              s"no usable cryptoKey version found for ${keyId.unwrap}",
              retryable = true,
            )
          )
      }
    )
  }

  private val errorMessagesToRetry =
    Set(
      "io.grpc.StatusRuntimeException: UNAVAILABLE: Connection closed",
      "Internal error encountered",
      "INTERNAL: http2 exception",
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def convertPublicKeyFromPemToDer(pubKeyPEM: String): Either[String, ByteString] = {
    val pemParser: PEMParser = new PEMParser(new StringReader(pubKeyPEM))
    try {
      Option(pemParser.readObject) match {
        case Some(spki: SubjectPublicKeyInfo) =>
          Right(ByteString.copyFrom(spki.getEncoded))
        case Some(_) =>
          Left("unexpected type conversion")
        case None =>
          Left("could not parse public key info from PEM format")
      }
    } catch {
      case e: IOException =>
        Left(
          s"failed to convert public key from PEM to DER format: ${ThrowableUtil.messageWithStacktrace(e)}"
        )
    } finally {
      pemParser.close()
    }
  }

  private def errorHandler(
      err: RuntimeException,
      kmsErrorGen: (String, Boolean) => KmsError,
  ): KmsError =
    err match {
      // we look for network failure errors to retry on
      case networkErr if errorMessagesToRetry.exists(networkErr.getMessage.contains(_)) =>
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), true)
      // we retry on resource exceptions as well
      case resourceException: ResourceExhaustedException =>
        logger.debug(s"ResourceExhaustedException with retry: ${resourceException.isRetryable}")(
          TraceContext.empty
        )
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), true)
      // CancelledException is a subclass of ApiException, so this case must come *before*
      // the generic `ApiException` clause to ensure CancelledExceptions are handled specifically.
      case cancelled: com.google.api.gax.rpc.CancelledException
          if Option(cancelled.getMessage).exists(_.contains("CANCELLED")) =>
        logger.debug("Got CancelledException(CANCELLED) — treating it as retryable")(
          TraceContext.empty
        )
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), true)
      case internalErr: com.google.api.gax.rpc.InternalException
          if Option(internalErr.getMessage)
            .exists(errMsg => errorMessagesToRetry.exists(errMsg.contains(_))) =>
        logger.debug(
          "Got InternalException(Internal error encountered) — treating it as retryable"
        )(
          TraceContext.empty
        )
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), true)
      case apiErr: ApiException if apiErr.isRetryable =>
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), true)
      case _ =>
        kmsErrorGen(ThrowableUtil.messageWithStacktrace(err), false)
    }

  private def wrapKmsCall[A](
      kmsErrorGen: (String, Boolean) => KmsError,
      functionName: String,
  )(
      kmsCall: => A
  )(implicit ec: ExecutionContext, tc: TraceContext): EitherT[FutureUnlessShutdown, KmsError, A] =
    EitherT {
      synchronizeWithClosingF(functionName) {
        Future {
          blocking {
            Either.catchOnly[RuntimeException](kmsCall)
          }
        }
      }
    }.leftMap[KmsError](err =>
      errorHandler(err, (errStr, retryable) => kmsErrorGen(errStr, retryable))
    )

  /** Creates a GCP KMS key based on a series of specifications and returns its key identifier.
    *
    * @param keySpec
    *   specifies the type of KMS key to create (e.g. SYMMETRIC_DEFAULT (AES-256-CBC) or RSA_2048).
    * @param keyPurpose
    *   the cryptographic operations for which you can use the KMS key (e.g. signing or encryption).
    * @param keyRingId
    *   specifies the key ring to which the new key will be associated to. A key ring can be set for
    *   multi-region, which automatically sets all its keys to be multi-region.
    * @return
    *   a key id or an error if it fails to create a key
    */
  private def createKey(
      keySpec: CryptoKeyVersionAlgorithm,
      keyPurpose: CryptoKeyPurpose,
      keyRingId: String,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] = {
    val kmsKeyIdStr = UUID.randomUUID().toString
    val keyRingName = gcp.KeyRingName.of(location.getProject, location.getLocation, keyRingId)
    for {
      _ <- loggerKms.withLogging[gcp.CryptoKey](
        loggerKms.createKeyRequestMsg(keyPurpose.name, keySpec.name),
        _ => loggerKms.createKeyResponseMsg(kmsKeyIdStr, keyPurpose.name, keySpec.name),
      ) {
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsCreateKeyError(errStr, retryable),
          functionName = functionFullName,
        ) {
          val key =
            gcp.CryptoKey
              .newBuilder()
              .setPurpose(keyPurpose)
              .setVersionTemplate(
                gcp.CryptoKeyVersionTemplate
                  .newBuilder()
                  .setAlgorithm(keySpec)
                  .setProtectionLevel(gcp.ProtectionLevel.HSM)
              )
              .build()
          kmsClient.createCryptoKey(keyRingName, kmsKeyIdStr, key)
        }
      }
      kmsKeyId <- String300
        .create(kmsKeyIdStr)
        .toEitherT[FutureUnlessShutdown]
        .map(KmsKeyId.apply)
        .leftMap[KmsError](err => KmsCreateKeyError(err))
    } yield kmsKeyId
  }

  override protected def generateSigningKeyPairInternal(
      signingKeySpec: SigningKeySpec,
      @unused name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keySpec <- convertToGcpSigningScheme(signingKeySpec)
        .leftMap(err => KmsCreateKeyError(err))
        .toEitherT[FutureUnlessShutdown]
      // GCP KMS does not allow to store the name alongside the key
      kmsKeyId <- createKey(
        keySpec,
        CryptoKeyPurpose.ASYMMETRIC_SIGN,
        config.keyRingId,
      )
    } yield kmsKeyId

  override protected def generateSymmetricEncryptionKeyInternal(
      @unused name: Option[KeyName]
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    // GCP KMS does not allow to store the name alongside the key
    createKey(
      CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION,
      CryptoKeyPurpose.ENCRYPT_DECRYPT,
      config.keyRingId,
    )

  override protected def generateAsymmetricEncryptionKeyPairInternal(
      encryptionKeySpec: EncryptionKeySpec,
      @unused name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keySpec <- convertToGcpAsymmetricKeyEncryptionSpec(encryptionKeySpec)
        .leftMap(err => KmsCreateKeyError(err))
        .toEitherT[FutureUnlessShutdown]
      // GCP KMS does not allow to store the name alongside the key
      kmsKeyId <- createKey(
        keySpec,
        CryptoKeyPurpose.ASYMMETRIC_DECRYPT,
        config.keyRingId,
      )
    } yield kmsKeyId

  private def getPublicKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, gcp.PublicKey] =
    cryptoKeyVersionName(keyId).flatMap { keyVersionName =>
      loggerKms.withLogging[gcp.PublicKey](
        loggerKms.getPublicKeyRequestMsg(keyId.unwrap),
        publicKey => loggerKms.getPublicKeyResponseMsg(keyId.unwrap, publicKey.getAlgorithm.name),
      )(
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsGetPublicKeyError(keyId, errStr, retryable),
          functionName = functionFullName,
        )(kmsClient.getPublicKey(keyVersionName))
      )
    }

  override protected def getPublicSigningKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsSigningPublicKey] =
    for {
      pkResponse <- getPublicKeyInternal(keyId)
      keySpec <- convertFromGcpSigningScheme(pkResponse.getAlgorithm)
        .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      pubKeyRaw <- convertPublicKeyFromPemToDer(pkResponse.getPem)
        .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      pubKey <- KmsSigningPublicKey
        .create(pubKeyRaw, keySpec)
        .leftMap[KmsError](err => KmsGetPublicKeyError(keyId, err.toString))
        .toEitherT[FutureUnlessShutdown]
    } yield pubKey

  override protected def getPublicEncryptionKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsEncryptionPublicKey] =
    for {
      pkResponse <- getPublicKeyInternal(keyId)
      keySpec <- convertFromGcpAsymmetricEncryptionSpec(pkResponse.getAlgorithm)
        .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      pubKeyRaw <- convertPublicKeyFromPemToDer(pkResponse.getPem)
        .leftMap[KmsError](KmsGetPublicKeyError(keyId, _))
        .toEitherT[FutureUnlessShutdown]
      pubKey <- KmsEncryptionPublicKey
        .create(pubKeyRaw, keySpec)
        .leftMap[KmsError](err => KmsGetPublicKeyError(keyId, err))
        .toEitherT[FutureUnlessShutdown]
    } yield pubKey

  private def convertToGcpSigningScheme(
      signingKeySpec: SigningKeySpec
  ): Either[String, CryptoKeyVersionAlgorithm] =
    signingKeySpec match {
      case SigningKeySpec.EcCurve25519 =>
        Right(CryptoKeyVersionAlgorithm.EC_SIGN_ED25519)
      case SigningKeySpec.EcP256 =>
        Right(CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256)
      case SigningKeySpec.EcP384 =>
        Right(CryptoKeyVersionAlgorithm.EC_SIGN_P384_SHA384)
      case SigningKeySpec.EcSecp256k1 =>
        Right(CryptoKeyVersionAlgorithm.EC_SIGN_SECP256K1_SHA256)
    }

  private def convertToGcpAsymmetricEncryptionSpec(
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec
  ): Either[String, CryptoKeyVersionAlgorithm] =
    encryptionAlgorithmSpec match {
      case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
        Left(s"Unsupported encryption specification: ${encryptionAlgorithmSpec.name}")
      case EncryptionAlgorithmSpec.RsaOaepSha256 =>
        Right(CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256)
    }

  private def convertToGcpAsymmetricKeyEncryptionSpec(
      encryptionKeySpec: EncryptionKeySpec
  ): Either[String, CryptoKeyVersionAlgorithm] =
    encryptionKeySpec match {
      case EncryptionKeySpec.EcP256 =>
        Left(s"Unsupported encryption key type: ${encryptionKeySpec.name}")
      case EncryptionKeySpec.Rsa2048 =>
        Right(CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256)
    }

  private def convertFromGcpSigningScheme(
      keySpec: CryptoKeyVersionAlgorithm
  ): Either[String, SigningKeySpec] =
    keySpec match {
      case CryptoKeyVersionAlgorithm.EC_SIGN_ED25519 => Right(SigningKeySpec.EcCurve25519)
      case CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256 => Right(SigningKeySpec.EcP256)
      case CryptoKeyVersionAlgorithm.EC_SIGN_P384_SHA384 => Right(SigningKeySpec.EcP384)
      case CryptoKeyVersionAlgorithm.EC_SIGN_SECP256K1_SHA256 => Right(SigningKeySpec.EcSecp256k1)
      case _ => Left(s"Unsupported signing key type: ${keySpec.toString}")
    }

  private def convertFromGcpAsymmetricEncryptionSpec(
      keySpec: CryptoKeyVersionAlgorithm
  ): Either[String, EncryptionKeySpec] =
    keySpec match {
      case CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256 =>
        Right(EncryptionKeySpec.Rsa2048)
      case _ => Left(s"Unsupported encryption key type: ${keySpec.toString}")
    }

  override protected def keyExistsAndIsActiveInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    retrieveKeyMetadata(keyId)
      .leftMap[KmsError] {
        case err: KmsRetrieveKeyMetadataError if !err.retryable =>
          KmsCannotFindKeyError(keyId, err.show)
        case err => err
      }
      .flatMap { keyMetadata =>
        keyMetadata.getState match {
          case pending @ CryptoKeyVersion.CryptoKeyVersionState.PENDING_GENERATION =>
            EitherT.leftT[FutureUnlessShutdown, Unit](
              KmsKeyDisabledError(
                keyId,
                s"key is $pending",
                retryable = true,
              )
            )
          case CryptoKeyVersion.CryptoKeyVersionState.ENABLED =>
            EitherT.rightT[FutureUnlessShutdown, KmsError](())
          // non retryable error
          case otherState =>
            EitherT.leftT[FutureUnlessShutdown, Unit](
              KmsKeyDisabledError(
                keyId,
                s"key is $otherState",
              )
            )
        }
      }

  override protected def encryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144] = {
    val keyName = cryptoKeyName(keyId)
    val encryptionAlgorithm = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    for {
      dataEnc <- loggerKms.withLogging[ByteString](
        loggerKms.encryptRequestMsg(keyId.unwrap, encryptionAlgorithm.name),
        _ => loggerKms.encryptResponseMsg(keyId.unwrap, encryptionAlgorithm.name),
      )(
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsEncryptError(keyId, errStr, retryable),
          functionName = functionFullName,
        )(kmsClient.encrypt(keyName, data.unwrap).getCiphertext)
      )
      ciphertext <- ByteString6144
        .create(dataEnc)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[KmsError](err =>
          KmsError
            .KmsEncryptError(keyId, s"generated ciphertext does not adhere to bound: $err)")
        )
    } yield ciphertext
  }

  override protected def decryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString6144,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096] = {
    val keyName = cryptoKeyName(keyId)
    val encryptionAlgorithm = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    for {
      dataPlain <- loggerKms.withLogging[ByteString](
        loggerKms.decryptRequestMsg(keyId.unwrap, encryptionAlgorithm.name),
        _ => loggerKms.decryptResponseMsg(keyId.unwrap, encryptionAlgorithm.name),
      )(
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsDecryptError(keyId, errStr, retryable),
          functionName = functionFullName,
        )(
          kmsClient.decrypt(keyName, data.unwrap).getPlaintext
        )
      )
      plaintext <- ByteString4096
        .create(dataPlain)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[KmsError](err =>
          KmsError.KmsDecryptError(keyId, s"plaintext does not adhere to bound: $err)")
        )
    } yield plaintext
  }

  override protected def decryptAsymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190] =
    for {
      keyName <- cryptoKeyVersionName(keyId)
      encryptionAlgorithm <- convertToGcpAsymmetricEncryptionSpec(encryptionAlgorithmSpec)
        .leftMap(err => KmsDecryptError(keyId, err))
        .toEitherT[FutureUnlessShutdown]
      dataPlain <- loggerKms.withLogging[ByteString](
        loggerKms.decryptRequestMsg(keyId.unwrap, encryptionAlgorithm.name),
        _ => loggerKms.decryptResponseMsg(keyId.unwrap, encryptionAlgorithm.name),
      )(
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsDecryptError(keyId, errStr, retryable),
          functionName = functionFullName,
        )(
          kmsClient.asymmetricDecrypt(keyName, data.unwrap).getPlaintext
        )
      )
      plaintext <- ByteString190
        .create(dataPlain)
        .toEitherT[FutureUnlessShutdown]
        .leftMap[KmsError](err =>
          KmsError.KmsDecryptError(keyId, s"plaintext does not adhere to bound: $err)")
        )
    } yield plaintext

  private def signWithAlgorithm(
      keyId: KmsKeyId,
      keyVersionName: gcp.CryptoKeyVersionName,
      signingAlgorithm: CryptoKeyVersionAlgorithm,
      data: ByteString,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    loggerKms.withLogging[ByteString](
      loggerKms.signRequestMsg(keyId.unwrap, "data", signingAlgorithm.name),
      _ => loggerKms.signResponseMsg(keyId.unwrap, signingAlgorithm.name),
    )(
      wrapKmsCall(
        kmsErrorGen = (errStr, retryable) => KmsSignError(keyId, errStr, retryable),
        functionName = functionFullName,
      ) {
        val request =
          AsymmetricSignRequest.newBuilder().setData(data).setName(keyVersionName.toString).build()
        kmsClient.asymmetricSign(request).getSignature
      }
    )

  override protected def signInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
      signingAlgorithmSpec: SigningAlgorithmSpec,
      signingKeySpec: SigningKeySpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    cryptoKeyVersionName(keyId).flatMap { keyVersionName =>
      signingAlgorithmSpec match {
        case SigningAlgorithmSpec.EcDsaSha256 =>
          signingKeySpec match {
            case SigningKeySpec.EcP256 =>
              signWithAlgorithm(
                keyId,
                keyVersionName,
                CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256,
                data.unwrap,
              )
            case SigningKeySpec.EcSecp256k1 =>
              signWithAlgorithm(
                keyId,
                keyVersionName,
                CryptoKeyVersionAlgorithm.EC_SIGN_SECP256K1_SHA256,
                data.unwrap,
              )
            case SigningKeySpec.EcP384 | SigningKeySpec.EcCurve25519 =>
              EitherT.leftT[FutureUnlessShutdown, ByteString](
                KmsError.KmsSignError(
                  keyId,
                  s"unsupported signing key spec $signingKeySpec for algorithm $signingAlgorithmSpec",
                )
              )
          }
        case SigningAlgorithmSpec.EcDsaSha384 =>
          signWithAlgorithm(
            keyId,
            keyVersionName,
            CryptoKeyVersionAlgorithm.EC_SIGN_P384_SHA384,
            data.unwrap,
          )
        case SigningAlgorithmSpec.Ed25519 =>
          signWithAlgorithm(
            keyId,
            keyVersionName,
            CryptoKeyVersionAlgorithm.EC_SIGN_ED25519,
            data.unwrap,
          )
      }
    }

  override protected def deleteKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    cryptoKeyVersionName(keyId).flatMap { keyVersionName =>
      // Drop the cached resolved version since after destruction the version is no longer
      // usable; subsequent operations should re-list and pick a different one (or fail
      // upstream).
      resolvedKeyVersions.remove(keyId.unwrap).discard
      loggerKms.withLogging[Unit](
        loggerKms.deleteKeyRequestMsg(keyId.unwrap),
        _ => loggerKms.deleteKeyResponseMsg(keyId.unwrap),
      )(
        wrapKmsCall(
          kmsErrorGen = (errStr, retryable) => KmsDeleteKeyError(keyId, errStr, retryable),
          functionName = functionFullName,
        )(
          kmsClient.destroyCryptoKeyVersion(keyVersionName).discard
        )
      )
    }

  /** Re-issues the list lookup (rather than going through the cached version) so that callers
    * such as `keyExistsAndIsActive` always see the current state of the latest key version.
    */
  private def retrieveKeyMetadata(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, gcp.CryptoKeyVersion] =
    listLatestCryptoKeyVersion(keyId)

  override def onClosed(): Unit = LifeCycle.close(kmsClient)(logger)

}

object GcpKms extends Kms.SupportedSchemes {

  val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
    NonEmpty.mk(
      Set,
      SigningKeySpec.EcP256,
      SigningKeySpec.EcP384,
      SigningKeySpec.EcSecp256k1,
      SigningKeySpec.EcCurve25519,
    )

  val supportedSigningAlgoSpecs: NonEmpty[Set[SigningAlgorithmSpec]] =
    NonEmpty.mk(
      Set,
      SigningAlgorithmSpec.EcDsaSha256,
      SigningAlgorithmSpec.EcDsaSha384,
      SigningAlgorithmSpec.Ed25519,
    )

  val supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]] =
    NonEmpty.mk(Set, EncryptionKeySpec.Rsa2048)

  val supportedEncryptionAlgoSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]] =
    NonEmpty.mk(Set, EncryptionAlgorithmSpec.RsaOaepSha256)

  def create(
      config: KmsConfig.Gcp,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): Either[KmsError, GcpKms] =
    for {
      kms <-
        Either
          .catchOnly[IOException] {
            val credentials = GoogleCredentials.getApplicationDefault()
            val keyManagementServiceSettings =
              config.endpointOverride match {
                case Some(endpoint) =>
                  val channelProvider = gcp.KeyManagementServiceSettings
                    .defaultGrpcTransportProviderBuilder()
                    .setEndpoint(endpoint)
                    .setChannelConfigurator(
                      new ApiFunction[ManagedChannelBuilder[?], ManagedChannelBuilder[?]] {
                        override def apply(
                            managedChannelBuilder: ManagedChannelBuilder[?]
                        ): ManagedChannelBuilder[?] = {
                          managedChannelBuilder
                            .overrideAuthority("cloudkms.googleapis.com")
                          managedChannelBuilder
                        }
                      }
                    )
                    .build()

                  gcp.KeyManagementServiceSettings
                    .newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .setTransportChannelProvider(channelProvider)
                    .setEndpoint(endpoint)
                    .build()
                case None =>
                  gcp.KeyManagementServiceSettings
                    .newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                    .build()
              }
            new GcpKms(
              config,
              gcp.LocationName.of(config.projectId, config.locationId),
              gcp.KeyManagementServiceClient.create(keyManagementServiceSettings),
              timeouts,
              loggerFactory,
            )
          }
          .leftMap[KmsError](err => KmsCreateClientError(ThrowableUtil.messageWithStacktrace(err)))
    } yield kms

}
