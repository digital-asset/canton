// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.tracing.TraceContext
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.hybrid.HybridKeyTemplates
import com.google.crypto.tink.proto.*
import com.google.crypto.tink.signature.SignatureKeyTemplates

import java.security.GeneralSecurityException
import scala.concurrent.{ExecutionContext, Future}

class TinkPrivateCrypto private (
    pureCrypto: TinkPureCrypto,
    override val defaultSigningKeyScheme: SigningKeyScheme,
    override val defaultEncryptionKeyScheme: EncryptionKeyScheme,
    override protected val store: CryptoPrivateStore,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi {

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  private def generateKey[E](
      keyTemplate: KeyTemplate,
      errFn: GeneralSecurityException => E,
  ): Either[E, KeysetHandle] =
    Either
      .catchOnly[GeneralSecurityException](KeysetHandle.generateNew(keyTemplate))
      .leftMap(errFn)

  override protected def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair] =
    for {
      keyTemplate <- scheme match {
        case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
          EitherT.rightT(HybridKeyTemplates.ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM)
      }
      keysetHandle <- generateKey[EncryptionKeyGenerationError](
        keyTemplate,
        EncryptionKeyGenerationError.GeneralError,
      ).toEitherT
      publicKeysetHandle = keysetHandle.getPublicKeysetHandle
      fingerprint <- TinkKeyFormat
        .fingerprint(publicKeysetHandle, pureCrypto.defaultHashAlgorithm)
        .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.FingerprintError)
        .toEitherT
      keypair = EncryptionKeyPair.create(
        id = fingerprint,
        format = CryptoKeyFormat.Tink,
        publicKeyBytes = TinkKeyFormat.serializeHandle(publicKeysetHandle),
        privateKeyBytes = TinkKeyFormat.serializeHandle(keysetHandle),
        scheme = scheme,
      )
    } yield keypair

  override protected[canton] def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair] = {
    for {
      keyTemplate <- for {
        template <- scheme match {
          case SigningKeyScheme.Ed25519 =>
            EitherT.rightT(SignatureKeyTemplates.ED25519)
          case SigningKeyScheme.EcDsaP256 =>
            EitherT.rightT(SignatureKeyTemplates.ECDSA_P256)
          case SigningKeyScheme.EcDsaP384 =>
            // Overwrite the hash function to use SHA384
            EitherT.rightT(
              SignatureKeyTemplates.createEcdsaKeyTemplate(
                HashType.SHA384,
                EllipticCurveType.NIST_P384,
                EcdsaSignatureEncoding.DER,
                OutputPrefixType.RAW,
              )
            )
          case SigningKeyScheme.Sm2 =>
            EitherT.leftT(SigningKeyGenerationError.UnsupportedKeyScheme(scheme))
        }
      } yield {
        // Uses RAW key templates such that the signatures are not prefixed with a Tink prefix.
        KeyTemplate
          .newBuilder(template)
          .setOutputPrefixType(OutputPrefixType.RAW)
          .build()
      }
      keysetHandle <- generateKey[SigningKeyGenerationError](
        keyTemplate,
        SigningKeyGenerationError.GeneralError,
      ).toEitherT
      publicKeysetHandle = keysetHandle.getPublicKeysetHandle
      fingerprint <- TinkKeyFormat
        .fingerprint(publicKeysetHandle, pureCrypto.defaultHashAlgorithm)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.FingerprintError)
        .toEitherT
      keypair = SigningKeyPair.create(
        id = fingerprint,
        format = CryptoKeyFormat.Tink,
        publicKeyBytes = TinkKeyFormat.serializeHandle(publicKeysetHandle),
        privateKeyBytes = TinkKeyFormat.serializeHandle(keysetHandle),
        scheme = scheme,
      )
    } yield keypair
  }
}

object TinkPrivateCrypto {
  def create(
      pureCrypto: TinkPureCrypto,
      defaultSigningKeyScheme: SigningKeyScheme,
      defaultEncryptionKeyScheme: EncryptionKeyScheme,
      privateStore: CryptoPrivateStore,
  )(implicit ec: ExecutionContext): TinkPrivateCrypto =
    new TinkPrivateCrypto(
      pureCrypto,
      defaultSigningKeyScheme,
      defaultEncryptionKeyScheme,
      privateStore,
    )
}
