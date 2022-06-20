// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.data.EitherT
import cats.instances.future._
import cats.syntax.either._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.tracing.TraceContext
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.crypto.tink.subtle.{Ed25519Sign, EllipticCurves}
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.gm.{GMNamedCurves, GMObjectIdentifiers}
import org.bouncycastle.jce.spec.ECNamedCurveParameterSpec

import java.security.{
  GeneralSecurityException,
  InvalidAlgorithmParameterException,
  KeyPair => JKeyPair,
  KeyPairGenerator,
  NoSuchAlgorithmException,
}
import scala.concurrent.{ExecutionContext, Future}

class JcePrivateCrypto(
    pureCrypto: JcePureCrypto,
    override val defaultSigningKeyScheme: SigningKeyScheme,
    override val defaultEncryptionKeyScheme: EncryptionKeyScheme,
    override protected val store: CryptoPrivateStore,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi {

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  // Internal case class to ensure we don't mix up the private and public key bytestrings
  private case class RawKeyPair(id: Fingerprint, publicKey: ByteString, privateKey: ByteString)

  private def fingerprint(publicKey: ByteString): Fingerprint =
    Fingerprint.create(publicKey, pureCrypto.defaultHashAlgorithm)

  private def fromJavaKeyPair(javaKeyPair: JKeyPair): RawKeyPair = {
    // Encode public key as X509 subject public key info in DER
    val publicKey = ByteString.copyFrom(javaKeyPair.getPublic.getEncoded)

    // Encode private key as PKCS8 in DER
    val privateKey = ByteString.copyFrom(javaKeyPair.getPrivate.getEncoded)

    val keyId = fingerprint(publicKey)

    RawKeyPair(keyId, publicKey, privateKey)
  }

  private def fromJavaSigningKeyPair(
      javaKeyPair: JKeyPair,
      scheme: SigningKeyScheme,
  ): SigningKeyPair = {
    val rawKeyPair = fromJavaKeyPair(javaKeyPair)
    SigningKeyPair.create(
      id = rawKeyPair.id,
      format = CryptoKeyFormat.Der,
      publicKeyBytes = rawKeyPair.publicKey,
      privateKeyBytes = rawKeyPair.privateKey,
      scheme = scheme,
    )
  }

  private def generateEcDsaSigningKeyPair(
      curveType: CurveType,
      scheme: SigningKeyScheme,
  ): Either[SigningKeyGenerationError, SigningKeyPair] =
    for {
      javaKeyPair <- Either
        .catchOnly[GeneralSecurityException](EllipticCurves.generateKeyPair(curveType))
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError)
    } yield fromJavaSigningKeyPair(javaKeyPair, scheme)

  override protected def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair] =
    EitherT.fromEither {
      scheme match {
        case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
          for {
            javaKeyPair <- Either
              .catchOnly[GeneralSecurityException](
                EllipticCurves.generateKeyPair(CurveType.NIST_P256)
              )
              .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.GeneralError)
            rawKeyPair = fromJavaKeyPair(javaKeyPair)
          } yield EncryptionKeyPair.create(
            id = rawKeyPair.id,
            format = CryptoKeyFormat.Der,
            publicKeyBytes = rawKeyPair.publicKey,
            privateKeyBytes = rawKeyPair.privateKey,
            scheme = scheme,
          )
      }
    }

  override protected def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair] = scheme match {
    case SigningKeyScheme.Ed25519 =>
      for {
        rawKeyPair <- Either
          .catchOnly[GeneralSecurityException](Ed25519Sign.KeyPair.newKeyPair())
          .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError)
          .toEitherT
        publicKey = ByteString.copyFrom(rawKeyPair.getPublicKey)
        privateKey = ByteString.copyFrom(rawKeyPair.getPrivateKey)
        id = fingerprint(publicKey)
        keyPair = SigningKeyPair
          .create(
            id = id,
            format = CryptoKeyFormat.Raw,
            publicKeyBytes = publicKey,
            privateKeyBytes = privateKey,
            scheme = scheme,
          )
      } yield keyPair

    case SigningKeyScheme.Sm2 =>
      for {
        keyGen <- Either
          .catchOnly[NoSuchAlgorithmException](KeyPairGenerator.getInstance("EC", "BC"))
          .leftMap(SigningKeyGenerationError.GeneralError)
          .toEitherT

        params = GMNamedCurves.getByOID(GMObjectIdentifiers.sm2p256v1)
        spec = new ECNamedCurveParameterSpec(
          GMObjectIdentifiers.sm2p256v1.toString,
          params.getCurve,
          params.getG,
          params.getN,
        )
        _ <- Either
          .catchOnly[InvalidAlgorithmParameterException](keyGen.initialize(spec))
          .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError)
          .toEitherT

        javaKeyPair = keyGen.generateKeyPair()
      } yield fromJavaSigningKeyPair(javaKeyPair, scheme)

    case SigningKeyScheme.EcDsaP256 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P256, scheme).toEitherT

    case SigningKeyScheme.EcDsaP384 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P384, scheme).toEitherT

  }

}
