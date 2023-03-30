// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.CryptoProvider
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.tink.TinkJavaConverter
import com.google.crypto.tink.subtle.EllipticCurves
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.DEROctetString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.gm.GMObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.sec.SECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters
import sun.security.ec.ECPrivateKeyImpl

import java.io.IOException
import java.security.spec.{InvalidKeySpecException, PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.security.{
  GeneralSecurityException,
  KeyFactory,
  NoSuchAlgorithmException,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
}

class JceJavaConverter(hashAlgorithm: HashAlgorithm) extends JavaKeyConverter {

  import com.digitalasset.canton.util.ShowUtil.*

  private def ensureFormat(
      key: CryptoKey,
      format: CryptoKeyFormat,
  ): Either[JavaKeyConversionError, Unit] =
    Either.cond(
      key.format == format,
      (),
      JavaKeyConversionError.UnsupportedKeyFormat(key.format, format),
    )

  private def toJavaEcDsa(
      privateKey: PrivateKey,
      curveType: CurveType,
  ): Either[JavaKeyConversionError, JPrivateKey] =
    for {
      _ <- ensureFormat(privateKey, CryptoKeyFormat.Der)
      ecPrivateKey <- Either
        .catchOnly[GeneralSecurityException](
          EllipticCurves.getEcPrivateKey(curveType, privateKey.key.toByteArray)
        )
        .leftMap(JavaKeyConversionError.GeneralError)
      _ =
        // There exists a race condition in ECPrivateKey before java 15
        if (Runtime.version.feature < 15)
          ecPrivateKey match {
            case pk: ECPrivateKeyImpl =>
              /* Force the initialization of the private key's internal array data structure.
               * This prevents concurrency problems later on during decryption, while generating the shared secret,
               * due to a race condition in getArrayS.
               */
              pk.getArrayS.discard
            case _ => ()
          }
    } yield ecPrivateKey

  private def toJavaEcDsa(
      publicKey: PublicKey,
      curveType: CurveType,
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
    for {
      _ <- ensureFormat(publicKey, CryptoKeyFormat.Der)
      // We are using the tink-subtle API here, thus using the TinkJavaConverter to have a consistent mapping of curve
      // type to algo id.
      algoId <- TinkJavaConverter
        .fromCurveType(curveType)
        .leftMap(JavaKeyConversionError.InvalidKey)
      ecPublicKey <- Either
        .catchOnly[GeneralSecurityException](
          EllipticCurves.getEcPublicKey(publicKey.key.toByteArray)
        )
        .leftMap(JavaKeyConversionError.GeneralError)
    } yield (algoId, ecPublicKey)

  override def toJava(privateKey: PrivateKey): Either[JavaKeyConversionError, JPrivateKey] = {

    def convert(
        format: CryptoKeyFormat,
        pkcs8PrivateKey: Array[Byte],
        keyInstance: String,
    ): Either[JavaKeyConversionError, JPrivateKey] =
      for {
        _ <- ensureFormat(privateKey, format)
        pkcs8KeySpec = new PKCS8EncodedKeySpec(pkcs8PrivateKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](KeyFactory.getInstance(keyInstance, "BC"))
          .leftMap(JavaKeyConversionError.GeneralError)
        javaPrivateKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePrivate(pkcs8KeySpec))
          .leftMap(err => JavaKeyConversionError.InvalidKey(show"$err"))
      } yield javaPrivateKey

    (privateKey: @unchecked) match {
      case sigKey: SigningPrivateKey =>
        sigKey.scheme match {
          case SigningKeyScheme.Ed25519 =>
            val privateKeyInfo = new PrivateKeyInfo(
              new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519),
              new DEROctetString(privateKey.key.toByteArray),
            )
            convert(CryptoKeyFormat.Raw, privateKeyInfo.getEncoded, "Ed25519")

          case SigningKeyScheme.Sm2 =>
            convert(CryptoKeyFormat.Der, privateKey.key.toByteArray, "EC")

          case SigningKeyScheme.EcDsaP256 => toJavaEcDsa(privateKey, CurveType.NIST_P256)
          case SigningKeyScheme.EcDsaP384 => toJavaEcDsa(privateKey, CurveType.NIST_P384)
        }
      case encKey: EncryptionPrivateKey =>
        encKey.scheme match {
          case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
            toJavaEcDsa(privateKey, CurveType.NIST_P256)
          case EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
            convert(CryptoKeyFormat.Der, privateKey.key.toByteArray, "EC")
        }
    }
  }

  override def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] = {

    def convert(
        format: CryptoKeyFormat,
        x509PublicKey: Array[Byte],
        keyInstance: String,
    ): Either[JavaKeyConversionError, JPublicKey] =
      for {
        _ <- ensureFormat(publicKey, format)
        x509KeySpec = new X509EncodedKeySpec(x509PublicKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](KeyFactory.getInstance(keyInstance, "BC"))
          .leftMap(JavaKeyConversionError.GeneralError)
        javaPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(x509KeySpec))
          .leftMap(err => JavaKeyConversionError.InvalidKey(show"$err"))
      } yield javaPublicKey

    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey =>
        sigKey.scheme match {
          case SigningKeyScheme.Ed25519 =>
            val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
            val x509PublicKey = new SubjectPublicKeyInfo(algoId, publicKey.key.toByteArray)
            convert(CryptoKeyFormat.Raw, x509PublicKey.getEncoded, "Ed25519").map(pk =>
              (algoId, pk)
            )

          case SigningKeyScheme.Sm2 =>
            val algoId = new AlgorithmIdentifier(GMObjectIdentifiers.sm2p256v1)
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "EC").map(pk => (algoId, pk))

          case SigningKeyScheme.EcDsaP256 => toJavaEcDsa(publicKey, CurveType.NIST_P256)
          case SigningKeyScheme.EcDsaP384 => toJavaEcDsa(publicKey, CurveType.NIST_P384)
        }
      case encKey: EncryptionPublicKey =>
        encKey.scheme match {
          case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
            toJavaEcDsa(publicKey, CurveType.NIST_P256)
          case EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
            val algoId = new AlgorithmIdentifier(SECObjectIdentifiers.secp256r1)
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "EC").map(pk => (algoId, pk))

        }
    }
  }

  override def fromJavaSigningKey(
      javaPublicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {

    def ensureJceSupportedScheme(scheme: SigningKeyScheme): Either[JavaKeyConversionError, Unit] = {
      val supportedSchemes = CryptoProvider.Jce.signing.supported
      Either.cond(
        supportedSchemes.contains(scheme),
        (),
        JavaKeyConversionError.UnsupportedKeyScheme(scheme, supportedSchemes),
      )
    }

    javaPublicKey.getAlgorithm match {
      case "EC" =>
        for {
          scheme <- JavaKeyConverter.toSigningKeyScheme(algorithmIdentifier)
          _ <- Either.cond(
            SigningKeyScheme.EcSchemes.contains(scheme),
            (),
            JavaKeyConversionError.UnsupportedKeyScheme(scheme, SigningKeyScheme.EcSchemes),
          )
          _ <- ensureJceSupportedScheme(scheme)

          publicKeyBytes = ByteString.copyFrom(javaPublicKey.getEncoded)
          id = Fingerprint.create(publicKeyBytes, hashAlgorithm)
          publicKey = new SigningPublicKey(
            id = id,
            format = CryptoKeyFormat.Der,
            key = publicKeyBytes,
            scheme = scheme,
          )
        } yield publicKey

      // With support for EdDSA (since Java 15) the more general 'EdDSA' algorithm identifier is also used
      // See https://bugs.openjdk.org/browse/JDK-8190219
      case "Ed25519" | "EdDSA" =>
        for {
          scheme <- JavaKeyConverter.toSigningKeyScheme(algorithmIdentifier)
          _ <- Either.cond(
            scheme == SigningKeyScheme.Ed25519,
            (),
            JavaKeyConversionError.UnsupportedKeyScheme(
              scheme,
              NonEmpty.mk(Set, SigningKeyScheme.Ed25519),
            ),
          )
          _ <- ensureJceSupportedScheme(scheme)

          // The encoded public key has a prefix that we drop
          publicKeyEncoded = ByteString.copyFrom(
            javaPublicKey.getEncoded.takeRight(Ed25519PublicKeyParameters.KEY_SIZE)
          )
          publicKeyParams <- Either
            .catchOnly[IOException](new Ed25519PublicKeyParameters(publicKeyEncoded.newInput()))
            .leftMap(JavaKeyConversionError.GeneralError)
          publicKeyBytes = ByteString.copyFrom(publicKeyParams.getEncoded)
          id = Fingerprint.create(publicKeyBytes, hashAlgorithm)
          publicKey = new SigningPublicKey(
            id = id,
            format = CryptoKeyFormat.Raw,
            key = publicKeyBytes,
            scheme = scheme,
          )
        } yield publicKey

      case unsupportedAlgo =>
        Left(
          JavaKeyConversionError.InvalidKey(
            s"Java public key of kind $unsupportedAlgo not supported."
          )
        )
    }
  }

}
