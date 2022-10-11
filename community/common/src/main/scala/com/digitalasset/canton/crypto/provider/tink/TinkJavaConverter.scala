// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.tink.TinkKeyFormat.serializeHandle
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.crypto.tink.proto.KeyData.KeyMaterialType
import com.google.crypto.tink.proto.*
import com.google.crypto.tink.subtle.EllipticCurves
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.crypto.tink.{KeysetHandle, proto as tinkproto}
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.x509.AlgorithmIdentifier
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers

import java.security.interfaces.ECPublicKey
import java.security.{PrivateKey as JPrivateKey, PublicKey as JPublicKey}

/** Converter methods from Tink to Java security keys and vice versa. */
class TinkJavaConverter(hashAlgorithm: HashAlgorithm) extends JavaKeyConverter {

  /** Extract the first key from the keyset. In Canton we only deal with Tink keysets of size 1. */
  private def getFirstKey(
      keysetHandle: KeysetHandle
  ): Either[JavaKeyConversionError, tinkproto.Keyset.Key] =
    for {
      // No other way to access the public key directly than going via protobuf
      keysetProto <- ProtoConverter
        .protoParser(tinkproto.Keyset.parseFrom)(serializeHandle(keysetHandle))
        .leftMap(err =>
          JavaKeyConversionError.InvalidKey(s"Failed to parser tink keyset proto: $err")
        )
      key <- Either.cond(
        keysetProto.getKeyCount == 1,
        keysetProto.getKey(0),
        JavaKeyConversionError.InvalidKey(
          s"Not exactly one key in the keyset, but ${keysetProto.getKeyCount} keys."
        ),
      )
    } yield key

  /** Map EC-DSA public keys to the correct curves. */
  private def getCurve(publicKey: EcdsaPublicKey): Either[JavaKeyConversionError, CurveType] =
    publicKey.getParams.getCurveValue match {
      case com.google.crypto.tink.proto.EllipticCurveType.NIST_P256_VALUE =>
        EllipticCurves.CurveType.NIST_P256.asRight
      case com.google.crypto.tink.proto.EllipticCurveType.NIST_P384_VALUE =>
        EllipticCurves.CurveType.NIST_P384.asRight
      case unsupportedCurveValue =>
        JavaKeyConversionError
          .InvalidKey(s"Unsupported curve value: $unsupportedCurveValue")
          .asLeft[CurveType]
    }

  /** Sanity check that the given key data is of appropriate type (such as asymmetric public/private key). */
  private def verifyKeyType(
      keyData: KeyData,
      keyTypeValue: Int,
  ): Either[JavaKeyConversionError, Unit] = {
    val keyMaterialTypeValue = keyData.getKeyMaterialTypeValue
    Either.cond(
      keyMaterialTypeValue == keyTypeValue,
      (),
      JavaKeyConversionError.InvalidKey(
        s"Invalid key material type value $keyMaterialTypeValue, must be value: $keyTypeValue"
      ),
    )
  }

  /** Converts a Tink private key to a [[java.security.PrivateKey]].
    *
    * The conversion has to go via protobuf serialization of the Tink key as Tink does not provide access to the key
    * directly.
    */
  override def toJava(privateKey: PrivateKey): Either[JavaKeyConversionError, JPrivateKey] =
    for {
      keysetHandle <- TinkKeyFormat
        .deserializeHandle(privateKey.key)
        .leftMap(err => JavaKeyConversionError.InvalidKey(s"Failed to deserialize keyset: $err"))
      keyset0 <- getFirstKey(keysetHandle)
      keyData = keyset0.getKeyData

      // Sanity check that the key is a asymmetric private key and of type EC DSA
      _ <- verifyKeyType(keyData, KeyMaterialType.ASYMMETRIC_PRIVATE_VALUE)

      privateKey <- keyData.getTypeUrl match {
        case "type.googleapis.com/google.crypto.tink.EcdsaPrivateKey" =>
          for {
            protoPrivateKey <- ProtoConverter
              .protoParser(tinkproto.EcdsaPrivateKey.parseFrom)(keyData.getValue)
              .leftMap(err =>
                JavaKeyConversionError.InvalidKey(
                  s"Failed to parse key proto from keyset as EC-DSA: $err"
                )
              )
            curve <- getCurve(protoPrivateKey.getPublicKey)
            // Get the EC private key
            privKey = EllipticCurves.getEcPrivateKey(curve, protoPrivateKey.getKeyValue.toByteArray)
          } yield privKey
        case unsupportedKeyTypeUrl =>
          Left(
            JavaKeyConversionError.InvalidKey(s"Unsupported key type url: $unsupportedKeyTypeUrl")
          )
      }
    } yield privateKey

  /** Converts a Tink public key to a [[java.security.PublicKey]] with an X509 [[org.bouncycastle.asn1.x509.AlgorithmIdentifier]] for the curve.
    *
    * The conversion has to go via protobuf serialization of the Tink key as Tink does not provide access to the key
    * directly.
    */
  override def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
    for {
      keysetHandle <- TinkKeyFormat
        .deserializeHandle(publicKey.key)
        .leftMap(err => JavaKeyConversionError.InvalidKey(s"Failed to deserialize keyset: $err"))
      keyset0 <- getFirstKey(keysetHandle)
      keyData = keyset0.getKeyData

      // Sanity check that the key is a asymmetric public key and of type EC DSA
      _ <- verifyKeyType(keyData, KeyMaterialType.ASYMMETRIC_PUBLIC_VALUE)

      keyTuple <- keyData.getTypeUrl match {
        case "type.googleapis.com/google.crypto.tink.EcdsaPublicKey" =>
          for {
            protoPublicKey <- ProtoConverter
              .protoParser(tinkproto.EcdsaPublicKey.parseFrom)(keyData.getValue)
              .leftMap(err =>
                JavaKeyConversionError.InvalidKey(
                  s"Failed to parse key proto from keyset as EC DSA: $err"
                )
              )
            curve <- getCurve(protoPublicKey)
            algoId <- TinkJavaConverter
              .fromCurveType(curve)
              .leftMap(JavaKeyConversionError.InvalidKey)
            // Get the EC public key and verify that the public key is on the supported curve
            pubKey = EllipticCurves.getEcPublicKey(
              curve,
              protoPublicKey.getX.toByteArray,
              protoPublicKey.getY.toByteArray,
            )
          } yield (algoId, pubKey)
        case unsupportedKeyTypeUrl =>
          Left(
            JavaKeyConversionError.InvalidKey(s"Unsupported key type url: $unsupportedKeyTypeUrl")
          )
      }
    } yield keyTuple

  override def fromJavaSigningKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {
    publicKey match {
      case ecPubKey: ECPublicKey =>
        for {
          scheme <- JavaKeyConverter
            .toSigningKeyScheme(algorithmIdentifier)
          hashAndCurve <- scheme match {
            case SigningKeyScheme.EcDsaP256 =>
              (HashType.SHA256, EllipticCurveType.NIST_P256).asRight
            case SigningKeyScheme.EcDsaP384 =>
              (HashType.SHA384, EllipticCurveType.NIST_P384).asRight
            case SigningKeyScheme.Ed25519 | SigningKeyScheme.Sm2 =>
              JavaKeyConversionError
                .UnsupportedKeyScheme(scheme, SigningKeyScheme.EcDsaSchemes)
                .asLeft
          }
          (hashType, curve) = hashAndCurve

          // Importing a java public key into Tink has to go via protobuf
          ecdsaParams = tinkproto.EcdsaParams
            .newBuilder()
            .setHashType(hashType)
            .setCurve(curve)
            .setEncoding(tinkproto.EcdsaSignatureEncoding.DER)

          ecdsaPubKey = tinkproto.EcdsaPublicKey
            .newBuilder()
            .setVersion(0)
            .setParams(ecdsaParams)
            .setX(ByteString.copyFrom(ecPubKey.getW.getAffineX.toByteArray))
            .setY(ByteString.copyFrom(ecPubKey.getW.getAffineY.toByteArray))
            .build()

          keydata = tinkproto.KeyData
            .newBuilder()
            .setTypeUrl("type.googleapis.com/google.crypto.tink.EcdsaPublicKey")
            .setValue(ecdsaPubKey.toByteString)
            .setKeyMaterialType(KeyMaterialType.ASYMMETRIC_PUBLIC)
            .build()

          // We cannot reconstruct the key id from the java public key, therefore this public key only supports "raw" signatures.
          keyId = 0
          key = tinkproto.Keyset.Key
            .newBuilder()
            .setKeyData(keydata)
            .setStatus(KeyStatusType.ENABLED)
            .setKeyId(keyId)
            .setOutputPrefixType(OutputPrefixType.RAW)
            .build()

          keyset = tinkproto.Keyset.newBuilder().setPrimaryKeyId(keyId).addKey(key).build()
          keysetHandle <- TinkKeyFormat
            .deserializeHandle(keyset.toByteString)
            .leftMap(err => JavaKeyConversionError.InvalidKey(err.toString))
          fingerprint <- TinkKeyFormat
            .fingerprint(keysetHandle, hashAlgorithm)
            .leftMap(JavaKeyConversionError.InvalidKey)
        } yield new SigningPublicKey(
          fingerprint,
          CryptoKeyFormat.Tink,
          TinkKeyFormat.serializeHandle(keysetHandle),
          scheme,
        )

      case unsupportedKey =>
        Left(JavaKeyConversionError.InvalidKey(s"Unsupported Java public key: $unsupportedKey"))
    }
  }
}

object TinkJavaConverter {

  def fromCurveType(curveType: CurveType): Either[String, AlgorithmIdentifier] =
    for {
      asnObjId <- curveType match {
        // CCF prefers the X9 identifiers (ecdsa-shaX) and not the SEC OIDs (secp384r1)
        case CurveType.NIST_P256 => Right(X9ObjectIdentifiers.ecdsa_with_SHA256)
        case CurveType.NIST_P384 => Right(X9ObjectIdentifiers.ecdsa_with_SHA384)
        case CurveType.NIST_P521 => Left("Elliptic curve NIST-P521 not supported right now")
      }
    } yield new AlgorithmIdentifier(asnObjId)

}
