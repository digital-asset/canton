// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SigningKeySpec
import com.digitalasset.canton.crypto.provider.jce
import io.circe.Json
import io.circe.syntax.*
import org.bouncycastle.util.BigIntegers

import java.nio.charset.StandardCharsets.UTF_8
import java.security.interfaces.{ECPublicKey, EdECPublicKey}
import java.security.{MessageDigest, PublicKey as JavaPublicKey}
import java.util.Base64
import scala.collection.immutable.SortedMap

object JceJwks {
  private[crypto] def toJwk(publicKey: SigningPublicKey): Either[JwksError, Jwk] =
    for {
      jwkFactory <- JwkFactory.fromKeySpec(publicKey.keySpec)

      javaPublicKey <- jce.JceJavaKeyConverter
        .toJava(publicKey)
        .leftMap(err => JwksError.JceJavaKeyConversionError(err))

      additionalAttributes <- jwkFactory.additionalAttributes(javaPublicKey)
      thumbprintAttributes = additionalAttributes + ("kty" -> Json.fromString(jwkFactory.kty.value))

      jwk = Jwk(
        kid = thumbprint(thumbprintAttributes),
        kty = jwkFactory.kty,
        alg = jwkFactory.alg,
        additionalAttributes = additionalAttributes,
      )
    } yield jwk

  private def base64Url(bytes: Array[Byte]): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  private sealed trait JwkFactory {

    /** `kty` as defined by the JWK specification. */
    def kty: Jwk.Kty

    /** `alg` as defined by the JWK specification. */
    def alg: Jwk.Alg

    /** Retrieve the required attributes to encode the public key. */
    def additionalAttributes(javaPublicKey: JavaPublicKey): Either[JwksError, Map[String, Json]]
  }

  private object JwkFactory {
    def fromKeySpec: (SigningKeySpec => Either[JwksError, JwkFactory]) = {
      case SigningKeySpec.EcP256 => Right(JwkFactory.EcP256: JwkFactory)
      case SigningKeySpec.EcP384 => Right(JwkFactory.EcP384)
      case SigningKeySpec.EcCurve25519 => Right(JwkFactory.EcCurve25519)
      case SigningKeySpec.EcSecp256k1 => Right(JwkFactory.EcSecp256k1)
      case other => Left(JwksError.UnsupportedKeySpec(other))
    }

    case object EcP256 extends JwkFactory {
      override def kty = Jwk.Kty.EC
      override def alg = Jwk.Alg.ES256
      override def additionalAttributes(javaPublicKey: JavaPublicKey) = javaPublicKey match {
        case ec: ECPublicKey => {
          val w = ec.getW
          val x = base64Url(BigIntegers.asUnsignedByteArray(32, w.getAffineX))
          val y = base64Url(BigIntegers.asUnsignedByteArray(32, w.getAffineY))
          Right(
            Map(
              "crv" -> Json.fromString("P-256"),
              "x" -> Json.fromString(x),
              "y" -> Json.fromString(y),
            )
          )
        }

        case _ =>
          Left(JwksError.KeyParameterExtractionError("EcP256: Expecting ECPublicKey"))
      }
    }

    case object EcP384 extends JwkFactory {
      override def kty = Jwk.Kty.EC
      override def alg = Jwk.Alg.ES384
      override def additionalAttributes(javaPublicKey: JavaPublicKey) = javaPublicKey match {
        case ec: ECPublicKey => {
          val w = ec.getW
          val x = base64Url(BigIntegers.asUnsignedByteArray(48, w.getAffineX))
          val y = base64Url(BigIntegers.asUnsignedByteArray(48, w.getAffineY))
          Right(
            Map(
              "crv" -> Json.fromString("P-384"),
              "x" -> Json.fromString(x),
              "y" -> Json.fromString(y),
            )
          )
        }

        case _ =>
          Left(JwksError.KeyParameterExtractionError("EcP384: Expecting ECPublicKey"))
      }
    }

    /** Ed25519-based JWKS as defined in <https://datatracker.ietf.org/doc/html/rfc8037> */
    case object EcCurve25519 extends JwkFactory {
      override def kty = Jwk.Kty.OKP
      override def alg = Jwk.Alg.EdDSA
      override def additionalAttributes(javaPublicKey: JavaPublicKey) = javaPublicKey match {
        case eced: EdECPublicKey => {
          // The encoding of the "x" parameter is defined in
          // <https://datatracker.ietf.org/doc/html/rfc8032>.
          // This boils down to little endian encoding with a sign bit.
          val point = eced.getPoint
          val bigEndian = BigIntegers.asUnsignedByteArray(32, point.getY)
          val bytes = bigEndian.reverse
          if (point.isXOdd) bytes(31) = (bytes(31) | 0x80).toByte
          Right(
            Map(
              "crv" -> Json.fromString("Ed25519"),
              "x" -> Json.fromString(base64Url(bytes)),
            )
          )
        }

        case _ =>
          Left(
            JwksError.KeyParameterExtractionError("EcCurve25519: Expecting EdECPublicKey")
          )
      }
    }

    /** EcSecp256k1-based JWKS as defined in <https://datatracker.ietf.org/doc/html/rfc8812> */
    case object EcSecp256k1 extends JwkFactory {
      override def kty = Jwk.Kty.EC
      override def alg = Jwk.Alg.ES256K
      override def additionalAttributes(javaPublicKey: JavaPublicKey) = javaPublicKey match {
        case ec: ECPublicKey => {
          val w = ec.getW
          val x = base64Url(BigIntegers.asUnsignedByteArray(32, w.getAffineX))
          val y = base64Url(BigIntegers.asUnsignedByteArray(32, w.getAffineY))
          Right(
            Map(
              "crv" -> Json.fromString("secp256k1"),
              "x" -> Json.fromString(x),
              "y" -> Json.fromString(y),
            )
          )
        }

        case _ =>
          Left(
            JwksError.KeyParameterExtractionError("EcSecp256k1: Expecting ECPublicKey")
          )
      }
    }
  }

  /** As defined by <https://datatracker.ietf.org/doc/html/rfc7638>: hash of the required elements
    * of the JWK as JSON. The JSON uses compact notation (noSpaces) and lexiographically sorted
    * members (SortedMap).
    */
  private[crypto] def thumbprint(attributes: Map[String, Json]): String =
    base64Url(
      MessageDigest
        .getInstance("SHA-256")
        .digest(SortedMap.from(attributes).asJson.noSpaces.getBytes(UTF_8))
    )
}
