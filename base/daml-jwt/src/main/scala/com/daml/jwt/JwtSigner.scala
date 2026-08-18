// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.algorithms.Algorithm
import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.nio.charset.Charset
import java.security.Signature
import java.security.interfaces.{ECPrivateKey, EdECPrivateKey, RSAPrivateKey}

trait JwtSigner {

  /** Kept for backwards-compat with older tests, prefer signPayload. */
  def sign(jwt: DecodedJwt[String]): Either[Error, Jwt]

  def alg: String
  def kid: Option[String]

  final def signPayload(payload: String): Either[Error, Jwt] = {
    val header = kid match {
      case Some(keyId) => s"""{"alg": "$alg", "typ": "JWT", "kid": "$keyId"}"""
      case None => s"""{"alg": "$alg", "typ": "JWT"}"""
    }
    sign(DecodedJwt(header, payload))
  }
}

object JwtSigner extends WithExecuteUnsafe {

  private val charset = Charset.forName("ASCII")

  // Used for some of the more exotic algorithms.  Having a dedicated provider here is fine since JwtSigner is only used in tests.
  private lazy val bouncyCastleProvider: BouncyCastleProvider = new BouncyCastleProvider()

  final case class HMAC256(secret: String, kid: Option[String] = None) extends JwtSigner {
    def alg = "HS256"
    def sign(jwt: DecodedJwt[String]): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(Algorithm.HMAC256(secret), Symbol("HMAC256.sign"))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol("HMAC256.sign"),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  final case class RSA256(privateKey: RSAPrivateKey, kid: Option[String] = None) extends JwtSigner {
    def alg = "RS256"
    def sign(jwt: DecodedJwt[String]): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(Algorithm.RSA256(null, privateKey), Symbol("RSA256.sign"))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol("RSA256.sign"),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  sealed class ECDSA(privateKey: ECPrivateKey, algorithm: ECPrivateKey => Algorithm) {
    def sign(jwt: DecodedJwt[String]): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(algorithm(privateKey), Symbol(algorithm.getClass.getTypeName))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol(algorithm.getClass.getTypeName),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  final case class ES256(privateKey: ECPrivateKey, kid: Option[String] = None)
      extends ECDSA(privateKey, Algorithm.ECDSA256)
      with JwtSigner {
    def alg = "ES256"
  }

  final case class ES384(privateKey: ECPrivateKey, kid: Option[String] = None)
      extends ECDSA(privateKey, Algorithm.ECDSA384)
      with JwtSigner {
    def alg = "ES384"
  }

  final case class ES512(privateKey: ECPrivateKey, kid: Option[String] = None)
      extends ECDSA(privateKey, Algorithm.ECDSA512)
      with JwtSigner {
    def alg = "ES512"
  }

  final case class EdDSA(privateKey: EdECPrivateKey, kid: Option[String] = None) extends JwtSigner {
    def alg = "EdDSA"
    def sign(jwt: DecodedJwt[String]): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)
        bytesToSign = base64Jwt.header ++ Array('.': Byte) ++ base64Jwt.payload
        sig = Signature.getInstance("Ed25519", bouncyCastleProvider)
        _ = sig.initSign(privateKey)
        _ = sig.update(bytesToSign)
        signature = sig.sign();
        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  private def str(bs: Array[Byte]) = new String(bs, charset)

  private def base64Encode(a: DecodedJwt[String]): Either[Error, DecodedJwt[Array[Byte]]] =
    a.transform(base64Encode)

  private def base64Encode(str: String): Either[Error, Array[Byte]] =
    base64Encode(str.getBytes)

  private def base64Encode(bs: Array[Byte]): Either[Error, Array[Byte]] =
    Base64
      .encodeWithoutPadding(bs)
      .left
      .map(_.within(Symbol("JwtSigner.base64Encode")))
}
