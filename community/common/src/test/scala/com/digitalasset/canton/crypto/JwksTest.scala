// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.auth0.jwk.Jwk as Auth0Jwk
import com.digitalasset.canton.crypto.provider.jce.{JceJavaKeyConverter, JceSecurityProvider}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import io.circe.Json
import org.bouncycastle.jce.ECNamedCurveTable
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.jce.spec.ECNamedCurveSpec
import org.scalatest.wordspec.AsyncWordSpec

import java.math.BigInteger
import java.security.spec.{
  ECPoint,
  ECPublicKeySpec,
  EdECPoint,
  EdECPublicKeySpec,
  NamedParameterSpec,
}
import java.security.{KeyFactory, PublicKey as JavaPublicKey, Security}
import java.util.{Base64, List as JavaList, Map as JavaMap}
import scala.jdk.CollectionConverters.*

trait JwksTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def jwksProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      unsupportedSigningKeySpecs: Set[SigningKeySpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit = {
    Security.addProvider(new BouncyCastleProvider())

    forAll(supportedSigningKeySpecs) { signingKeySpec =>
      s"JWK conversion for $signingKeySpec keys" should {

        "match the corresponding Java PublicKey" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              // proof of ownership is added internally
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            )
            jwk = crypto.pureCrypto.toJwk(publicKey).valueOrFail("toJwk(publicKey)")
            jPublicKey = JceJavaKeyConverter.toJava(publicKey).valueOrFail("toJava(publicKey)")

          } yield {
            toAuth0(jwk).getPublicKey() shouldBe jPublicKey
          }
        }
      }
    }

    forAll(unsupportedSigningKeySpecs) { signingKeySpec =>
      s"Public Key with (unsupported) $signingKeySpec key" should {

        "fail to convert to a JWK" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              // proof of ownership is added internally
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            )
            jwk = crypto.pureCrypto.toJwk(publicKey)

          } yield {
            jwk.left.value shouldBe JwksError.UnsupportedKeySpec(signingKeySpec)
          }
        }
      }
    }

  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def toAuth0(jwk: Jwk): Auth0Jwk = {
    val values = jwk.toJsonObject
    jwk.alg match {
      case Jwk.Alg.EdDSA => new EcCurve25519Jwk(values)
      case Jwk.Alg.ES256K => new EcSecp256k1Jwk(values)
      case _ =>
        Auth0Jwk.fromValues(values.flatMap { case (k, v) =>
          // Note that we currently only support strings here.  If we ever produce
          // JWKs containing arrays or other JSON values, this code needs to be
          // updated.
          v.asString.map(k -> (_: Object))
        }.asJava)
    }
  }

  /** Tiny helper for Auth0 conversion below. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def getOrNull(values: Map[String, Json], key: String): String =
    values.getOrElse(key, null).asString.orNull

  /** The auth0 library we are using does not support Ed25519-based JWKs. The Jwk class in that
    * library only stores attributes and provides a conversion method to Java public keys, so we
    * only really need to implement the latter.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private class EcCurve25519Jwk(values: Map[String, Json])
      extends Auth0Jwk(
        getOrNull(values, "kid"), // id
        getOrNull(values, "kty"), // type
        getOrNull(values, "alg"), // algorithm
        getOrNull(values, "use"), // usage
        (null: JavaList[String]), // operations
        (null: String), // certificateUrl
        (null: JavaList[String]), // certificateChain
        (null: String), // certificateThumbprint
        (Map[String, Object](
          "crv" -> getOrNull(values, "crv"),
          "x" -> getOrNull(values, "x"),
        ).asJava: JavaMap[String, Object]), // additionalAttributes
      ) {

    override def getPublicKey(): JavaPublicKey = {
      // Parse point from the "x" parameter, as defined by
      // <https://datatracker.ietf.org/doc/html/rfc8032>.
      // This boils down to little endian encoding with a sign bit.
      val bytes = Base64.getUrlDecoder.decode(getOrNull(values, "x"))
      val lastByte = bytes(bytes.length - 1)
      val odd = (lastByte & 0x80) != 0
      bytes(bytes.length - 1) = (lastByte & 0x7f).toByte // Clear sign bit
      val y = new BigInteger(1, bytes.reverse)

      // Construct Java Key.
      val point = new EdECPoint(odd, y)
      val params = NamedParameterSpec.ED25519
      val spec = new EdECPublicKeySpec(params, point)
      KeyFactory.getInstance("EdDSA").generatePublic(spec)
    }
  }

  /** See the comment for EcCurve25519Jwk. Note that secp256k1 requires BouncyCastle.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private class EcSecp256k1Jwk(values: Map[String, Json])
      extends Auth0Jwk(
        getOrNull(values, "kid"), // id
        getOrNull(values, "kty"), // type
        getOrNull(values, "alg"), // algorithm
        getOrNull(values, "use"), // usage
        (null: JavaList[String]), // operations
        (null: String), // certificateUrl
        (null: JavaList[String]), // certificateChain
        (null: String), // certificateThumbprint
        (Map[String, Object](
          "crv" -> getOrNull(values, "crv"),
          "x" -> getOrNull(values, "x"),
          "y" -> getOrNull(values, "y"),
        ).asJava: JavaMap[String, Object]), // additionalAttributes
      ) {

    override def getPublicKey(): JavaPublicKey = {
      val xBytes = Base64.getUrlDecoder.decode(getOrNull(values, "x"))
      val yBytes = Base64.getUrlDecoder.decode(getOrNull(values, "y"))
      val x = new BigInteger(1, xBytes)
      val y = new BigInteger(1, yBytes)
      val curveParams = ECNamedCurveTable.getParameterSpec("secp256k1")
      val curveSpec = new ECNamedCurveSpec(
        "secp256k1",
        curveParams.getCurve,
        curveParams.getG,
        curveParams.getN,
        curveParams.getH,
      )
      val point = new ECPoint(x, y)
      val keySpec = new ECPublicKeySpec(point, curveSpec)
      val keyFactory = KeyFactory.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
      keyFactory.generatePublic(keySpec)
    }
  }
}
