// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.crypto.provider.jce
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import io.circe.Json

/** A JSON Web Key.
  *
  * JWKs are part of JOSE (JavaScript Object Signing and Encryption), a loose collection of RFCs
  * that define JWTs, JWKs, and more.
  *
  * JWKs themselves are defined in <https://datatracker.ietf.org/doc/html/rfc7517>.
  *
  * We can construct JWKs from Canton's SigningPublicKey. This allows us to integrate with modern
  * web apps.
  *
  * @param kid
  *   The key ID, in canton this will always be the standard JSON Web Key Thumbprint as defined by
  *   RFC 7638
  * @param kty
  *   The key type, for example "EC" or "OKP"
  * @param alg
  *   The signing algorithm, for example "ES256" or "EdDSA"
  * @param additionalAttributes
  *   Any additional attributes such as "crv" or "x"
  */
final case class Jwk(
    kid: String,
    kty: Jwk.Kty,
    alg: Jwk.Alg,
    additionalAttributes: Map[String, Json],
) {
  val use = "sig"

  def toJsonObject: Map[String, Json] = additionalAttributes ++ Map(
    "kty" -> Json.fromString(kty.value),
    "alg" -> Json.fromString(alg.value),
    "kid" -> Json.fromString(kid),
    "use" -> Json.fromString(use),
  )
}

object Jwk {
  sealed trait Kty {

    /** The JSON representation for this key type. */
    def value: String
  }

  object Kty {
    case object EC extends Kty { override val value = "EC" }

    /** Octet Key Pair, used for Edwards curves. */
    case object OKP extends Kty { override val value = "OKP" }
  }

  sealed trait Alg {

    /** The JSON representation for this algorithm. */
    def value: String
  }

  object Alg {
    case object ES256 extends Alg { override val value = "ES256" }

    case object ES384 extends Alg { override val value = "ES384" }

    case object EdDSA extends Alg { override val value = "EdDSA" }

    case object ES256K extends Alg { override val value = "ES256K" }
  }
}

trait JwksOps {
  def toJwk(publicKey: SigningPublicKey): Either[JwksError, Jwk]
}

sealed trait JwksError extends Product with Serializable with PrettyPrinting

object JwksError {
  final case class GeneralError(error: Throwable) extends JwksError {
    override protected def pretty: Pretty[GeneralError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class JceJavaKeyConversionError(error: jce.JceJavaKeyConversionError)
      extends JwksError {
    override protected def pretty: Pretty[JceJavaKeyConversionError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class UnsupportedKeySpec(
      keySpec: SigningKeySpec
  ) extends JwksError {
    override protected def pretty: Pretty[UnsupportedKeySpec] =
      prettyOfClass(param("keySpec", _.keySpec))
  }

  final case class KeyParameterExtractionError(message: String) extends JwksError {
    override protected def pretty: Pretty[KeyParameterExtractionError] =
      prettyOfClass(unnamedParam(_.message.singleQuoted))
  }

}
