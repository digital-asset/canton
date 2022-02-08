// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either._
import cats.syntax.foldable._
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** The expansion step of the HMAC-based key derivation function (HKDF) as defined in:
  * https://tools.ietf.org/html/rfc5869
  */
trait HkdfOps {
  this: HmacOps =>

  import HkdfError._

  /** Produce a new secret from the given secret and purpose
    *
    * @param keyMaterial Cryptographically secure, uniformly random initial key material. Must be at least as long as
    *                    the length of the hash function chosen for the HMAC scheme.
    * @param outputBytes The length of the produced secret. May be at most 255 times the size of the output of the
    *                    selected hashing algorithm. If you need to derive multiple keys, set the `info` parameter
    *                    to different values, for each key that you need.
    * @param info        Specify the purpose of the derived key (optional). Note that you can derive multiple
    *                    independent keys from the same key material by varying the purpose.
    * @param algorithm   The hash algorithm to be used for the HKDF construction
    * @return
    */
  def hkdfExpand(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] = {
    for {
      _ <- Either.cond(outputBytes >= 0, (), HkdfOutputNegative(outputBytes))
      hashBytes = algorithm.hashAlgorithm.length
      _ <- Either.cond[HkdfError, Unit](
        keyMaterial.unwrap.size >= hashBytes,
        (),
        HkdfKeyTooShort(length = keyMaterial.unwrap.size, needed = hashBytes),
      )
      prk <- HmacSecret.create(keyMaterial.unwrap).leftMap(HkdfHmacError)
      nrChunks = scala.math.ceil(outputBytes.toDouble / hashBytes).toInt
      _ <- Either
        .cond[HkdfError, Unit](
          nrChunks <= 255,
          (),
          HkdfOutputTooLong(length = outputBytes, maximum = hashBytes * 255),
        )
      outputAndLast <- (1 to nrChunks).toList
        .foldM(ByteString.EMPTY -> ByteString.EMPTY) { case ((out, last), chunk) =>
          val chunkByte = ByteString.copyFrom(Array[Byte](chunk.toByte))
          hmacWithSecret(prk, last.concat(info.bytes).concat(chunkByte), algorithm)
            .bimap(HkdfHmacError, hmac => out.concat(hmac.unwrap) -> hmac.unwrap)
        }
      (out, _last) = outputAndLast
    } yield SecureRandomness(out.substring(0, outputBytes))
  }

}

/** Ensures unique values of "info" HKDF parameter for the different usages of the KDF. E.g., we may have
  * one purpose for deriving the encryption key for a view from a random value, and another one for deriving the random
  * values used for the subviews.
  */
class HkdfInfo private (val bytes: ByteString) extends AnyVal

object HkdfInfo {

  /** Use when deriving a view encryption key from randomness */
  val ViewKey = new HkdfInfo(ByteString.copyFromUtf8("view-key"))

  /** Use when deriving subview-randomness from the randomness used for a view */
  def subview(position: MerklePathElement) =
    new HkdfInfo(ByteString.copyFromUtf8("subview-").concat(position.encodeDeterministically))

  /** Used to specify arbitrary randomness for golden tests. Don't use in production! */
  @VisibleForTesting
  def testOnly(bytes: ByteString) = new HkdfInfo(bytes)
}

sealed trait HkdfError extends Product with Serializable with PrettyPrinting

object HkdfError {

  case class HkdfOutputNegative(length: Int) extends HkdfError {
    override def pretty: Pretty[HkdfOutputNegative] = prettyOfClass(
      param("length", _.length)
    )
  }

  case class HkdfKeyTooShort(length: Int, needed: Long) extends HkdfError {
    override def pretty: Pretty[HkdfKeyTooShort] = prettyOfClass(
      param("length", _.length),
      param("needed", _.needed),
    )
  }

  case class HkdfOutputTooLong(length: Int, maximum: Long) extends HkdfError {
    override def pretty: Pretty[HkdfOutputTooLong] = prettyOfClass(
      param("length", _.length),
      param("maximum", _.maximum),
    )
  }

  case class HkdfHmacError(error: HmacError) extends HkdfError {
    override def pretty: Pretty[HkdfHmacError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

}
