// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.util.NoCopy
import com.google.protobuf.ByteString

import scala.util.Random

/** The class is a tag that denotes a byte string as a securely generated random value.
  *
  * Not an AnyVal as we also want it to be a serializable value such that we can encrypt it.
  */
case class SecureRandomness private[crypto] (unwrap: ByteString)
    extends HasCryptographicEvidence
    with NoCopy {
  override def getCryptographicEvidence: ByteString = unwrap
}

/** Cryptographically-secure randomness */
object SecureRandomness {

  private[crypto] def apply(unwrap: ByteString): SecureRandomness =
    new SecureRandomness(unwrap)

  private val rand = new java.security.SecureRandom()

  def randomBytes(length: Int): Array[Byte] = {
    val secretBytes = new Array[Byte](length)
    rand.nextBytes(secretBytes)
    secretBytes
  }

  def randomByteString(length: Int): ByteString = ByteString.copyFrom(randomBytes(length))

  def secureRandomness(length: Int): SecureRandomness = SecureRandomness(randomByteString(length))

  /** Recover secure randomness from a byte string. Use for deserialization only. Fails if the provided byte string
    * is not of the expected length.
    */
  def fromByteString(
      expectedLength: Int
  )(bytes: ByteString): Either[DeserializationError, SecureRandomness] = {
    if (bytes.size != expectedLength)
      Left(
        DeserializationError(
          s"Expected $expectedLength bytes of serialized randomness, got ${bytes.size}",
          bytes,
        )
      )
    else Right(new SecureRandomness(bytes))
  }
}

/** Pseudo randomness, MUST NOT be used for security-relevant operations. */
object PseudoRandom {

  private val rand = new Random(new java.security.SecureRandom())

  def randomAlphaNumericString(length: Int): String = rand.alphanumeric.take(length).mkString

  def randomUnsigned(maxValue: Int): Int = rand.between(0, maxValue)

}
