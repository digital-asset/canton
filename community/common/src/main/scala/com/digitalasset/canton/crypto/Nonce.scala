// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.util.{HexString, NoCopy}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

final case class Nonce private (private val bytes: ByteString)
    extends NoCopy
    with HasCryptographicEvidence {
  def toProtoPrimitive: ByteString = bytes

  override def getCryptographicEvidence: ByteString = bytes
}

object Nonce {
  // The (oracle) database backend can only handle Nonce's up to a length of 500 bytes
  // If you try to insert a longer Nonce, the database will throw an error - see documentation at `LengthLimitedString` for more details
  // If you want to save a Nonce larger than this, please consult the team
  val length: Int = 20

  private[this] def apply(bytes: ByteString): Nonce =
    throw new UnsupportedOperationException("Use the generate method instead")

  implicit val setNonceParameter: SetParameter[Nonce] =
    (nonce, pp) => pp.setString(HexString.toHexString(nonce.toProtoPrimitive))

  implicit val getNonceResult: GetResult[Nonce] = GetResult { r =>
    HexString
      .parseToByteString(r.nextString())
      .map(new Nonce(_))
      .getOrElse(throw new DbSerializationException(s"Could not deserialize nonce from db"))
  }

  def generate(): Nonce = new Nonce(SecureRandomness.randomByteString(length))

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[Nonce] =
    Either.cond(
      bytes.size() == length,
      new Nonce(bytes),
      CryptoDeserializationError(
        DeserializationError(s"Nonce of invalid length: ${bytes.size()}", bytes)
      ),
    )
}
