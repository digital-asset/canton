// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.config.RequireTypes.String300
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbSerializationException}
import com.digitalasset.canton.util.{HexString, NoCopy}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

final case class Nonce private (private val bytes: ByteString)
    extends NoCopy
    with HasCryptographicEvidence {
  def toProtoPrimitive: ByteString = bytes
  def toLengthLimitedHexString: String300 =
    String300.tryCreate(HexString.toHexString(this.toProtoPrimitive))

  override def getCryptographicEvidence: ByteString = bytes
}

object Nonce {

  /** As of now, the database schemas can only handle nonces up to a length of 150 bytes. Thus the length of a [[Nonce]] should never exceed that.
    * If we ever want to create a [[Nonce]] larger than that, we can increase it up to 500 bytes after which we are limited by Oracle length limits.
    * See the documentation at [[com.digitalasset.canton.config.RequireTypes.LengthLimitedString]] for more details.
    */
  val length: Int = 20

  private[this] def apply(bytes: ByteString): Nonce =
    throw new UnsupportedOperationException("Use the generate method instead")

  implicit val setNonceParameter: SetParameter[Nonce] =
    (nonce, pp) => pp >> nonce.toLengthLimitedHexString

  implicit val getNonceResult: GetResult[Nonce] = GetResult { r =>
    val hexString = r.nextString()
    if (hexString.length > String300.maxLength)
      throw new DbDeserializationException(
        s"Base16-encoded authentication token of length ${hexString.length} exceeds allowed limit of ${String300.maxLength}."
      )
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
