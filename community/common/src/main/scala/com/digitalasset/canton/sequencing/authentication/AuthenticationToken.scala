// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.syntax.either._
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.util.{HexString, NoCopy}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

case class AuthenticationToken private (private val bytes: ByteString)
    extends NoCopy
    with HasCryptographicEvidence {
  def toProtoPrimitive: ByteString = bytes

  override def getCryptographicEvidence: ByteString = bytes
}

object AuthenticationToken {
  // The (oracle) database backend can only handle AuthenticationToken's up to a length of 500 bytes
  // If you try to insert a longer Nonce, the database will throw an error - see documentation at `LengthLimitedString` for more details
  // If you want to save a AuthenticationToken larger than this, please consult the team
  val length: Int = 20

  private[this] def apply(bytes: ByteString): AuthenticationToken =
    throw new UnsupportedOperationException("Use the generate methods instead")

  def generate(): AuthenticationToken = {
    new AuthenticationToken(SecureRandomness.randomByteString(length))
  }

  def fromProtoPrimitive(bytes: ByteString): Either[DeserializationError, AuthenticationToken] =
    Either.cond(
      bytes.size() == length,
      new AuthenticationToken(bytes),
      DeserializationError(s"Authentication token of wrong size: ${bytes.size()}", bytes),
    )

  def tryFromProtoPrimitive(bytes: ByteString): AuthenticationToken =
    fromProtoPrimitive(bytes).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid authentication token: $err")
    )

  implicit val setAuthenticationTokenParameter: SetParameter[AuthenticationToken] =
    (token, pp) => pp.setString(HexString.toHexString(token.toProtoPrimitive))

  implicit val getAuthenticationTokenResult: GetResult[AuthenticationToken] = GetResult { r =>
    HexString
      .parseToByteString(r.nextString())
      .map(new AuthenticationToken(_))
      .getOrElse(
        throw new DbSerializationException(s"Could not deserialize authentication token from db")
      )
  }
}
