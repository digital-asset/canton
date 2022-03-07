// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.authentication

import cats.syntax.either._
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.String300
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbSerializationException}
import com.digitalasset.canton.util.{HexString, NoCopy}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

case class AuthenticationToken private (private val bytes: ByteString)
    extends NoCopy
    with HasCryptographicEvidence {
  def toProtoPrimitive: ByteString = bytes

  def toLengthLimitedHexString: String300 =
    // Authentication tokens have at most 150 bytes
    checked(String300.tryCreate(HexString.toHexString(this.toProtoPrimitive)))

  override def getCryptographicEvidence: ByteString = bytes
}

object AuthenticationToken {

  /** As of now, the database schemas can only handle authentication tokens up to a length of 150 bytes. Thus the length of an [[AuthenticationToken]] should never exceed that.
    * If we ever want to create an [[AuthenticationToken]] larger than that, we can increase it up to 500 bytes after which we are limited by Oracle length limits.
    * See the documentation at [[com.digitalasset.canton.config.RequireTypes.LengthLimitedString]] for more details.
    */
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
    (token, pp) => pp >> token.toLengthLimitedHexString

  implicit val getAuthenticationTokenResult: GetResult[AuthenticationToken] = GetResult { r =>
    val hexString = r.nextString()
    if (hexString.length > String300.maxLength)
      throw new DbDeserializationException(
        s"Base16-encoded authentication token of length ${hexString.length} exceeds allowed limit of ${String300.maxLength}."
      )
    HexString
      .parseToByteString(hexString)
      .map(new AuthenticationToken(_))
      .getOrElse(
        throw new DbSerializationException(s"Could not deserialize authentication token from db")
      )
  }
}
