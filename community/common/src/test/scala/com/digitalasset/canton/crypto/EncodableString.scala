// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto
import java.nio.charset.StandardCharsets

import com.digitalasset.canton.serialization.{
  DeserializationError,
  DeterministicEncoding,
  HasCryptographicEvidence,
}
import com.google.protobuf.ByteString

final case class EncodableString(string: String) extends HasCryptographicEvidence {
  def encodeDeterministically: ByteString = DeterministicEncoding.encodeString(string)
  override def getCryptographicEvidence: ByteString = encodeDeterministically
}

object EncodableString {
  private def fromByteArray(bytes: Array[Byte]): Either[DeserializationError, EncodableString] = {
    // TODO(Andreas): Check length of array -> May return Left on invalid lengths
    Right(EncodableString(new String(bytes, 4, bytes.length - 4, StandardCharsets.UTF_8)))
  }

  def fromByteString(bytes: ByteString): Either[DeserializationError, EncodableString] =
    fromByteArray(bytes.toByteArray)
}
