// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.google.protobuf.ByteString
import scalapb.TypeMapper

import scala.language.implicitConversions

final class ProtoUnvalidatedByteString(private val byteString: ByteString)
    extends AnyVal
    with ProtoUnvalidated[ByteString] {
  override private[validation] def unvalidated: ByteString = byteString
}

object ProtoUnvalidatedByteString {
  def apply(byteString: ByteString): ProtoUnvalidatedByteString = new ProtoUnvalidatedByteString(
    byteString
  )
  implicit val typeMapper: TypeMapper[ByteString, ProtoUnvalidatedByteString] =
    TypeMapper(new ProtoUnvalidatedByteString(_))(_.byteString)

  implicit def fromByteString(byteString: ByteString): ProtoUnvalidatedByteString = apply(
    byteString
  )
}
