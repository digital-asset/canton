// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.LfInterfaceId
import com.digitalasset.canton.ProtoDeserializationError.ValueDeserializationError

object InterfaceIdSyntax {
  implicit class LfInterfaceIdSyntax(private val interfaceId: LfInterfaceId) extends AnyVal {
    def toProtoPrimitive: String = interfaceId.toString()
  }

  def fromProtoPrimitive(
      interfaceIdP: String
  ): Either[ValueDeserializationError, LfInterfaceId] = LfInterfaceId
    .fromString(interfaceIdP)
    .leftMap(err => ValueDeserializationError("interface", err))
}
