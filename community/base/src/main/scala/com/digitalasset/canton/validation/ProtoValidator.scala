// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.either.*
import com.digitalasset.base.validation.StringValidator
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

/** Validates an untrusted value of type `A` at the deserialization boundary. Checks are
  * protocol-version-gated to roll out stricter checks only with new protocol versions.
  */
trait ProtoValidator[A] {
  def validate(value: A, protocolVersion: ProtocolVersion, field: Option[String]): ParsingResult[A]
}

object ProtoValidator {

  def apply[A](implicit validator: ProtoValidator[A]): ProtoValidator[A] = validator

  implicit val string: ProtoValidator[String] = (value, protocolVersion, field) =>
    if (protocolVersion > ProtocolVersion.v35)
      StringValidator
        .validate(value)
        .bimap(v => StringConversionError(v.message, field), _ => value)
    else Right(value)

  implicit def option[A](implicit validator: ProtoValidator[A]): ProtoValidator[Option[A]] =
    (value, protocolVersion, field) =>
      value match {
        case Some(a) => validator.validate(a, protocolVersion, field).map(Some(_))
        case None => Right(None)
      }
}
