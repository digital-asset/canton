// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.either.*
import com.digitalasset.base.validation.StringValidator
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

/** Validates an untrusted value of type `A` at the deserialization boundary. The content check runs
  * from the validating protocol version on, so older peers stay compatible. Protocol-version
  * dispatch lives in [[ProtoValidation]]; this trait only owns the check and the version it runs
  * from.
  */
trait ProtoValidator[A] {

  /** The unconditional content check. */
  private[validation] def validate(value: A, field: Option[String]): ParsingResult[A]

  /** Run [[validate]] only from the validating protocol version on; older versions pass through
    * unchecked.
    */
  private[validation] def validate(
      value: A,
      pv: ProtocolVersion,
      field: Option[String],
  ): ParsingResult[A] =
    if (pv >= ProtocolVersion.stringValidation) validate(value, field) else Right(value)
}

object ProtoValidator {

  def apply[A](implicit validator: ProtoValidator[A]): ProtoValidator[A] = validator

  implicit val string: ProtoValidator[String] = (value: String, field: Option[String]) =>
    StringValidator
      .validate(value)
      .bimap(v => StringConversionError(v.message, field), _ => value)
}
