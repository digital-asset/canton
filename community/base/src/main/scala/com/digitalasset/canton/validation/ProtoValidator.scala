// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.either.*
import com.digitalasset.base.validation.StringValidator
import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion

/** Validates an untrusted value of type `A` at the deserialization boundary. When using the method
  * with `pv`, the instance can decide whether and how to perform the check depending on the `pv` in
  * order to guarantee backwards compatibility among nodes. Protocol-version dispatch lives in
  * [[ProtoValidation]].
  */
trait ProtoValidator[A] {

  /** The unconditional content check, run as-is for `AlwaysValidation`. */
  private[validation] def validateAlways(value: A, field: Option[String]): ParsingResult[A]

  /** The check that decides, according to the PV, whether to run [[validateAlways]] or not. This
    * enables backwards-compatibility by applying validation only starting from the PV that
    * introduces this validator.
    */
  private[validation] def validate(
      value: A,
      pv: ProtocolVersion,
      field: Option[String],
  ): ParsingResult[A]
}

object ProtoValidator {

  def apply[A](implicit validator: ProtoValidator[A]): ProtoValidator[A] = validator

  implicit val string: ProtoValidator[String] = new ProtoValidator[String] {
    override private[validation] def validateAlways(
        value: String,
        field: Option[String],
    ): ParsingResult[String] =
      StringValidator
        .validate(value)
        .bimap(v => StringConversionError(v.message, field), _ => value)

    override private[validation] def validate(
        value: String,
        pv: ProtocolVersion,
        field: Option[String],
    ): ParsingResult[String] =
      if (pv >= ProtocolVersion.v36) validateAlways(value, field) else Right(value)
  }
}
