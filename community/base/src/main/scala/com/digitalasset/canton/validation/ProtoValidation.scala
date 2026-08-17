// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.traverse.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersionValidation

object ProtoValidation {

  /** Validate `value` via its `ProtoValidator`, dispatching on the `ProtocolVersionValidation`:
    * `PV` gates the check on the negotiated version, `NoValidation` passes through unchecked, and
    * `AlwaysValidation` runs the check unconditionally.
    */
  def validate[A](
      value: ProtoUnvalidated[A],
      field: Option[String],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[A] =
    pvv match {
      case ProtocolVersionValidation.PV(pv) => validator.validate(value.unvalidated, pv, field)
      case ProtocolVersionValidation.NoValidation => Right(value.unvalidated)
      case ProtocolVersionValidation.AlwaysValidation =>
        validator.validate(value.unvalidated, field)
    }

  /** `validate` an optional field, validating the content when present. */
  def validate[A](
      value: Option[ProtoUnvalidated[A]],
      field: Option[String],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[Option[A]] =
    value.traverse(validate(_, field, pvv))

  /** `validate` every element of a repeated field (fails on the first invalid element). */
  def validate[A](
      values: Seq[ProtoUnvalidated[A]],
      field: Option[String],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[Seq[A]] =
    values.traverse(validate(_, field, pvv))

  /** `validate`, then `parse` the value with its field name — e.g. `validateThen(msg.f, "f",
    * pvv)(Xyz.fromProtoPrimitive)`.
    */
  def validateThen[A, B](value: ProtoUnvalidated[A], field: String, pvv: ProtocolVersionValidation)(
      parse: (A, String) => ParsingResult[B]
  )(implicit validator: ProtoValidator[A]): ParsingResult[B] =
    validate(value, Some(field), pvv).flatMap(parse(_, field))

  /** `validateThen` an optional field, validating and parsing the content when present. */
  def validateThen[A, B](
      value: Option[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Option[B]] =
    value.traverse(validateThen(_, field, pvv)(parse))

  /** `validateThen` every element of a repeated field (fails on the first invalid element). */
  def validateThen[A, B](
      values: Seq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Seq[B]] =
    values.traverse(validateThen(_, field, pvv)(parse))
}
