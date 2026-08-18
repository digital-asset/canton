// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

object ProtoValidation {

  // TODO(#34856) Replace with per-field limits; Int.MaxValue makes the bound check a no-op
  val MaxCollectionSize: Int = Int.MaxValue

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

  /** Bound a repeated field and hand back its raw elements; an unbounded length is itself
    * unvalidated input. The whole check for a repeated message field, whose elements validate
    * themselves in their own `fromProto`. For [[ProtoUnvalidated]] elements use `validate`, which
    * also checks their content.
    *
    * @param field
    *   the bounded field, named in the error
    * @param pvv
    *   `PV` gates the bound on the negotiated version (so older peers stay compatible),
    *   `NoValidation` leaves it unbounded, `AlwaysValidation` bounds it unconditionally
    * @param maxLength
    *   the largest accepted element count
    */
  def validateLength[E](
      values: Seq[E],
      field: Option[String],
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  ): ParsingResult[Seq[E]] = {
    def bounded: ParsingResult[Seq[E]] =
      Either.cond(
        values.sizeIs <= maxLength,
        values,
        InvariantViolation(
          field.getOrElse(""),
          s"repeated field has ${values.size} elements, exceeding the maximum of $maxLength",
        ),
      )

    pvv match {
      case ProtocolVersionValidation.PV(pv) =>
        if (pv > ProtocolVersion.v35) bounded else Right(values)
      case ProtocolVersionValidation.NoValidation => Right(values)
      case ProtocolVersionValidation.AlwaysValidation => bounded
    }
  }

  /** [[validateLength]], then `parse` every element, for elements with no [[ProtoValidator]] of
    * their own, i.e. a repeated message or enum field.
    *
    * @param parse
    *   applied to each element with the field name
    */
  def validateLengthThen[E, B](
      values: Seq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(parse: (E, String) => ParsingResult[B]): ParsingResult[Seq[B]] =
    validateLength(values, Some(field), pvv, maxLength).flatMap(_.traverse(parse(_, field)))

  /** [[validateLength]], then the matching [[ProtoValidator]] on every element. */
  def validate[A](
      values: Seq[ProtoUnvalidated[A]],
      field: Option[String],
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(implicit validator: ProtoValidator[A]): ParsingResult[Seq[A]] =
    validateLength(values, field, pvv, maxLength).flatMap(_.traverse(validate(_, field, pvv)))

  /** `validate`, then `parse` every element, e.g. `validateThen(msg.parties, "parties",
    * pvv)(PartyId.fromProtoPrimitive)`.
    */
  def validateThen[A, B](
      values: Seq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Seq[B]] =
    validate(values, Some(field), pvv, maxLength).flatMap(_.traverse(parse(_, field)))
}
