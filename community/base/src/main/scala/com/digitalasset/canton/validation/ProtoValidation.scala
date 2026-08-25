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

  /** Validate `value` via its `ProtoValidator`.
    *
    * @param field
    *   the validated field, named in the error; `validateNoField` is the only caller without one
    * @param pvv
    *   `PV` runs the check from the validating protocol version on, `NoValidation` passes through
    *   unchecked, `AlwaysValidation` runs it unconditionally
    */
  def validate[A](
      value: ProtoUnvalidated[A],
      field: String,
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[A] =
    validateOptionalField(value, Some(field), pvv)

  /** `validate` an optional field, validating the content when present. */
  def validate[A](
      value: Option[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[Option[A]] =
    value.traverse(validate(_, field, pvv))

  /** `validate` a value with no field to blame: needed by the Chimney bridge, whose failure path
    * comes from the transformer. Package-private so no `fromProto` can drop the name that
    * `validate` requires.
    */
  private[validation] def validateNoField[A](
      value: ProtoUnvalidated[A],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[A] =
    validateOptionalField(value, None, pvv)

  private def validateOptionalField[A](
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

  /** `validate`, then `parse`, e.g. `validateThen(msg.f, "f", pvv)(Xyz.fromProtoPrimitive)`.
    *
    * @param parse
    *   applied to the validated value with the field name
    */
  def validateThen[A, B](value: ProtoUnvalidated[A], field: String, pvv: ProtocolVersionValidation)(
      parse: (A, String) => ParsingResult[B]
  )(implicit validator: ProtoValidator[A]): ParsingResult[B] =
    validate(value, field, pvv).flatMap(parse(_, field))

  /** `validateThen` an optional field, validating and parsing the content when present. */
  def validateThen[A, B](
      value: Option[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Option[B]] =
    value.traverse(validateThen(_, field, pvv)(parse))

  /** Bound a repeated field and hand back its raw elements; an unbounded length is itself
    * unvalidated input. The whole check for a repeated message field, whose elements validate
    * themselves in their own `fromProto`. For [[ProtoUnvalidated]] elements use `validate`, which
    * also checks their content.
    *
    * @param field
    *   the bounded field, named in the error
    * @param pvv
    *   `PV` applies the bound from the validating protocol version on (so older peers stay
    *   compatible), `NoValidation` leaves it unbounded, `AlwaysValidation` bounds it
    *   unconditionally
    * @param maxLength
    *   the largest accepted element count
    */
  def validateLength[E](
      seq: ProtoUnvalidatedSeq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  ): ParsingResult[Seq[E]] = {
    def bounded: ParsingResult[Seq[E]] =
      Either.cond(
        seq.sizeIs <= maxLength,
        seq.elements,
        InvariantViolation(
          field,
          s"repeated field has ${seq.size} elements, exceeding the maximum of $maxLength",
        ),
      )

    pvv match {
      case ProtocolVersionValidation.PV(pv) =>
        if (pv >= ProtocolVersion.boundsCheck) bounded else Right(seq.elements)
      case ProtocolVersionValidation.NoValidation => Right(seq.elements)
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
      seq: ProtoUnvalidatedSeq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(parse: (E, String) => ParsingResult[B]): ParsingResult[Seq[B]] =
    validateLength(seq, field, pvv, maxLength).flatMap(_.traverse(parse(_, field)))

  /** [[validateLength]], then the matching [[ProtoValidator]] on every element. */
  def validate[A](
      seq: ProtoUnvalidatedSeq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(implicit validator: ProtoValidator[A]): ParsingResult[Seq[A]] =
    validateLength(seq, field, pvv, maxLength).flatMap(_.traverse(validate(_, field, pvv)))

  /** `validate`, then `parse` every element, e.g. `validateThen(msg.parties, "parties",
    * pvv)(PartyId.fromProtoPrimitive)`.
    */
  def validateThen[A, B](
      seq: ProtoUnvalidatedSeq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: Int,
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Seq[B]] =
    validate(seq, field, pvv, maxLength).flatMap(_.traverse(parse(_, field)))
}
