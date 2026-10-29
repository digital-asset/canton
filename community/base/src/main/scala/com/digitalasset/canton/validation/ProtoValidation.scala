// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString

object ProtoValidation {

  // TODO(#34856) Replace with per-field limits; PositiveInt.MaxValue makes the bound a no-op
  val MaxCollectionSize: ValidationParam[PositiveInt] =
    ValidationParam(ProtocolVersion.minimum -> PositiveInt.MaxValue)

  /** Validate `value` via its `ProtoValidator`.
    *
    * @param field
    *   the validated field, named in the error
    * @param pvv
    *   `PV(pv)` passes `pv` to the validator, which decides from it whether to run the check, so
    *   that a message from an older node is not rejected, `NoValidation` returns the value
    *   unchecked, `AlwaysValidation` runs the check unconditionally
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
    * comes from the transformer, and by the [[ProtoUnvalidatedString]] pretty instance.
    * Package-private so no `fromProto` can drop the name that `validate` requires.
    */
  private[validation] def validateNoField[A](
      value: ProtoUnvalidated[A],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[A] =
    validateOptionalField(value, None, pvv)

  /** @param field
    *   `None` only for [[validateNoField]]; every `fromProto` names the field it validates.
    */
  private def validateOptionalField[A](
      value: ProtoUnvalidated[A],
      field: Option[String],
      pvv: ProtocolVersionValidation,
  )(implicit validator: ProtoValidator[A]): ParsingResult[A] =
    pvv match {
      case ProtocolVersionValidation.PV(pv) => validator.validate(value.unvalidated, pv, field)
      case ProtocolVersionValidation.NoValidation => Right(value.unvalidated)
      case ProtocolVersionValidation.AlwaysValidation =>
        validator.validateAlways(value.unvalidated, field)
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

  /** Bound the number of elements of a repeated field and hand back its raw elements. A repeated
    * field with no bound lets a peer send an arbitrarily large message. Use this for a repeated
    * message field, whose elements validate themselves in their own `fromProto`. For
    * [[ProtoUnvalidated]] elements use `validate`, which also checks their content.
    *
    * @param field
    *   the bounded field, named in the error
    * @param pvv
    *   `PV(pv)` applies the bound listed for `pv`, and no bound at all if `pv` is older than every
    *   version listed, `NoValidation` applies no bound, `AlwaysValidation` applies
    *   [[ValidationParam.latest]]
    * @param maxLength
    *   the largest accepted element count, per protocol version
    */
  def validateLength[E](
      seq: ProtoUnvalidatedSeq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  ): ParsingResult[Seq[E]] =
    validateSeqLength(seq = seq.elements, field = field, pvv = pvv, maxLength = maxLength)

  /** `validateLength` for a plain `Seq`, whose elements are not wrapped in a
    * [[ProtoUnvalidatedSeq]]. Prefer `validateLength` when the field is a [[ProtoUnvalidatedSeq]].
    *
    * @param field
    *   the bounded field, named in the error
    * @param pvv
    *   `PV(pv)` applies the bound listed for `pv`, and no bound at all if `pv` is older than every
    *   version listed, `NoValidation` applies no bound, `AlwaysValidation` applies
    *   [[ValidationParam.latest]]
    * @param maxLength
    *   the largest accepted element count, per protocol version
    */
  def validateSeqLength[E](
      seq: Seq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  ): ParsingResult[Seq[E]] =
    valueForPvv(pvv, maxLength).fold[ParsingResult[Seq[E]]](Right(seq)) { max =>
      Either.cond(
        seq.sizeIs <= max.unwrap,
        seq,
        InvariantViolation(
          field,
          s"repeated field has ${seq.size} elements, exceeding the maximum of ${max.unwrap}",
        ),
      )
    }

  /** Bound the byte size of a `ByteString` field and return the raw bytes. An unbounded byte field
    * lets a peer send an arbitrarily large message payload.
    *
    * @param field
    *   the bounded field, named in the error
    * @param pvv
    *   `PV(pv)` applies the bound listed for `pv`, and no bound at all if `pv` is older than every
    *   version listed, `NoValidation` applies no bound, `AlwaysValidation` applies
    *   [[ValidationParam.latest]]
    * @param maxLength
    *   the largest accepted byte size, per protocol version
    */
  def validateLength(
      bytes: ByteString,
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  ): ParsingResult[ByteString] =
    validateCondition(
      pvv,
      maxLength,
    )(
      max => bytes.size() <= max.value,
      max =>
        InvariantViolation(
          field,
          s"Byte length for $field (${bytes.size()} bytes) exceeds maximum limit of ${max.value} bytes",
        ),
    ).map(_ => bytes)

  /** The value a check runs with, or `None` when it does not run: `PV(pv)` takes the value listed
    * for `pv`, `NoValidation` always skips, `AlwaysValidation` takes the value of the newest listed
    * version. `validateOptionalField` does the same for validators, which resolve the version
    * themselves.
    */
  private def valueForPvv[A](
      pvv: ProtocolVersionValidation,
      values: ValidationParam[A],
  ): Option[A] =
    pvv match {
      case ProtocolVersionValidation.PV(pv) => values.forProtocolVersion(pv)
      case ProtocolVersionValidation.NoValidation => None
      case ProtocolVersionValidation.AlwaysValidation => Some(values.latest)
    }

  /** Check a condition against a value that depends on the protocol version, for example a depth or
    * a size limit that a later version tightened. The condition is skipped on a version older than
    * every version listed in `parameter`. For the length of a repeated field, use `validateLength`.
    *
    * @param pvv
    *   `PV(pv)` uses the value listed for `pv`, `NoValidation` skips the check, `AlwaysValidation`
    *   uses the value of the newest listed version
    * @param param
    *   the parameter the condition reads, per protocol version
    * @param condition
    *   applied to that value, `false` means the message is rejected
    * @param mkError
    *   applied to that same value to build the error, naming the field itself. Only called when
    *   `condition` fails.
    */
  def validateCondition[A](
      pvv: ProtocolVersionValidation,
      param: ValidationParam[A],
  )(
      condition: A => Boolean,
      mkError: A => ProtoDeserializationError,
  ): ParsingResult[Unit] =
    valueForPvv(pvv, param).fold[ParsingResult[Unit]](Right(())) { value =>
      Either.cond(condition(value), (), mkError(value))
    }

  /** `validateLength`, then `parse` every element, for elements with no [[ProtoValidator]] of their
    * own, i.e. a repeated message or enum field.
    *
    * @param parse
    *   applied to each element with the field name, once the length check has passed
    */
  def validateLengthThen[E, B](
      seq: ProtoUnvalidatedSeq[E],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  )(parse: (E, String) => ParsingResult[B]): ParsingResult[Seq[B]] =
    validateLength(seq, field, pvv, maxLength).flatMap(_.traverse(parse(_, field)))

  /** `validateLength`, then the matching [[ProtoValidator]] on every element.
    *
    * @param maxLength
    *   bounds the element count only. Each element is then validated with the same `pvv`.
    */
  def validate[A](
      seq: ProtoUnvalidatedSeq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  )(implicit validator: ProtoValidator[A]): ParsingResult[Seq[A]] =
    validateLengthThen(seq, field, pvv, maxLength)((elem, elemField) =>
      validate(elem, elemField, pvv)
    )

  /** `validate` the collection, then `parse` every validated element, for example
    * `validateThen(msg.parties, field, pvv, maxParties)(PartyId.fromProtoPrimitive)`.
    */
  def validateThen[A, B](
      seq: ProtoUnvalidatedSeq[ProtoUnvalidated[A]],
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  )(parse: (A, String) => ParsingResult[B])(implicit
      validator: ProtoValidator[A]
  ): ParsingResult[Seq[B]] =
    validate(seq, field, pvv, maxLength).flatMap(_.traverse(parse(_, field)))

  /** `validateLength` of a ByteString, then `parse` it. */
  def validateLengthThen[B](
      bytes: ByteString,
      field: String,
      pvv: ProtocolVersionValidation,
      maxLength: ValidationParam[PositiveInt],
  )(parse: ByteString => ParsingResult[B]): ParsingResult[B] =
    validateLength(bytes, field, pvv, maxLength).flatMap(parse)
}
