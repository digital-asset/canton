// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, StringConversionError}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class ProtoValidationTest extends AnyWordSpec with EitherValues with Matchers {

  // every entry point requires the name; only the errors carry it as an Option
  private val fieldName = "f"
  private val field = Some(fieldName)
  private val bad = "a\u0000b" // NUL, rejected by the content check
  // Proto string fields arrive wrapped; wrap test inputs the same way.
  private def u(s: String): ProtoUnvalidatedString = ProtoUnvalidatedString(s)
  private def seq[E](es: E*): ProtoUnvalidatedSeq[E] = ProtoUnvalidatedSeq(es)
  private def seqStr(ss: String*): ProtoUnvalidatedSeq[ProtoUnvalidatedString] = seq(ss.map(u)*)
  // Each check has its own validating protocol version, so pin each fixture to that alias.
  private val pvvStrings = ProtocolVersionValidation.PV(ProtocolVersion.v36)
  private val pvvBounds = ProtocolVersionValidation.PV(ProtocolVersion.v36)
  private val pvv35 = ProtocolVersionValidation.PV(ProtocolVersion.v35) // below both of them
  // The common case for a protocol message: a bound that has held since the framework was
  // introduced, stated with the version it applies from.
  private def maxLength(n: Int): ValidationParam[PositiveInt] =
    ValidationParam(ProtocolVersion.v36 -> PositiveInt.tryCreate(n))

  /** The field an error blames, the caller's only pointer to what failed. */
  private def fieldOf(err: ProtoDeserializationError): Option[String] = err match {
    case StringConversionError(_, f) => f
    case InvariantViolation(f, _) => f
    case other => fail(s"expected an error carrying a field, got $other")
  }

  "ProtoValidation.validate" should {
    "validate only from the validating protocol version on" in {
      // We use the string validator instance, whose validating protocol version is stringValidation.
      ProtoValidation
        .validate(u(bad), fieldName, pvvStrings)
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(u(bad), fieldName, pvv35)
        .value shouldBe bad
    }

    "pass through a trusted NoValidation source unchecked" in {
      ProtoValidation
        .validate(u(bad), fieldName, ProtocolVersionValidation.NoValidation)
        .value shouldBe bad
    }

    "enforce with AlwaysValidation regardless of protocol version" in {
      ProtoValidation
        .validate(u(bad), fieldName, ProtocolVersionValidation.AlwaysValidation)
        .left
        .value shouldBe a[StringConversionError]
    }

    "validate an optional field" in {
      ProtoValidation
        .validate(Option(bad).map(u), fieldName, pvvStrings)
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(
          Option.empty[ProtoUnvalidatedString],
          fieldName,
          pvvStrings,
        )
        .value shouldBe None
    }

    "return the validated field name in the error" in {
      val err = ProtoValidation
        .validate(u(bad), fieldName, pvvStrings)
        .left
        .value

      err shouldBe a[StringConversionError]
      fieldOf(err) shouldBe field
    }

    "return the field name from the optional overload too" in {
      fieldOf(
        ProtoValidation.validate(Option(u(bad)), fieldName, pvvStrings).left.value
      ) shouldBe field
    }

    "still check the content on the no-field path, reporting no field name" in {
      fieldOf(
        ProtoValidation.validateNoField(u(bad), pvvStrings).left.value
      ) shouldBe None
    }
  }

  "ProtoValidation.validateLength" should {
    "return the raw elements when the collection is within the bound" in {
      ProtoValidation
        .validateLength(seqStr("a", "b"), fieldName, pvvBounds, maxLength(2))
        .value shouldBe Seq(u("a"), u("b"))
    }

    "reject a collection longer than the bound" in {
      val err = ProtoValidation
        .validateLength(seqStr("a", "b", "c"), fieldName, pvvBounds, maxLength(2))
        .left
        .value

      err shouldBe a[InvariantViolation]
      err.message should include("3 elements, exceeding the maximum of 2")
    }

    "bound only from the validating protocol version on" in {
      ProtoValidation
        .validateLength(seqStr("a", "b", "c"), fieldName, pvv35, maxLength(1))
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "not apply a bound introduced in a later protocol version" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          pvvBounds,
          ValidationParam(ProtocolVersion.v37 -> PositiveInt.one),
        )
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "apply a bound introduced in a later protocol version from that version on" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.PV(ProtocolVersion.v37),
          ValidationParam(ProtocolVersion.v37 -> PositiveInt.one),
        )
        .left
        .value shouldBe a[InvariantViolation]
    }

    "apply the bound of the newest entry at or below the protocol version" in {
      val tightenedInV37 = ValidationParam(
        ProtocolVersion.v36 -> PositiveInt.three,
        ProtocolVersion.v37 -> PositiveInt.one,
      )

      ProtoValidation
        .validateLength(seqStr("a", "b", "c"), fieldName, pvvBounds, tightenedInV37)
        .value shouldBe Seq(u("a"), u("b"), u("c"))

      val err = ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.PV(ProtocolVersion.v37),
          tightenedInV37,
        )
        .left
        .value

      err.message should include("exceeding the maximum of 1")
    }

    "not bound a trusted NoValidation source" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.NoValidation,
          maxLength(1),
        )
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "apply the newest bound on an AlwaysValidation read" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          maxLength(3),
        )
        .value shouldBe Seq(u("a"), u("b"), u("c"))
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          ValidationParam(
            ProtocolVersion.v36 -> PositiveInt.three,
            ProtocolVersion.v37 -> PositiveInt.two,
          ),
        )
        .left
        .value shouldBe a[InvariantViolation]
    }
  }

  "ProtoValidation.validateLength for ByteString" should {
    val validBytes = ByteString.copyFromUtf8("123") // 3 bytes
    val invalidBytes = ByteString.copyFromUtf8("1234") // 4 bytes

    "return the ByteString when the size is within the bound" in {
      ProtoValidation
        .validateLength(validBytes, fieldName, pvvBounds, maxLength(3))
        .value shouldBe validBytes
    }

    "reject a ByteString larger than the bound" in {
      val err = ProtoValidation
        .validateLength(invalidBytes, fieldName, pvvBounds, maxLength(3))
        .left
        .value

      err shouldBe a[ProtoDeserializationError.InvariantViolation]
      err.message should include("exceeds maximum limit of 3 bytes")
      err.asInstanceOf[ProtoDeserializationError.InvariantViolation].field shouldBe Some(fieldName)
    }

    "bound only from the validating protocol version on" in {
      ProtoValidation
        .validateLength(invalidBytes, fieldName, pvv35, maxLength(3))
        .value shouldBe invalidBytes
    }

    "not bound a trusted NoValidation source" in {
      ProtoValidation
        .validateLength(
          invalidBytes,
          fieldName,
          ProtocolVersionValidation.NoValidation,
          maxLength(3),
        )
        .value shouldBe invalidBytes
    }

    "bound an AlwaysValidation read unconditionally" in {
      ProtoValidation
        .validateLength(
          validBytes,
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          maxLength(3),
        )
        .value shouldBe validBytes

      ProtoValidation
        .validateLength(
          invalidBytes,
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          maxLength(3),
        )
        .left
        .value shouldBe a[ProtoDeserializationError.InvariantViolation]
    }
  }

  "ProtoValidation.validateCondition" should {
    val error = InvariantViolation("my field", "BOOM")

    "return unit when the condition holds" in {
      ProtoValidation
        .validateCondition(
          pvvBounds,
          ValidationParam(ProtocolVersion.v36 -> 2),
        )(value => 3 > value, _ => error)
        .value shouldBe ()
    }

    "return the provided error when the condition does not hold" in {
      ProtoValidation
        .validateCondition(
          pvvBounds,
          ValidationParam(ProtocolVersion.v36 -> 3),
        )(value => 2 > value, _ => error)
        .left
        .value shouldBe error
    }

    "not check a condition introduced in a later protocol version" in {
      ProtoValidation
        .validateCondition(
          pvvBounds,
          ValidationParam(ProtocolVersion.v37 -> 3),
        )(value => 2 > value, _ => error)
        .value shouldBe ()
    }

    "check a condition introduced in a later protocol version from that version on" in {
      ProtoValidation
        .validateCondition(
          ProtocolVersionValidation.PV(ProtocolVersion.v37),
          ValidationParam(ProtocolVersion.v37 -> 3),
        )(value => 2 > value, _ => error)
        .left
        .value shouldBe error
    }

    "check a condition introduced in a later protocol version on AlwaysValidation" in {
      ProtoValidation
        .validateCondition(
          ProtocolVersionValidation.AlwaysValidation,
          ValidationParam(ProtocolVersion.v37 -> 3),
        )(value => 2 > value, _ => error)
        .left
        .value shouldBe error
    }

    "read the limit the condition compares against off the protocol version" in {
      val tightenedInV37 = ValidationParam(
        ProtocolVersion.v36 -> 3,
        ProtocolVersion.v37 -> 1,
      )
      def check(pvv: ProtocolVersionValidation) =
        ProtoValidation.validateCondition(pvv, tightenedInV37)(
          max => 2 <= max,
          max => InvariantViolation(fieldName, s"exceeds the maximum of $max"),
        )

      check(pvvBounds).value shouldBe ()
      check(ProtocolVersionValidation.PV(ProtocolVersion.v37)).left.value.message should include(
        "exceeds the maximum of 1"
      )
      // below every entry the condition does not run, so an older peer stays compatible
      check(pvv35).value shouldBe ()
      // no version to pick by, so the newest limit applies
      check(ProtocolVersionValidation.AlwaysValidation).left.value shouldBe a[InvariantViolation]
      check(ProtocolVersionValidation.NoValidation).value shouldBe ()
    }
  }

  "ProtoValidation.validateLengthThen" should {
    "bound the collection, then parse every element" in {
      ProtoValidation
        .validateLengthThen(seq(1, 2), "f", pvvBounds, maxLength(2))((i, _) => Right(i + 1))
        .value shouldBe Seq(2, 3)
    }

    "reject a collection longer than the bound without parsing" in {
      ProtoValidation
        .validateLengthThen(seq(1, 2, 3), "f", pvvBounds, maxLength(2))((_, _) =>
          fail("parsed an element of an over-long collection")
        )
        .left
        .value shouldBe a[InvariantViolation]
    }
  }

  "ProtoValidation.validate for collections" should {
    "bound the collection and validate every element" in {
      ProtoValidation
        .validate(seqStr("ok", "fine"), fieldName, pvvBounds, ProtoValidation.MaxCollectionSize)
        .value shouldBe Seq("ok", "fine")
      ProtoValidation
        .validate(seqStr("ok", bad), fieldName, pvvBounds, ProtoValidation.MaxCollectionSize)
        .left
        .value shouldBe a[StringConversionError]
    }

    "check the bound before the elements" in {
      // `bad` would fail the content check, but the length is rejected first.
      ProtoValidation
        .validate(seqStr("ok", bad), fieldName, pvvBounds, maxLength(1))
        .left
        .value shouldBe a[InvariantViolation]
    }

    "name the field in both the content and the length error" in {
      fieldOf(
        ProtoValidation
          .validate(seqStr(bad), fieldName, pvvBounds, ProtoValidation.MaxCollectionSize)
          .left
          .value
      ) shouldBe field
      fieldOf(
        ProtoValidation
          .validate(seqStr("ok", "fine"), fieldName, pvvBounds, maxLength(1))
          .left
          .value
      ) shouldBe field
    }

    "run both checks only from the validating protocol version on" in {
      ProtoValidation.validate(seqStr("ok", bad), fieldName, pvv35, maxLength(1)).value shouldBe
        Seq("ok", bad)
    }
  }

  "ProtoValidation.validateThen for collections" should {
    "validate then parse every element with the field name" in {
      ProtoValidation
        .validateThen(seqStr("ok", "fine"), "f", pvvBounds, ProtoValidation.MaxCollectionSize)(
          (v, _) => Right(v.length)
        )
        .value shouldBe Seq(2, 4)
    }

    "fail the validation before parsing" in {
      ProtoValidation
        .validateThen(seqStr("ok", bad), "f", pvvBounds, ProtoValidation.MaxCollectionSize)(
          (v, _) => Right(v.length)
        )
        .left
        .value shouldBe a[StringConversionError]
    }
  }

  "the repeated-string transformation" should {
    "make a community-base proto collection readable only through ProtoValidation" in {
      // Guards the field_transformation in the root package.proto: `all` is a ProtoUnvalidatedSeq,
      // so it has no map of its own and must go through ProtoValidation to be read.
      val proto =
        v30.Stakeholders(
          all = Seq("alice", "bob").map(u),
          signatories = Seq.empty[ProtoUnvalidatedString],
        )

      proto.all shouldBe a[ProtoUnvalidatedSeq[?]]

      ProtoValidation
        .validate(proto.all, "all", pvvBounds, ProtoValidation.MaxCollectionSize)
        .value shouldBe Seq("alice", "bob")
    }
  }
}
