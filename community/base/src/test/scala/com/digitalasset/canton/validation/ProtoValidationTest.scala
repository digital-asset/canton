// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, StringConversionError}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProtoValidationTest extends AnyWordSpec with EitherValues with Matchers {

  // every entry point requires the name; only the errors carry it as an Option
  private val fieldName = "f"
  private val field = Some(fieldName)
  private val bad = "a\u0000b" // NUL, rejected by the content check
  // Proto string fields arrive wrapped; wrap test inputs the same way.
  private def u(s: String): ProtoUnvalidatedString = ProtoUnvalidatedString(s)
  private def seq[E](es: E*): ProtoUnvalidatedSeq[E] = ProtoUnvalidatedSeq(es)
  private def seqStr(ss: String*): ProtoUnvalidatedSeq[ProtoUnvalidatedString] = seq(ss.map(u)*)
  // Each check has its own validating protocol version, so pin each fixture to that alias.
  private val pvvStrings = ProtocolVersionValidation.PV(ProtocolVersion.stringValidation)
  private val pvvBounds = ProtocolVersionValidation.PV(ProtocolVersion.boundsCheck)
  private val pvv35 = ProtocolVersionValidation.PV(ProtocolVersion.v35) // below both of them

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
        .validateLength(seqStr("a", "b"), fieldName, pvvBounds, maxLength = 2)
        .value shouldBe Seq(u("a"), u("b"))
    }

    "reject a collection longer than the bound" in {
      val err = ProtoValidation
        .validateLength(seqStr("a", "b", "c"), fieldName, pvvBounds, maxLength = 2)
        .left
        .value

      err shouldBe a[InvariantViolation]
      err.message should include("3 elements, exceeding the maximum of 2")
    }

    "bound only from the validating protocol version on" in {
      ProtoValidation
        .validateLength(seqStr("a", "b", "c"), fieldName, pvv35, maxLength = 1)
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "not bound a trusted NoValidation source" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.NoValidation,
          maxLength = 1,
        )
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "bound an AlwaysValidation read unconditionally" in {
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          maxLength = 3,
        )
        .value shouldBe Seq(u("a"), u("b"), u("c"))
      ProtoValidation
        .validateLength(
          seqStr("a", "b", "c"),
          fieldName,
          ProtocolVersionValidation.AlwaysValidation,
          maxLength = 2,
        )
        .left
        .value shouldBe a[InvariantViolation]
    }
  }

  "ProtoValidation.validateCondition" should {
    val error = InvariantViolation("my field", "BOOM")

    "return unit when the condition holds" in {
      ProtoValidation
        .validateCondition(pvvBounds, 3 > 2, error)
        .value shouldBe ()
    }

    "return the provided error when the condition does not hold" in {
      ProtoValidation
        .validateCondition(pvvBounds, 2 > 3, error)
        .left
        .value shouldBe error
    }
  }

  "ProtoValidation.validateLengthThen" should {
    "bound the collection, then parse every element" in {
      ProtoValidation
        .validateLengthThen(seq(1, 2), "f", pvvBounds, maxLength = 2)((i, _) => Right(i + 1))
        .value shouldBe Seq(2, 3)
    }

    "reject a collection longer than the bound without parsing" in {
      ProtoValidation
        .validateLengthThen(seq(1, 2, 3), "f", pvvBounds, maxLength = 2)((_, _) =>
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
        .validate(seqStr("ok", bad), fieldName, pvvBounds, maxLength = 1)
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
          .validate(seqStr("ok", "fine"), fieldName, pvvBounds, maxLength = 1)
          .left
          .value
      ) shouldBe field
    }

    "run both checks only from the validating protocol version on" in {
      ProtoValidation.validate(seqStr("ok", bad), fieldName, pvv35, maxLength = 1).value shouldBe
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
