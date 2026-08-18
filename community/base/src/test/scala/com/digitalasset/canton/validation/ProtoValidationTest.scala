// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, StringConversionError}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProtoValidationTest extends AnyWordSpec with EitherValues with Matchers {

  private val field = Some("f")
  private val bad = "a\u0000b" // NUL, rejected by the content check
  // Proto string fields arrive wrapped; wrap test inputs the same way.
  private def u(s: String): ProtoUnvalidatedString = ProtoUnvalidatedString(s)
  private def seq(ss: String*): Seq[ProtoUnvalidatedString] = ss.map(u)
  private val pvv36 = ProtocolVersionValidation.PV(ProtocolVersion.v36)
  private val pvv35 = ProtocolVersionValidation.PV(ProtocolVersion.v35)

  "ProtoValidation.validate" should {
    "gate a validation on a protocol version" in {
      // We use the string validator instance in this test. String validation is only enabled in pv36 or later
      ProtoValidation
        .validate(u(bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(u(bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v35))
        .value shouldBe bad
    }

    "pass through a trusted NoValidation source unchecked" in {
      ProtoValidation
        .validate(u(bad), field, ProtocolVersionValidation.NoValidation)
        .value shouldBe bad
    }

    "enforce with AlwaysValidation regardless of protocol version" in {
      ProtoValidation
        .validate(u(bad), field, ProtocolVersionValidation.AlwaysValidation)
        .left
        .value shouldBe a[StringConversionError]
    }

    "validate an optional field" in {
      ProtoValidation
        .validate(Option(bad).map(u), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(
          Option.empty[ProtoUnvalidatedString],
          field,
          ProtocolVersionValidation.PV(ProtocolVersion.v36),
        )
        .value shouldBe None
    }

    "validate every element of a repeated field" in {
      ProtoValidation
        .validate(Seq("ok", bad).map(u), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(
          Seq("ok", "fine").map(u),
          field,
          ProtocolVersionValidation.PV(ProtocolVersion.v36),
        )
        .value shouldBe Seq("ok", "fine")
    }

    "return the validated field name in the error" in {
      val err = ProtoValidation
        .validate(u(bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value

      err shouldBe a[StringConversionError]
      err.asInstanceOf[StringConversionError].field shouldBe field
    }
  }

  "ProtoValidation.validateLength" should {
    "return the raw elements when the collection is within the bound" in {
      ProtoValidation
        .validateLength(seq("a", "b"), field, pvv36, maxLength = 2)
        .value shouldBe Seq(u("a"), u("b"))
    }

    "reject a collection longer than the bound" in {
      val err = ProtoValidation
        .validateLength(seq("a", "b", "c"), field, pvv36, maxLength = 2)
        .left
        .value

      err shouldBe a[InvariantViolation]
      err.message should include("3 elements, exceeding the maximum of 2")
    }

    "gate the bound on the protocol version" in {
      ProtoValidation
        .validateLength(seq("a", "b", "c"), field, pvv35, maxLength = 1)
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "not bound a trusted NoValidation source" in {
      ProtoValidation
        .validateLength(seq("a", "b", "c"), field, ProtocolVersionValidation.NoValidation, 1)
        .value shouldBe Seq(u("a"), u("b"), u("c"))
    }

    "bound an AlwaysValidation read unconditionally" in {
      ProtoValidation
        .validateLength(seq("a", "b", "c"), field, ProtocolVersionValidation.AlwaysValidation, 3)
        .value shouldBe Seq(u("a"), u("b"), u("c"))
      ProtoValidation
        .validateLength(seq("a", "b", "c"), field, ProtocolVersionValidation.AlwaysValidation, 2)
        .left
        .value shouldBe a[InvariantViolation]
    }
  }

  "ProtoValidation.validateLengthThen" should {
    "bound the collection, then parse every element" in {
      ProtoValidation
        .validateLengthThen(Seq(1, 2), "f", pvv36, maxLength = 2)((i, _) => Right(i + 1))
        .value shouldBe Seq(2, 3)
    }

    "reject a collection longer than the bound without parsing" in {
      ProtoValidation
        .validateLengthThen(Seq(1, 2, 3), "f", pvv36, maxLength = 2)((_, _) =>
          fail("parsed an element of an over-long collection")
        )
        .left
        .value shouldBe a[InvariantViolation]
    }
  }

  "ProtoValidation.validate for collections" should {
    "bound the collection and validate every element" in {
      ProtoValidation
        .validate(seq("ok", "fine"), field, pvv36, ProtoValidation.MaxCollectionSize)
        .value shouldBe Seq("ok", "fine")
      ProtoValidation
        .validate(seq("ok", bad), field, pvv36, ProtoValidation.MaxCollectionSize)
        .left
        .value shouldBe a[StringConversionError]
    }

    "check the bound before the elements" in {
      // `bad` would fail the content check, but the length is rejected first.
      ProtoValidation
        .validate(seq("ok", bad), field, pvv36, maxLength = 1)
        .left
        .value shouldBe a[InvariantViolation]
    }

    "gate both checks on the protocol version" in {
      ProtoValidation.validate(seq("ok", bad), field, pvv35, maxLength = 1).value shouldBe
        Seq("ok", bad)
    }
  }

  "ProtoValidation.validateThen for collections" should {
    "validate then parse every element with the field name" in {
      ProtoValidation
        .validateThen(seq("ok", "fine"), "f", pvv36, ProtoValidation.MaxCollectionSize)((v, _) =>
          Right(v.length)
        )
        .value shouldBe Seq(2, 4)
    }

    "fail the validation before parsing" in {
      ProtoValidation
        .validateThen(seq("ok", bad), "f", pvv36, ProtoValidation.MaxCollectionSize)((v, _) =>
          Right(v.length)
        )
        .left
        .value shouldBe a[StringConversionError]
    }
  }

}
