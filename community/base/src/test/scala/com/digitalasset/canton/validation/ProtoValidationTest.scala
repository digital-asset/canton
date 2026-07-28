// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProtoValidationTest extends AnyWordSpec with EitherValues with Matchers {

  private val field = Some("f")
  private val bad = "a\u0000b" // NUL — rejected by the content check

  "ProtoValidation.validate" should {
    "gate a validation on a protocol version" in {
      // We use the string validator instance in this test. String validation is only enabled in pv36 or later
      ProtoValidation
        .validate(bad, field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(bad, field, ProtocolVersionValidation.PV(ProtocolVersion.v35))
        .value shouldBe bad
    }

    "pass through a trusted NoValidation source unchecked" in {
      ProtoValidation
        .validate(bad, field, ProtocolVersionValidation.NoValidation)
        .value shouldBe bad
    }

    "enforce with AlwaysValidation regardless of protocol version" in {
      ProtoValidation
        .validate(bad, field, ProtocolVersionValidation.AlwaysValidation)
        .left
        .value shouldBe a[StringConversionError]
    }

    "validate an optional field" in {
      ProtoValidation
        .validate(Option(bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(Option.empty[String], field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .value shouldBe None
    }

    "validate every element of a repeated field" in {
      ProtoValidation
        .validate(Seq("ok", bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
      ProtoValidation
        .validate(Seq("ok", "fine"), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .value shouldBe Seq("ok", "fine")
    }

    "return the validated field name in the error" in {
      val err = ProtoValidation
        .validate(bad, field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value

      err shouldBe a[StringConversionError]
      err.asInstanceOf[StringConversionError].field shouldBe field
    }
  }

  "ProtoValidation.validateThen" should {
    "validate then parse with the field name" in {
      ProtoValidation
        .validateThen("ok", "f", ProtocolVersionValidation.PV(ProtocolVersion.v36))((v, _) =>
          Right(v.length)
        )
        .value shouldBe 2
    }

    "fail the validation before parsing" in {
      ProtoValidation
        .validateThen(bad, "f", ProtocolVersionValidation.PV(ProtocolVersion.v36))((v, _) =>
          Right(v.length)
        )
        .left
        .value shouldBe a[StringConversionError]
    }
  }
}
