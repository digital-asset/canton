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

  "ProtoValidator.option" should {
    val validator = ProtoValidator[Option[String]]

    "validate the content when present" in {
      validator.validate(Some("ok"), ProtocolVersion.v36, field).value shouldBe Some("ok")
      validator.validate(Some(bad), ProtocolVersion.v36, field).left.value shouldBe a[
        StringConversionError
      ]
    }

    "accept an empty option" in {
      validator.validate(None, ProtocolVersion.v36, field).value shouldBe None
    }
  }

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

    "validate an optional field" in {
      ProtoValidation
        .validate(Option(bad), field, ProtocolVersionValidation.PV(ProtocolVersion.v36))
        .left
        .value shouldBe a[StringConversionError]
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
}
