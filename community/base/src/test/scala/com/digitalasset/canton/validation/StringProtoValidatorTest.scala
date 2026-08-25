// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringProtoValidatorTest extends AnyWordSpec with EitherValues with Matchers {

  private val field = Some("f")
  private val bad = "a\u0000b" // NUL — rejected by the content check
  private val validator = ProtoValidator[String]

  "ProtoValidator[String] unconditional check" should {
    "accept clean content" in {
      validator.validate("ok", field).value shouldBe "ok"
    }

    "reject bad content" in {
      validator.validate(bad, field).left.value shouldBe a[StringConversionError]
    }
  }

  "ProtoValidator[String] validating protocol version" should {
    "reject bad content from that version on" in {
      validator
        .validate(bad, ProtocolVersion.stringValidation, field)
        .left
        .value shouldBe a[StringConversionError]
    }

    "pass bad content through below it (backwards compatibility)" in {
      validator.validate(bad, ProtocolVersion.v35, field).value shouldBe bad
    }
  }
}
