// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.ProtoDeserializationError.StringConversionError
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.Tables.Table
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, Inspectors}

final class StringProtoValidatorTest
    extends AnyWordSpec
    with EitherValues
    with Matchers
    with Inspectors {

  private val field = Some("f")
  private val bad = "a\u0000b" // NUL — rejected by the content check
  private val validator = ProtoValidator[String]

  "ProtoValidator[String] unconditional check" should {
    "accept clean content" in {
      validator.validateAlways("ok", field).value shouldBe "ok"
    }

    "reject bad content" in {
      validator.validateAlways(bad, field).left.value shouldBe a[StringConversionError]
    }
  }

  "ProtoValidator[String] validating protocol version" should {
    "reject bad content" in {
      forAll(Table("protocol version", ProtocolVersion.supported.forgetNE*)) { pv =>
        validator
          .validate(bad, pv, field)
          .left
          .value shouldBe a[StringConversionError]
      }
    }
  }
}
