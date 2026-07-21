// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.validation

import com.digitalasset.base.validation.StringViolation.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StringValidatorTest extends AnyWordSpec with Matchers with EitherValues {

  "validate" should {
    "accept a well-formed string, including paired surrogates" in {
      StringValidator.validate("validé😀").value shouldBe (())
    }

    "reject a NUL character" in {
      StringValidator.validate("a" + 0.toChar).left.value shouldBe a[NulCharacter]
    }

    "accept common whitespace control characters (tab, newline, CR)" in {
      StringValidator.validate("line1\nline2\tend\r").value shouldBe (())
    }

    "reject a non-whitespace control character (e.g. ESC) that can break a renderer" in {
      StringValidator.validate("esc" + 0x1b.toChar).left.value shouldBe a[ControlCharacter]
      StringValidator.validate("bell" + 0x07.toChar).left.value shouldBe a[ControlCharacter]
    }

    "reject an unpaired high surrogate" in {
      StringValidator.validate("\ud800foo").left.value shouldBe a[UnpairedHighSurrogate]
    }

    "reject an unpaired low surrogate" in {
      StringValidator.validate("\udc00bar").left.value shouldBe a[UnpairedLowSurrogate]
    }

    "not echo the offending content in the error message" in {
      StringValidator.validate("secret" + 0.toChar).left.value.message should not include "secret"
    }
  }
}
