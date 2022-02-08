// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class HexStringTest extends AnyWordSpec with BaseTest {
  "HexString" should {
    "correctly (de)serialize byte arrays" in {
      val bytes = new Array[Byte](32)
      scala.util.Random.nextBytes(bytes)
      val s = HexString.toHexString(bytes)
      val parsed = HexString.parse(s)
      parsed.value shouldBe bytes
    }

    "fail to deserialize gibberish hex arrays" in {
      val err1 = HexString.parse("0")
      val err2 = HexString.parse("blablablabla")
      err1 shouldBe None
      err2 shouldBe None
    }

  }
}
