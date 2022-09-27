// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class ProtocolVersionTest extends AnyWordSpec with BaseTest {
  "ProtocolVersion" should {
    "refuse release versions which are not protocol versions" in {
      ProtocolVersion.create("5.1.3").left.value shouldBe a[String]
      ProtocolVersion.create("5.1.0").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-SNAPSHOT").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-rc").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-rc9").left.value shouldBe a[String]
    }

    "parse version string if valid" in {
      // Old semver format
      ProtocolVersion.create("5.0.0").value shouldBe ProtocolVersion(5)
      ProtocolVersion.create("2.0.0").value shouldBe ProtocolVersion.v2

      // New format
      ProtocolVersion.create("2").value shouldBe ProtocolVersion.v2
      ProtocolVersion.create("0").value shouldBe ProtocolVersion(0)

      ProtocolVersion
        .create(Int.MaxValue.toString)
        .value shouldBe ProtocolVersion.dev

      ProtocolVersion
        .create(s"${Int.MaxValue.toString}.0.0")
        .value shouldBe ProtocolVersion.dev

      ProtocolVersion.create("DeV").value shouldBe ProtocolVersion.dev
    }

    "be comparable" in {
      ProtocolVersion.v2 < ProtocolVersion.v3 shouldBe true
      ProtocolVersion.v2 <= ProtocolVersion.v3 shouldBe true
      ProtocolVersion.v3 <= ProtocolVersion.v3 shouldBe true

      ProtocolVersion.v3 < ProtocolVersion.v2 shouldBe false
      ProtocolVersion.v3 <= ProtocolVersion.v2 shouldBe false

      ProtocolVersion.v3 <= ProtocolVersion.dev shouldBe true
      ProtocolVersion.v3 < ProtocolVersion.dev shouldBe true
      ProtocolVersion.dev <= ProtocolVersion.v3 shouldBe false

      ProtocolVersion.v3 == ProtocolVersion.v3 shouldBe true
      ProtocolVersion.v3 == ProtocolVersion.v2 shouldBe false
    }
  }
}
