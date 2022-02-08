// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.ProtocolVersion.canClientConnectToServer
import org.scalatest.wordspec.AnyWordSpec

class CantonVersionTest extends AnyWordSpec with BaseTest {
  private def v(rawVersion: String): ProtocolVersion = ProtocolVersion.create(rawVersion).value
  "CantonVersion" should {
    "parse version string if valid" in {
      ProtocolVersion.create("5.1.3").value shouldBe new ProtocolVersion(5, 1, 3, false)
      ProtocolVersion.create("1.43.3-SNAPSHOT").value shouldBe new ProtocolVersion(
        1,
        43,
        3,
        true,
        Some("SNAPSHOT"),
      )
      ProtocolVersion.create("1.43.3-rc").value shouldBe new ProtocolVersion(
        1,
        43,
        3,
        false,
        Some("rc"),
      )
      ProtocolVersion.create("1.43.3-rc9").value shouldBe new ProtocolVersion(
        1,
        43,
        3,
        false,
        Some("rc9"),
      )
      ProtocolVersion.create("1.1.1-SNAPSHT").value shouldBe new ProtocolVersion(
        1,
        1,
        1,
        false,
        Some("SNAPSHT"),
      )
      ProtocolVersion.create("1.1.1-rc10").value shouldBe new ProtocolVersion(
        1,
        1,
        1,
        false,
        Some("rc10"),
      )
      ProtocolVersion.create("1.1.1-rc0").value shouldBe new ProtocolVersion(
        1,
        1,
        1,
        false,
        Some("rc0"),
      )
      ProtocolVersion.create("1.1.1-").value shouldBe new ProtocolVersion(1, 1, 1, false, Some(""))
      ProtocolVersion.create("1.1.1-SNAPSHOT-rc").value shouldBe new ProtocolVersion(
        1,
        1,
        1,
        true,
        Some("SNAPSHOT-rc"),
      )

      ProtocolVersion.create("1").left.value shouldBe a[String]
      ProtocolVersion.create("1.0.-1").left.value shouldBe a[String]
      ProtocolVersion.create("1.1.10000").left.value shouldBe a[String]
      ProtocolVersion.create("foo.bar").left.value shouldBe a[String]
      ProtocolVersion.create("1.1.").left.value shouldBe a[String]
      ProtocolVersion.create("1.1.0snapshot").left.value shouldBe a[String]

    }

    "should prove that releases are correctly compared" in {

      v("1.2.3") == v("1.2.3") shouldBe true
      v("1.2.3") > v("1.2.0") shouldBe true
      v("1.2.3") > v("1.0.0") shouldBe true

      v("1.0.0-rc") == v("1.0.0") shouldBe false
      v("1.0.0-rc") == v("1.0.0-ia") shouldBe false
      v("1.0.0-a") < v("1.0.0-b") shouldBe true

      v("1.0.0") > v("1.0.0-b") shouldBe true
      v("1.0.0-1") < v("1.0.0-b") shouldBe true
      v("1.0.0-1") < v("1.0.0-2") shouldBe true
      v("1.0.0-a.b.c") < v("1.0.0-a.b.d") shouldBe true
      v("1.0.0-a.b.c.e") < v("1.0.0-a.b.d") shouldBe true

      // examples given in SemVer specification
      v("1.0.0-alpha") < v("1.0.0-alpha.1") shouldBe true
      v("1.0.0-alpha.1") < v("1.0.0-alpha.beta") shouldBe true
      v("1.0.0-alpha.beta") < v("1.0.0-beta") shouldBe true
      v("1.0.0-beta") < v("1.0.0-beta.2") shouldBe true
      v("1.0.0-beta.2") < v("1.0.0-beta.11") shouldBe true
      v("1.0.0-rc.1") < v("1.0.0") shouldBe true

      v("1.0.0-rc") > v("1.0.0-ia") shouldBe true
      v("1.0.0-rc-SNAPSHOT") == v("1.0.0-ia-SNAPSHOT") shouldBe false

      v("1.0.0-SNAPSHOT") < v("1.0.0") shouldBe true
      v("1.1.0-SNAPSHOT") > v("1.0.0") shouldBe true
      v("1.0.1-SNAPSHOT") > v("1.0.0") shouldBe true

    }

    "version check" should {
      "be successful for matching versions" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(v("1.2.3"), v("1.3.4")),
          server = v("1.3.4"),
          None,
        ) shouldBe Right(())
      }

      "fail with a nice message if incompatible" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(v("0.1.3")),
          server = v("0.2.0"),
          None,
        ).left.value shouldBe (VersionNotSupportedError(v("0.2.0"), Seq(v("0.1.3"))))
      }
    }
  }

}
