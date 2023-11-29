// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.ProtocolVersionCompatibility.canClientConnectToServer
import org.scalatest.wordspec.AnyWordSpec

class ProtocolVersionCompatibilityTest extends AnyWordSpec with BaseTest {
  "ProtocolVersionCompatibility" should {
    "version check" should {
      "be successful for matching versions" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(ProtocolVersion.v6, ProtocolVersion.dev),
          server = ProtocolVersion.dev,
          None,
        ) shouldBe Right(())
      }

      "fail with a nice message if incompatible" in {
        canClientConnectToServer(
          clientSupportedVersions = Seq(ProtocolVersion.v6),
          server = ProtocolVersion.dev,
          None,
        ).left.value shouldBe (VersionNotSupportedError(
          ProtocolVersion.dev,
          Seq(ProtocolVersion.v6),
        ))
      }
    }
  }
}
