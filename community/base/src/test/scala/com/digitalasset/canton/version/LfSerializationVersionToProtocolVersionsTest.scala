// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.protocol.LfSerializationVersion
import com.digitalasset.canton.version.LfSerializationVersionToProtocolVersions.{
  getMinimumSupportedProtocolVersion,
  maxSerializationVersionForProtocolVersion,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class LfSerializationVersionToProtocolVersionsTest extends AnyWordSpec with Matchers {

  val supportedSerializationVersions =
    List(LfSerializationVersion.V1, LfSerializationVersion.VDev)

  "DamlLFVersionToProtocolVersions" should {
    supportedSerializationVersions.foreach { version =>
      s"find the minimum protocol version for $version" in {
        assert(
          Try(
            getMinimumSupportedProtocolVersion(version)
          ).isSuccess,
          s"Add $version to damlLfVersionToProtocolVersions Map",
        )
      }
    }
    "find the maximum serialization version for a protocol version" in {
      maxSerializationVersionForProtocolVersion(
        ProtocolVersion.v34
      ) shouldBe LfSerializationVersion.V1
      maxSerializationVersionForProtocolVersion(
        ProtocolVersion.v35
      ) shouldBe LfSerializationVersion.V2
      maxSerializationVersionForProtocolVersion(
        ProtocolVersion.dev
      ) shouldBe LfSerializationVersion.VDev
    }
  }
}
