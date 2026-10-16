// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValidationParamTest extends AnyWordSpec with Matchers {

  private def n(i: Int): NonNegativeInt = NonNegativeInt.tryCreate(i)

  "VersionMap.forProtocolVersion" should {
    "apply a single entry from its version on" in {
      val value = ValidationParam(ProtocolVersion.v36 -> n(10))

      value.forProtocolVersion(ProtocolVersion.v35) shouldBe None
      value.forProtocolVersion(ProtocolVersion.v36) shouldBe Some(n(10))
      value.forProtocolVersion(ProtocolVersion.v37) shouldBe Some(n(10))
    }

    "carry each entry forward until the next one" in {
      val value = ValidationParam(ProtocolVersion.v36 -> n(10), ProtocolVersion.v37 -> n(20))

      value.forProtocolVersion(ProtocolVersion.v36) shouldBe Some(n(10))
      value.forProtocolVersion(ProtocolVersion.v37) shouldBe Some(n(20))
      value.forProtocolVersion(ProtocolVersion.dev) shouldBe Some(n(20))
    }

    "leave a version below every entry without a value" in {
      ValidationParam(ProtocolVersion.v37 -> n(20))
        .forProtocolVersion(ProtocolVersion.v36) shouldBe None
    }

    "ignore the order the entries are given in" in {
      ValidationParam(ProtocolVersion.v37 -> n(20), ProtocolVersion.v36 -> n(10))
        .forProtocolVersion(ProtocolVersion.v36) shouldBe Some(n(10))
    }
  }

  "VersionMap listing the minimum version" should {
    "apply on every supported version, including the oldest" in {
      val value = ValidationParam(ProtocolVersion.minimum -> n(10))

      value.forProtocolVersion(ProtocolVersion.minimum) shouldBe Some(n(10))
      value.forProtocolVersion(ProtocolVersion.v36) shouldBe Some(n(10))
      value.forProtocolVersion(ProtocolVersion.dev) shouldBe Some(n(10))
    }
  }

  "VersionMap.latest" should {
    "be the value of the newest entry" in {
      ValidationParam(
        ProtocolVersion.v36 -> n(10),
        ProtocolVersion.v37 -> n(20),
      ).latest shouldBe n(20)
    }
  }

  "VersionMap" should {
    "reject two values for the same protocol version" in {
      an[IllegalArgumentException] should be thrownBy
        ValidationParam(ProtocolVersion.v36 -> n(10), ProtocolVersion.v36 -> n(20))
    }
  }
}
