// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.v2.value.Identifier
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.examples.java.iou.Iou
import org.scalatest.wordspec.AnyWordSpec

class TemplateIdTest extends AnyWordSpec with BaseTest {
  "TemplateId" should {
    "round-trip a generated Java identifier" in {
      val identifier = Iou.TEMPLATE_ID_WITH_PACKAGE_ID
      TemplateId.fromJavaIdentifier(identifier).toJavaIdentifier shouldBe identifier
    }

    "round-trip a Scala identifier" in {
      val identifier = Identifier("p1", "m1", "e1")
      TemplateId.fromIdentifier(identifier).toIdentifier shouldBe identifier
    }
  }
}
