// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.QualifiedName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

class RequestInclusiveFilterTest extends AnyWordSpec with BeforeAndAfterAll with BaseTest {
  "RequestInclusiveFilter" should {

    "filter contracts based on stakeholders and templates" in {
      val alice: LfPartyId = LfPartyId.assertFromString("alice")
      val bob: LfPartyId = LfPartyId.assertFromString("bob")
      val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
      val eve: LfPartyId = LfPartyId.assertFromString("eve")

      def mkTemplateId(tpl: String) = {
        val defaultPackageId: LfPackageId = LfPackageId.assertFromString("pkg")
        Ref.Identifier(defaultPackageId, QualifiedName.assertFromString(s"module:$tpl"))
      }

      val template1 = mkTemplateId("tpl1")
      val template2 = mkTemplateId("tpl2")
      val template3 = mkTemplateId("tpl3")

      val filters = Map(
        alice -> None, // No filter
        bob -> Some(NonEmpty.apply(Seq, template1)), // only template1
        charlie -> Some(NonEmpty.apply(Seq, template1, template2)), // template1 and template2
      )

      // Alice: any template is fine
      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(alice),
        templateId = template3,
      ) shouldBe true

      // Bob: only template1
      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(bob),
        templateId = template1,
      ) shouldBe true

      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(bob),
        templateId = template2,
      ) shouldBe false

      // Charlie: both template1 and template2
      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(charlie),
        templateId = template1,
      ) shouldBe true

      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(charlie),
        templateId = template2,
      ) shouldBe true

      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(charlie),
        templateId = template3,
      ) shouldBe false

      // Eve: mentioned nowhere in the filter
      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(eve),
        templateId = template1,
      ) shouldBe false

      // Multiple stakeholders
      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(bob, charlie),
        templateId = template2,
      ) shouldBe true

      RequestInclusiveFilter(filters).satisfyFilter(
        stakeholders = Set(bob, charlie),
        templateId = template3,
      ) shouldBe false
    }
  }
}
