// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.QualifiedName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.{BaseTest, LfPackageId, LfPartyId}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

class StateApiServiceTest extends AnyWordSpec with BeforeAndAfterAll with BaseTest {
  "StateApiService" should {
    "validate validAt" in {
      val ledgerBegin = 10L
      val ledgerEnd = 20L

      StateApiServiceImpl
        .validateValidAt(validAt = ledgerBegin - 1, Some(ledgerBegin), ledgerEnd)
        .left
        .value shouldBe "validAt should not be smaller than ledger begin"

      StateApiServiceImpl
        .validateValidAt(validAt = ledgerEnd + 1, Some(ledgerBegin), ledgerEnd)
        .left
        .value shouldBe "validAt should not be greater than ledger end"

      StateApiServiceImpl
        .validateValidAt(validAt = ledgerEnd, Some(ledgerBegin), ledgerEnd)
        .value shouldBe ()

      StateApiServiceImpl
        .validateValidAt(validAt = ledgerBegin, Some(ledgerBegin), ledgerEnd)
        .value shouldBe ()

      StateApiServiceImpl
        .validateValidAt(validAt = 15, Some(ledgerBegin), ledgerEnd)
        .value shouldBe ()
    }

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
      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(alice),
        templateId = template3,
      ) shouldBe true

      // Bob: only template1
      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(bob),
        templateId = template1,
      ) shouldBe true

      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(bob),
        templateId = template2,
      ) shouldBe false

      // Charlie: both template1 and template2
      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(charlie),
        templateId = template1,
      ) shouldBe true

      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(charlie),
        templateId = template2,
      ) shouldBe true

      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(charlie),
        templateId = template3,
      ) shouldBe false

      // Eve: mentioned nowhere in the filter
      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(eve),
        templateId = template1,
      ) shouldBe false

      // Multiple stakeholders
      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(bob, charlie),
        templateId = template2,
      ) shouldBe true

      StateApiServiceImpl.contractFilter(filters)(
        stakeholders = Set(bob, charlie),
        templateId = template3,
      ) shouldBe false
    }
  }
}
