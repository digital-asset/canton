// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import com.digitalasset.canton.BaseTest
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
  }
}
