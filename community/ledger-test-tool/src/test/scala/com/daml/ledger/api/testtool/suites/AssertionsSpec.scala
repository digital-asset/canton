// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Assertions.emptyEventFormatErrorMessagePattern
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AssertionsSpec extends AnyWordSpec with Matchers {
  "The empty event format error pattern" should {
    "accept the legacy message" in {
      emptyEventFormatErrorMessagePattern
        .matcher("filtersByParty and filtersForAnyParty cannot be empty simultaneously")
        .matches() shouldBe true
    }

    "accept the message including contract-id filters" in {
      emptyEventFormatErrorMessagePattern
        .matcher(
          "filtersByParty, filtersForAnyParty and filtersByContractId cannot be empty simultaneously"
        )
        .matches() shouldBe true
    }

    "reject unrelated validation messages" in {
      emptyEventFormatErrorMessagePattern
        .matcher("filtersByParty cannot be empty simultaneously")
        .matches() shouldBe false
    }
  }
}
