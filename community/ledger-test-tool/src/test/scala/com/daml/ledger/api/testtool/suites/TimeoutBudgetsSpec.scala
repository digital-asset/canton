// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.{Eventually, LedgerTestCasesRunner}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeoutBudgetsSpec extends AnyWordSpec with Matchers {

  "the per-case timeout" should {
    // Check the lowest scale factor, where the cap is most likely to fire first.
    // If it does, we get a bare `TimedOut` instead of the failed assertion.
    "outlast the longest chain of eventually loops a single case can reach" in {
      LedgerTestCasesRunner.caseTimeout(1.0, 1.0).toMillis should be >
        (Eventually.DefaultMaxDeadline * 2).toMillis
    }
  }
}
