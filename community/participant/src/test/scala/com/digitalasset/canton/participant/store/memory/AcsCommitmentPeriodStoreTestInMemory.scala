// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStoreTest
import com.digitalasset.canton.platform.store.interning.StringInterning

import scala.concurrent.ExecutionContext

class AcsCommitmentPeriodStoreTestInMemory extends AcsCommitmentPeriodStoreTest {

  private def mkStore(stringInterning: StringInterning)(implicit
      executionContext: ExecutionContext
  ): InMemoryAcsCommitmentPeriodStore =
    new InMemoryAcsCommitmentPeriodStore(
      Eval.now(stringInterning),
      loggerFactory,
      enableConsistencyChecks = true,
    )

  "InMemoryAcsCommitmentPeriodStore" should {
    behave like acsCommitmentPeriodStore((stringInterning, executionContext) =>
      mkStore(stringInterning)(executionContext)
    )
  }

}
