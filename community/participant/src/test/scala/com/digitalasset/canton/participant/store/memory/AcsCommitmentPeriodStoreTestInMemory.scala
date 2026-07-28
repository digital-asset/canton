// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStoreTest
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class AcsCommitmentPeriodStoreTestInMemory extends AcsCommitmentPeriodStoreTest {
  override def minimumProtocolVersion: ProtocolVersion = ProtocolVersion.minimum

  private def mkStore(stringInterning: StringInterning, enableConsistencyChecks: Boolean)(implicit
      executionContext: ExecutionContext
  ): InMemoryAcsCommitmentPeriodStore =
    new InMemoryAcsCommitmentPeriodStore(
      Eval.now(stringInterning),
      loggerFactory,
      enableConsistencyChecks,
    )

  "InMemoryAcsCommitmentPeriodStore" should {
    behave like acsCommitmentPeriodStore(
      (stringInterning, enableConsistencyChecks, executionContext) =>
        mkStore(stringInterning, enableConsistencyChecks)(executionContext)
    )
  }

}
