// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import org.scalatest.wordspec.AnyWordSpec

class SubmissionRequestSizeLimitsTest extends AnyWordSpec with BaseTest {

  "SynchronizerLimits.transactionProtocolLimits" should {

    "provide default positive bounds for submission collections" in {
      val limits = SynchronizerLimits.default.transactionProtocolLimits
      limits.maxEnvelopes.value should be > 0
      limits.maxRecipientsPerBatch.value should be > 0
      limits.maxRecipientsPerEnvelope.value should be > 0
      limits.maxRecipientsTreeDepth.value should be > 0
    }

    "allow tightening specific bounds via copy" in {
      val tightened = SynchronizerLimits.default.copy(
        transactionProtocolLimits = SynchronizerLimits.default.transactionProtocolLimits.copy(
          maxEnvelopes = PositiveInt.one,
          maxRecipientsPerBatch = PositiveInt.tryCreate(2),
        )
      )

      tightened.transactionProtocolLimits.maxEnvelopes shouldBe PositiveInt.one
      tightened.transactionProtocolLimits.maxRecipientsPerBatch shouldBe PositiveInt.tryCreate(2)
    }
  }
}
