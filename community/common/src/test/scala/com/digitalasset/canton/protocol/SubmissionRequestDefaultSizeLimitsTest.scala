// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

final class SubmissionRequestDefaultSizeLimitsTest extends AnyWordSpec with BaseTest {
  "SynchronizerLimits.transactionProtocolLimits" should {
    "provide default positive bounds for submission collections from PV 36" in {
      ProtocolVersion.supported.filter(_ >= ProtocolVersion.v36).foreach { pv =>
        val limits = SynchronizerLimits.defaultFor(pv).transactionProtocolLimits
        limits.maxEnvelopes should be < PositiveInt.MaxValue
        limits.maxRecipientsPerBatch should be < PositiveInt.MaxValue
        limits.maxRecipientsPerEnvelope should be < PositiveInt.MaxValue
        limits.maxRecipientsTreeDepth should be < PositiveInt.MaxValue
      }
    }
  }
}
