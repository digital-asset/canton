// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.SubmissionTrackerStoreTest
import com.digitalasset.canton.topology.DefaultTestIdentities

final class SubmissionTrackerStoreTestInMemory extends SubmissionTrackerStoreTest {
  "InMemorySubmissionTrackerStore" should {
    behave like submissionTrackerStore(() =>
      new InMemorySubmissionTrackerStore(
        DefaultTestIdentities.physicalSynchronizerId,
        loggerFactory,
        timeouts,
      )
    )
  }
}
