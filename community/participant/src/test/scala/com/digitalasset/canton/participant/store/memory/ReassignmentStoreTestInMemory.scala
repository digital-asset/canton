// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.ReassignmentStoreTest
import com.digitalasset.canton.participant.topology.OfflineTopologyLookup
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

final class ReassignmentStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ReassignmentStoreTest {

  private def mk(
      synchronizer: IndexedSynchronizer,
      offlineTopologyLookup: OfflineTopologyLookup,
  ): InMemoryReassignmentStore =
    new InMemoryReassignmentStore(
      Target(synchronizer.synchronizerId),
      DefaultTestIdentities.participant1,
      offlineTopologyLookup,
      loggerFactory,
    )

  "ReassignmentStoreTestInMemory" should {
    behave like reassignmentStore(mk)
  }
}
