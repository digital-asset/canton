// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.PartyReplicationIndexingStoreTest

class InMemoryPartyReplicationIndexingStoreTest extends PartyReplicationIndexingStoreTest {
  "InMemoryInFlightSubmissionStore" should {
    behave like (partyReplicationIndexingStore(() => new InMemoryPartyReplicationIndexingStore()))
  }
}
