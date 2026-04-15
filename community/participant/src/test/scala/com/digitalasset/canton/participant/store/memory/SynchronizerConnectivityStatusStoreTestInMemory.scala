// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.SynchronizerConnectivityStatusStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class SynchronizerConnectivityStatusStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerConnectivityStatusStoreTest {

  "InMemorySynchronizerConnectivityStatusStore" should {
    behave like synchronizerConnectivityStatusStore(_ =>
      new InMemorySynchronizerConnectivityStatusStore()
    )
  }
}
