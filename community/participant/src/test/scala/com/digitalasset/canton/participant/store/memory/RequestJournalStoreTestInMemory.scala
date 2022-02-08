// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.RequestJournalStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class RequestJournalStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RequestJournalStoreTest {
  "InMemoryRequestJournalStore" should {
    behave like requestJournalStore(() => new InMemoryRequestJournalStore(loggerFactory))
  }
}
