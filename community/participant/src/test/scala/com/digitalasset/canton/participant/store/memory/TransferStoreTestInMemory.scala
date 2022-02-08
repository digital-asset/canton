// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.{BaseTest, DomainId}
import com.digitalasset.canton.participant.store.TransferStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class TransferStoreTestInMemory extends AsyncWordSpec with BaseTest with TransferStoreTest {

  def mk(domain: DomainId): InMemoryTransferStore = new InMemoryTransferStore(domain, loggerFactory)

  "TransferStoreTestInMemory" should {
    behave like transferStore(mk)
  }
}
