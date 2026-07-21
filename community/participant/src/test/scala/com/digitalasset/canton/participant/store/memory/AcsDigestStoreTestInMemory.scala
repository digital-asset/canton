// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.AcsDigestStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class AcsDigestStoreTestInMemory extends AsyncWordSpec with BaseTest with AcsDigestStoreTest {

  "InMemoryAcsDigestStore" should {
    behave like acsDigestSingleStoreTests((ec) =>
      InMemoryAcsDigestStore.create(Eval.now(mockStringInterning), loggerFactory)(ec)
    )
    behave like acsDigestMultiStoresTests((ec, _) =>
      InMemoryAcsDigestStore.create(Eval.now(mockStringInterning), loggerFactory)(ec)
    )
  }
}
