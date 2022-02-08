// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import com.digitalasset.canton.crypto.store.CryptoPrivateStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class CryptoPrivateStoreTestInMemory extends AsyncWordSpec with CryptoPrivateStoreTest {
  "InMemoryCryptoPrivateStore" should {
    behave like cryptoPrivateStore(new InMemoryCryptoPrivateStore(loggerFactory))
  }
}
