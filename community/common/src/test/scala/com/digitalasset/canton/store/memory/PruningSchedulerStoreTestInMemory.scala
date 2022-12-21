// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory;

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.String3
import com.digitalasset.canton.store.PruningSchedulerStoreTest
import org.scalatest.wordspec.AsyncWordSpec;

class PruningSchedulerStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with PruningSchedulerStoreTest {

  "InMemoryPruningSchedulerStore" should {
    behave like pruningSchedulerStore(() =>
      new InMemoryPruningSchedulerStore(String3.tryCreate("TST"), loggerFactory)
    )
  }
}