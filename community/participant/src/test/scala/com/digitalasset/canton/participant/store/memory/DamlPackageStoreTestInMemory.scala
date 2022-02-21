// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.DamlPackageStoreTest

class DamlPackageStoreTestInMemory extends DamlPackageStoreTest {
  "MemoryDamlPackagesDarsStore" should {
    behave like damlPackageStore(() => new InMemoryDamlPackageStore(loggerFactory))
  }
}
