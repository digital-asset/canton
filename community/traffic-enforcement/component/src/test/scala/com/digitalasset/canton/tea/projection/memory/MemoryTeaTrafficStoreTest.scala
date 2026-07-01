// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.tea.projection.TeaTrafficStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class MemoryTeaTrafficStoreTest extends AsyncWordSpec with BaseTest with TeaTrafficStoreTest {

  "MemoryTeaTrafficStore" should {
    behave like teaTrafficStore(() => new TeaMemoryTrafficStore())
  }

}
