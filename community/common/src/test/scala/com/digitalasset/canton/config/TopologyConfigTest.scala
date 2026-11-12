// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class TopologyConfigTest extends AnyWordSpec with BaseTest {
  "TopologyConfig" should {
    "preserve the existing snapshot check defaults when no override is set" in {
      TopologyConfig().shouldCheckTopologySnapshots(false) shouldBe false
      TopologyConfig().shouldCheckTopologySnapshots(true) shouldBe true
      TopologyConfig.forTesting.shouldCheckTopologySnapshots(false) shouldBe true
    }

    "disable snapshot checks independently of the other consistency checks" in {
      val config = TopologyConfig.forTesting.copy(
        enableTopologySnapshotConsistencyChecks = Some(false)
      )

      config.shouldCheckTopologySnapshots(true) shouldBe false
      config.shouldCheckTopologySnapshots(false) shouldBe false
      config.enableTopologyStateCacheConsistencyChecks shouldBe true
    }

    "enable snapshot checks independently of the other consistency checks" in {
      val config = TopologyConfig(enableTopologySnapshotConsistencyChecks = Some(true))

      config.shouldCheckTopologySnapshots(false) shouldBe true
      config.shouldCheckTopologySnapshots(true) shouldBe true
    }
  }
}
