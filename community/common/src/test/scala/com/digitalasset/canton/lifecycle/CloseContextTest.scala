// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class CloseContextTest extends AnyWordSpec with BaseTest {

  private def combine(parent1: FlagCloseable, parent2: FlagCloseable): CloseContext =
    CloseContext.combineUnsafe(CloseContext(parent1), CloseContext(parent2), timeouts, logger)

  "CloseContext.combineUnsafe" should {

    "close the combined context when either parent closes" in {
      forEvery(Seq(true, false)) { closeFirst =>
        val parent1 = FlagCloseable(logger, timeouts)
        val parent2 = FlagCloseable(logger, timeouts)
        val combined = combine(parent1, parent2)
        combined.context.isClosing shouldBe false

        if (closeFirst) parent1.close() else parent2.close()
        combined.context.isClosing shouldBe true

        parent1.close()
        parent2.close()
        succeed
      }
    }

    "be closed when combining parents that are already closed" in {
      val parent1 = FlagCloseable(logger, timeouts)
      val parent2 = FlagCloseable(logger, timeouts)
      parent1.close()
      parent2.close()

      combine(parent1, parent2).context.isClosing shouldBe true
    }
  }
}
