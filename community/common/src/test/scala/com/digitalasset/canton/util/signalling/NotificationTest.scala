// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.signalling

import com.digitalasset.canton.BaseTest
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class NotificationTest extends AnyWordSpec with BaseTest {
  "Notification" should {
    "correctly union" in {
      val broadcast = Notification.All
      val keys1 = Notification.Keys(NonEmpty(Set, 1, 2, 3))
      val keys2 = Notification.Keys(NonEmpty(Set, 4, 5, 6))
      val noTarget = Notification.NoTarget

      // identity
      forAll(Seq(broadcast, keys1, noTarget)) { notification =>
        Notification.union(notification, noTarget) shouldBe notification
        Notification.union(noTarget, notification) shouldBe notification
      }

      // keys union
      Notification.union(keys1, keys2) shouldBe Notification.union(keys2, keys1)

      // broadcast
      forAll(Seq(broadcast, keys1, noTarget)) { notification =>
        Notification.union(broadcast, notification) shouldBe broadcast
        Notification.union(notification, broadcast) shouldBe broadcast
      }
    }
  }
}
