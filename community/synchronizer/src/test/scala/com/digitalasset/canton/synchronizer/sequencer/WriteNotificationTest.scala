// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import org.scalatest.wordspec.AnyWordSpec

class WriteNotificationTest extends AnyWordSpec with BaseTest {

  "WriteNotification" should {
    "resolve to broadcast" in {
      val onlyBroadcast = NonEmpty(Set, SequencerMemberId.Broadcast)
      val onlyMemberIds = NonEmpty.from(Set.tabulate(5)(SequencerMemberId(_))).value

      WriteNotification.forMemberIds(onlyMemberIds).isBroadcast shouldBe false
      WriteNotification.forMemberIds(onlyBroadcast).isBroadcast shouldBe true
      WriteNotification.forMemberIds(onlyBroadcast ++ onlyMemberIds).isBroadcast shouldBe true
    }
    "correctly union" in {
      val broadcast = WriteNotification.All
      val members1 =
        WriteNotification.forMemberIds(NonEmpty(Set, 1, 2, 3).map(SequencerMemberId(_)))
      val members2 =
        WriteNotification.forMemberIds(NonEmpty(Set, 4, 5, 6).map(SequencerMemberId(_)))
      val noTarget = WriteNotification.NoTarget

      // identity
      forAll(Seq(broadcast, members1, noTarget)) { notification =>
        notification.union(noTarget) shouldBe notification
        noTarget.union(notification) shouldBe notification
      }

      // member union
      members1.union(members2) shouldBe members2.union(members1)

      // broadcast
      forAll(Seq(broadcast, members1, noTarget)) { notification =>
        broadcast.union(notification) shouldBe broadcast
        notification.union(broadcast) shouldBe broadcast
      }
    }
  }
}
