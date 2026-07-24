// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class WriteNotificationTest extends AnyWordSpec with BaseTest {

  "WriteNotification.forMemberIds" should {
    "resolve to broadcast" in {
      val onlyBroadcast = NonEmpty(Set, SequencerMemberId.Broadcast)
      val onlyMemberIds = NonEmpty.from(Set.tabulate(5)(SequencerMemberId(_))).value

      WriteNotification.forMemberIds(onlyMemberIds).isBroadcast shouldBe false
      WriteNotification.forMemberIds(onlyBroadcast).isBroadcast shouldBe true
      WriteNotification.forMemberIds(onlyBroadcast ++ onlyMemberIds).isBroadcast shouldBe true
    }
  }
}
