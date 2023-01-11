// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{Member, ParticipantId}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTreeTest extends AnyWordSpec with BaseTest {
  lazy val p1: Member = ParticipantId("participant1")
  lazy val p2: Member = ParticipantId("participant2")
  lazy val p3: Member = ParticipantId("participant3")
  lazy val p4: Member = ParticipantId("participant4")
  lazy val p5: Member = ParticipantId("participant5")
  lazy val p6: Member = ParticipantId("participant6")

  lazy val t1 = RecipientsTree.leaf(NonEmpty(Set, p1, p5))
  lazy val t2 = RecipientsTree.leaf(NonEmpty(Set, p3))
  lazy val t3 = RecipientsTree(NonEmpty(Set, p4, p2), Seq(t1, t2))

  lazy val t4 = RecipientsTree.leaf(NonEmpty(Set, p2, p6))

  lazy val t5 = RecipientsTree(NonEmpty(Set, p1), Seq(t3, t4))

  "RecipientsTree" when {
    "allRecipients" should {
      "give all recipients" in {
        t5.allRecipients shouldBe Set(p1, p2, p3, p4, p5, p6)
      }
    }

    "forMember" should {
      "give all subtrees containing the member" in {
        t5.forMember(p2).toSet shouldBe Set(t4, t3)
      }

      // If a member appears in both the root of a subtree and in the root of a sub-subtree, it receives only the top-level subtree.
      "give only the top-level subtree when there is a subtree and a sub-subtree" in {
        t5.forMember(p1) shouldBe List(t5)
      }
    }
  }

  "serialization and deserialization" should {
    "preserve the same thing" in {

      val serialized = t5.toProtoV0
      val deserialized = RecipientsTree.fromProtoV0(serialized)

      deserialized shouldBe Right(t5)
    }
  }
}
