// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.NonEmptySet
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.ParticipantId
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTreeTest extends AnyWordSpec with BaseTest {
  lazy val p1 = ParticipantId("participant1")
  lazy val p2 = ParticipantId("participant2")
  lazy val p3 = ParticipantId("participant3")
  lazy val p4 = ParticipantId("participant4")
  lazy val p5 = ParticipantId("participant5")
  lazy val p6 = ParticipantId("participant6")

  lazy val t1 = new RecipientsTree(NonEmptySet.of(p1, p5), List.empty)
  lazy val t2 = new RecipientsTree(NonEmptySet.of(p3), List.empty)
  lazy val t3 = new RecipientsTree(NonEmptySet.of(p4, p2), List(t1, t2))

  lazy val t4 = new RecipientsTree(NonEmptySet.of(p2, p6), List.empty)

  lazy val t5 = new RecipientsTree(NonEmptySet.of(p1), List(t3, t4))

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
