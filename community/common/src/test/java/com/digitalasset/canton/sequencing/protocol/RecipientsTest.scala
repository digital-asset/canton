// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.sequencing.protocol.Recipients.cc
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  lazy val recipients: Recipients = Recipients(NonEmpty(Seq, t5, t2, t3, t5, t6))

  "Recipients" should {

    "filter for a member that doesn't occur" in {
      recipients.forMember(p7) shouldBe None
    }

    "filter for a member that appears in one tree" in {
      recipients.forMember(p6) shouldBe Some(Recipients(NonEmpty(Seq, t6)))
    }

    "filter for a member that appears in several trees" in {
      recipients.forMember(p3) shouldBe Some(Recipients(NonEmpty(Seq, t3, t3, t3)))
    }

    "be preserved through serialization / deserialization" in {
      val proto = recipients.toProtoV0
      val fromProto = Recipients.fromProtoV0(proto)
      fromProto shouldBe Right(recipients)
    }

    "store all recipients" in {
      val all = recipients.allRecipients
      all shouldBe Set(p1, p2, p3, p4, p5, p6)
    }

    "test for a single group when present" in {
      val recipients =
        Recipients(NonEmpty(Seq, RecipientsTree.leaf(NonEmpty.mk(Set, p2, p1, p3))))
      recipients.asSingleGroup shouldBe NonEmpty.mk(Set, p3, p2, p1).some
    }

    "test for a single group when not present" in {

      // Multiple trees
      val case1 =
        Recipients(
          NonEmpty(
            List,
            RecipientsTree.leaf(NonEmpty.mk(Set, p2, p1, p3)),
            RecipientsTree.leaf(NonEmpty.mk(Set, p2)),
          )
        )
      case1.asSingleGroup shouldBe None

      // Tree with height > 1
      val case2 =
        Recipients(
          NonEmpty(
            List,
            RecipientsTree(
              NonEmpty.mk(Set, p2, p1, p3),
              Seq(RecipientsTree.leaf(NonEmpty.mk(Set, p1))),
            ),
          )
        )
      case2.asSingleGroup shouldBe None
    }

    "correctly compute leaf members" in {
      val recipients = Recipients(
        NonEmpty(
          List,
          RecipientsTree(
            NonEmpty.mk(Set, participant(1), participant(2)),
            Seq(
              RecipientsTree.leaf(NonEmpty.mk(Set, participant(3))),
              RecipientsTree.leaf(NonEmpty.mk(Set, participant(4))),
              RecipientsTree(
                NonEmpty.mk(Set, participant(5)),
                Seq(
                  RecipientsTree.leaf(NonEmpty.mk(Set, participant(6), participant(2)))
                ),
              ),
            ),
          ),
        )
      )
      recipients.leafMembers shouldBe
        NonEmpty.mk(Set, participant(2), participant(3), participant(4), participant(6))
    }
  }
}

object RecipientsTest {

  lazy val p1 = ParticipantId("participant1")
  lazy val p2 = ParticipantId("participant2")
  lazy val p3 = ParticipantId("participant3")
  lazy val p4 = ParticipantId("participant4")
  lazy val p5 = ParticipantId("participant5")
  lazy val p6 = ParticipantId("participant6")
  lazy val p7 = ParticipantId("participant7")
  lazy val p8 = ParticipantId("participant8")

  lazy val t1 = RecipientsTree.leaf(NonEmpty.mk(Set, p1))
  lazy val t2 = RecipientsTree.leaf(NonEmpty.mk(Set, p2))

  lazy val t3 = RecipientsTree(NonEmpty.mk(Set, p3), Seq(t1, t2))
  lazy val t4 = RecipientsTree.leaf(NonEmpty.mk(Set, p4))

  lazy val t5 = RecipientsTree(NonEmpty.mk(Set, p5), Seq(t3, t4))

  lazy val t6 = RecipientsTree.leaf(NonEmpty.mk(Set, p6))

  def testInstance: Recipients = {
    val dummyMember = ParticipantId("dummyParticipant")
    cc(dummyMember)
  }

  def participant(i: Int): ParticipantId = ParticipantId(s"participant$i")

}
