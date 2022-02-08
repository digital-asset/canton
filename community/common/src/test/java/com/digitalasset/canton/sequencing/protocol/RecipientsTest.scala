// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.option._
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.sequencing.protocol.Recipients.cc
import com.digitalasset.canton.sequencing.protocol.RecipientsTest._
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class RecipientsTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  lazy val recipients: Recipients = Recipients(NonEmptyList.of(t5, t2, t3, t5, t6))

  "Recipients" should {

    "filter for a member that doesn't occur" in {
      recipients.forMember(p7) shouldBe None
    }

    "filter for a member that appears in one tree" in {
      recipients.forMember(p6) shouldBe Some(Recipients(NonEmptyList.of(t6)))
    }

    "filter for a member that appears in several trees" in {
      recipients.forMember(p3) shouldBe Some(Recipients(NonEmptyList.of(t3, t3, t3)))
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
      val recipients = Recipients(NonEmptyList.of(RecipientsTree(NonEmptySet.of(p2, p1, p3), Nil)))
      recipients.asSingleGroup shouldBe NonEmptySet.of[Member](p3, p2, p1).some
    }

    "test for a single group when not present" in {

      // Multiple trees
      val case1 =
        Recipients(
          NonEmptyList.of(
            RecipientsTree(NonEmptySet.of(p2, p1, p3), Nil),
            RecipientsTree(NonEmptySet.of(p2), Nil),
          )
        )
      case1.asSingleGroup shouldBe None

      // Tree with height > 1
      val case2 =
        Recipients(
          NonEmptyList.of(
            RecipientsTree(
              NonEmptySet.of(p2, p1, p3),
              List(RecipientsTree(NonEmptySet.of(p1), Nil)),
            )
          )
        )
      case2.asSingleGroup shouldBe None
    }

    "correctly compute leaf members" in {
      val recipients = Recipients(
        NonEmptyList.of(
          RecipientsTree(
            NonEmptySet.of(participant(1), participant(2)),
            List(
              RecipientsTree(NonEmptySet.of(participant(3)), List()),
              RecipientsTree(NonEmptySet.of(participant(4)), List()),
              RecipientsTree(
                NonEmptySet.of(participant(5)),
                List(
                  RecipientsTree(NonEmptySet.of(participant(6), participant(2)), List())
                ),
              ),
            ),
          )
        )
      )
      recipients.leafMembers shouldBe NonEmptySet
        .of[Member](participant(2), participant(3), participant(4), participant(6))
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

  lazy val t1 = new RecipientsTree(NonEmptySet.of(p1), List.empty)
  lazy val t2 = new RecipientsTree(NonEmptySet.of(p2), List.empty)

  lazy val t3 = new RecipientsTree(NonEmptySet.of(p3), List(t1, t2))
  lazy val t4 = new RecipientsTree(NonEmptySet.of(p4), List.empty)

  lazy val t5 = new RecipientsTree(NonEmptySet.of(p5), List(t3, t4))

  lazy val t6 = new RecipientsTree(NonEmptySet.of(p6), List.empty)

  def testInstance: Recipients = {
    val dummyMember = ParticipantId("dummyParticipant")
    cc(dummyMember)
  }

  def participant(i: Int): ParticipantId = ParticipantId(s"participant$i")

}
