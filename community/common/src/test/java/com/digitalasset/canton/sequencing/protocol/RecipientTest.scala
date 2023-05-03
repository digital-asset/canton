// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import org.scalatest.wordspec.AnyWordSpec

class RecipientTest extends AnyWordSpec with BaseTest {
  val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::party"))
  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))

  val memberRecipient = MemberRecipient(ParticipantId("participant1"))
  val participantsOfParty = ParticipantsOfParty(alice)
  val sequencersOfDomain = SequencersOfDomain(domainId)
  val mediatorsOfDomain = MediatorsOfDomain(domainId, 99312312)

  "recipient test serialization" should {
    "be able to convert back and forth" in {
      Recipient.fromProtoPrimitive(
        memberRecipient.toProtoPrimitive,
        "recipient",
      ) shouldBe Right(memberRecipient)

      Recipient.fromProtoPrimitive(
        participantsOfParty.toProtoPrimitive,
        "recipient",
      ) shouldBe Right(participantsOfParty)

      Recipient.fromProtoPrimitive(
        sequencersOfDomain.toProtoPrimitive,
        "recipient",
      ) shouldBe Right(sequencersOfDomain)

      Recipient.fromProtoPrimitive(
        mediatorsOfDomain.toProtoPrimitive,
        "recipient",
      ) shouldBe Right(mediatorsOfDomain)
    }

    "act sanely on invalid inputs" in {
      forAll(
        Seq(
          "nothing valid",
          "",
          "::",
          "INV::invalid",
          "POP",
          "POP::incomplete",
          "POP::incomplete::",
          "POP,,alice::party",
          "MOD::99312312",
          "MOD::99312312::",
          "MOD::99312312::incomplete",
          "MOD::not-a-number::domain::da",
          "MOD::99312312993123129931231299312312::domain::da",
        )
      ) { str =>
        Recipient
          .fromProtoPrimitive(str, "recipient")
          .left
          .value shouldBe a[ProtoDeserializationError]
      }
    }
  }
}
