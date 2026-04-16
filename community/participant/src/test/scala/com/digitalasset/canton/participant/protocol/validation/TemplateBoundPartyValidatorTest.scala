// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.topology.transaction.TemplateBoundPartyMapping
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TemplateBoundPartyValidatorTest extends AnyWordSpec with Matchers {

  private val partyId = PartyId.tryFromProtoPrimitive("pool::1220abcdef")
  private val participantId = ParticipantId.tryFromProtoPrimitive("PAR::participant1::1220abcdef")

  private val config = TemplateBoundPartyMapping(
    partyId = partyId,
    hostingParticipantId = participantId,
    allowedTemplateIds = Set(
      "com.example:AMMPool:1.0",
      "com.example:Token:1.0",
    ),
    signingKeyHash = ByteString.copyFrom(Array.fill(32)(0x42.toByte)),
  )

  "TemplateBoundPartyValidator" should {

    "accept actions on allowed templates" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.example:AMMPool:1.0", "com.example:Token:1.0"),
      )
      result shouldBe TemplateBoundPartyValidator.Valid
    }

    "accept a subset of allowed templates" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.example:Token:1.0"),
      )
      result shouldBe TemplateBoundPartyValidator.Valid
    }

    "accept empty action set" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set.empty,
      )
      result shouldBe TemplateBoundPartyValidator.Valid
    }

    "reject actions on disallowed templates" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.example:AMMPool:1.0", "com.example:MaliciousDrain:1.0"),
      )
      result match {
        case TemplateBoundPartyValidator.Invalid(_, disallowed) =>
          disallowed shouldBe Set("com.example:MaliciousDrain:1.0")
        case TemplateBoundPartyValidator.Valid =>
          fail("Expected Invalid")
      }
    }

    "reject when all actions are on disallowed templates" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.evil:Steal:1.0", "com.evil:Drain:1.0"),
      )
      result match {
        case TemplateBoundPartyValidator.Invalid(_, disallowed) =>
          disallowed should have size 2
        case TemplateBoundPartyValidator.Valid =>
          fail("Expected Invalid")
      }
    }

    "reject a single disallowed template mixed with allowed ones" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.example:AMMPool:1.0", "com.example:Token:1.0", "com.evil:Steal:1.0"),
      )
      result match {
        case TemplateBoundPartyValidator.Invalid(_, disallowed) =>
          disallowed shouldBe Set("com.evil:Steal:1.0")
        case TemplateBoundPartyValidator.Valid =>
          fail("Expected Invalid")
      }
    }

    "produce a descriptive error message" in {
      val result = TemplateBoundPartyValidator.validate(
        partyId.toLf,
        config,
        Set("com.evil:Steal:1.0"),
      )
      result match {
        case invalid: TemplateBoundPartyValidator.Invalid =>
          invalid.message should include("pool")
          invalid.message should include("com.evil:Steal:1.0")
        case _ => fail("Expected Invalid")
      }
    }
  }

  "TemplateBoundPartyMapping" should {

    "isTemplateAllowed returns true for allowed templates" in {
      config.isTemplateAllowed("com.example:AMMPool:1.0") shouldBe true
      config.isTemplateAllowed("com.example:Token:1.0") shouldBe true
    }

    "isTemplateAllowed returns false for disallowed templates" in {
      config.isTemplateAllowed("com.example:Unknown:1.0") shouldBe false
      config.isTemplateAllowed("") shouldBe false
    }
  }
}
