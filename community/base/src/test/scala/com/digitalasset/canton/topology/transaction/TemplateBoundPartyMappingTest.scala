// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TemplateBoundPartyMappingTest extends AnyWordSpec with Matchers {

  private val partyId = PartyId.tryFromProtoPrimitive("pool::1220abcdef")
  private val participantId = ParticipantId.tryFromProtoPrimitive("PAR::participant1::1220abcdef")
  private val keyHash = ByteString.copyFrom(Array.fill(32)(0x42.toByte))

  private val mapping = TemplateBoundPartyMapping(
    partyId = partyId,
    hostingParticipantId = participantId,
    allowedTemplateIds = Set("com.example:AMMPool:1.0", "com.example:Token:1.0"),
    signingKeyHash = keyHash,
  )

  "TemplateBoundPartyMapping" should {

    "round-trip through protobuf serialization" in {
      val proto = mapping.toProtoV30
      val restored = TemplateBoundPartyMapping.fromProtoV30(proto)
      restored shouldBe Right(mapping)
    }

    "serialize party ID correctly" in {
      val proto = mapping.toProtoV30
      proto.party shouldBe partyId.toProtoPrimitive
    }

    "serialize participant ID correctly" in {
      val proto = mapping.toProtoV30
      proto.hostingParticipantUid shouldBe participantId.uid.toProtoPrimitive
    }

    "serialize allowed template IDs" in {
      val proto = mapping.toProtoV30
      proto.allowedTemplateIds.toSet shouldBe Set("com.example:AMMPool:1.0", "com.example:Token:1.0")
    }

    "serialize signing key hash" in {
      val proto = mapping.toProtoV30
      proto.signingKeyHash shouldBe keyHash
    }

    "wrap in TopologyMapping correctly" in {
      val topologyMapping = mapping.toProto
      topologyMapping.mapping.isTemplateBoundParty shouldBe true
    }

    "have correct topology mapping code" in {
      mapping.code shouldBe TopologyMapping.Code.TemplateBoundParty
    }

    "have correct namespace from party" in {
      mapping.namespace shouldBe partyId.namespace
    }

    "have correct UID from party" in {
      mapping.maybeUid shouldBe Some(partyId.uid)
    }

    "not be restricted to a domain" in {
      mapping.restrictedToDomain shouldBe None
    }

    "isTemplateAllowed with exact match" in {
      mapping.isTemplateAllowed("com.example:AMMPool:1.0") shouldBe true
    }

    "isTemplateAllowed rejects different version" in {
      mapping.isTemplateAllowed("com.example:AMMPool:2.0") shouldBe false
    }

    "isTemplateAllowed rejects different module" in {
      mapping.isTemplateAllowed("com.other:AMMPool:1.0") shouldBe false
    }

    "isTemplateAllowed rejects empty string" in {
      mapping.isTemplateAllowed("") shouldBe false
    }

    "isTemplateAllowed is case sensitive" in {
      mapping.isTemplateAllowed("com.example:ammpool:1.0") shouldBe false
    }

    "handle empty allowed template set" in {
      val emptyMapping = mapping.copy(allowedTemplateIds = Set.empty)
      emptyMapping.isTemplateAllowed("com.example:AMMPool:1.0") shouldBe false
      emptyMapping.isTemplateAllowed("") shouldBe false
    }

    "handle single allowed template" in {
      val singleMapping = mapping.copy(allowedTemplateIds = Set("com.example:Token:1.0"))
      singleMapping.isTemplateAllowed("com.example:Token:1.0") shouldBe true
      singleMapping.isTemplateAllowed("com.example:AMMPool:1.0") shouldBe false
    }

    "handle large allowed template set" in {
      val largeSet = (1 to 100).map(i => s"com.example:Template$i:1.0").toSet
      val largeMapping = mapping.copy(allowedTemplateIds = largeSet)
      largeMapping.isTemplateAllowed("com.example:Template50:1.0") shouldBe true
      largeMapping.isTemplateAllowed("com.example:Template101:1.0") shouldBe false
    }
  }

  "TemplateBoundPartyMapping proto deserialization" should {

    "handle empty allowed template list" in {
      val proto = v30.TemplateBoundParty(
        party = partyId.toProtoPrimitive,
        hostingParticipantUid = participantId.uid.toProtoPrimitive,
        allowedTemplateIds = Seq.empty,
        signingKeyHash = keyHash,
      )
      val result = TemplateBoundPartyMapping.fromProtoV30(proto)
      result.isRight shouldBe true
      result.map(_.allowedTemplateIds) shouldBe Right(Set.empty)
    }

    "preserve duplicate template IDs as a set" in {
      val proto = v30.TemplateBoundParty(
        party = partyId.toProtoPrimitive,
        hostingParticipantUid = participantId.uid.toProtoPrimitive,
        allowedTemplateIds = Seq("com.example:Token:1.0", "com.example:Token:1.0"),
        signingKeyHash = keyHash,
      )
      val result = TemplateBoundPartyMapping.fromProtoV30(proto)
      result.isRight shouldBe true
      result.map(_.allowedTemplateIds.size) shouldBe Right(1)
    }
  }
}
