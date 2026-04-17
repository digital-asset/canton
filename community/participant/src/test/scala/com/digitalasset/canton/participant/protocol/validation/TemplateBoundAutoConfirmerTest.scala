// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TemplateBoundPartyMapping
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TemplateBoundAutoConfirmerTest
    extends AsyncWordSpec
    with BaseTest
    with MockitoSugar {

  private implicit val ec: ExecutionContext = directExecutionContext

  private val partyA = PartyId.tryFromProtoPrimitive("partyA::1220abcdef")
  private val partyB = PartyId.tryFromProtoPrimitive("partyB::1220abcdef")
  private val partyC = PartyId.tryFromProtoPrimitive("partyC::1220abcdef")
  private val participantId = ParticipantId.tryFromProtoPrimitive("PAR::participant1::1220abcdef")
  private val keyHash = ByteString.copyFrom(Array.fill(32)(0x42.toByte))

  private val tbpConfig = TemplateBoundPartyMapping(
    partyId = partyA,
    hostingParticipantIds = Seq(participantId),
    allowedTemplateIds = Set("com.example:AMMPool:1.0"),
    signingKeyHash = keyHash,
  )

  private val confirmer = new TemplateBoundAutoConfirmer(loggerFactory)

  "TemplateBoundAutoConfirmer" should {

    "auto-confirm template-bound parties acting on allowed templates" in {
      val snapshot = mock[TopologySnapshot]
      when(snapshot.templateBoundPartyConfig(partyA.toLf))
        .thenReturn(FutureUnlessShutdown.pure(Some(tbpConfig)))
      when(snapshot.templateBoundPartyConfig(partyB.toLf))
        .thenReturn(FutureUnlessShutdown.pure(None))

      val templateIds = Map(
        partyA.toLf -> Set("com.example:AMMPool:1.0"),
        partyB.toLf -> Set("com.example:Token:1.0"),
      )

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set(partyA.toLf, partyB.toLf),
          actionTemplateIdsByParty = templateIds,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result shouldBe Right(Set(partyA.toLf))
        }
        .failOnShutdown
    }

    "reject when template-bound party acts on disallowed template" in {
      val snapshot = mock[TopologySnapshot]
      when(snapshot.templateBoundPartyConfig(partyA.toLf))
        .thenReturn(FutureUnlessShutdown.pure(Some(tbpConfig)))

      val templateIds = Map(
        partyA.toLf -> Set("com.example:EvilDrain:1.0"),
      )

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set(partyA.toLf),
          actionTemplateIdsByParty = templateIds,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result.isLeft shouldBe true
          result.left.getOrElse(fail()).message should include("EvilDrain")
        }
        .failOnShutdown
    }

    "return empty set when no parties are template-bound" in {
      val snapshot = mock[TopologySnapshot]
      when(snapshot.templateBoundPartyConfig(partyB.toLf))
        .thenReturn(FutureUnlessShutdown.pure(None))
      when(snapshot.templateBoundPartyConfig(partyC.toLf))
        .thenReturn(FutureUnlessShutdown.pure(None))

      val templateIds = Map(
        partyB.toLf -> Set("com.example:Token:1.0"),
        partyC.toLf -> Set("com.example:AMMPool:1.0"),
      )

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set(partyB.toLf, partyC.toLf),
          actionTemplateIdsByParty = templateIds,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result shouldBe Right(Set.empty)
        }
        .failOnShutdown
    }

    "handle empty involved parties" in {
      val snapshot = mock[TopologySnapshot]

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set.empty,
          actionTemplateIdsByParty = Map.empty,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result shouldBe Right(Set.empty)
        }
        .failOnShutdown
    }

    "allow template-bound party with no actions (party is observer)" in {
      val snapshot = mock[TopologySnapshot]
      when(snapshot.templateBoundPartyConfig(partyA.toLf))
        .thenReturn(FutureUnlessShutdown.pure(Some(tbpConfig)))

      // partyA is involved but has no actions (e.g., is only an observer)
      val templateIds = Map.empty[com.digitalasset.canton.LfPartyId, Set[String]]

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set(partyA.toLf),
          actionTemplateIdsByParty = templateIds,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result shouldBe Right(Set(partyA.toLf))
        }
        .failOnShutdown
    }

    "reject if any template-bound party fails, even if others pass" in {
      val tbpConfigB = tbpConfig.copy(
        partyId = partyB,
        allowedTemplateIds = Set("com.example:Token:1.0"),
      )
      val snapshot = mock[TopologySnapshot]
      when(snapshot.templateBoundPartyConfig(partyA.toLf))
        .thenReturn(FutureUnlessShutdown.pure(Some(tbpConfig)))
      when(snapshot.templateBoundPartyConfig(partyB.toLf))
        .thenReturn(FutureUnlessShutdown.pure(Some(tbpConfigB)))

      val templateIds = Map(
        partyA.toLf -> Set("com.example:AMMPool:1.0"), // valid
        partyB.toLf -> Set("com.example:EvilDrain:1.0"), // invalid
      )

      confirmer
        .checkAndAutoConfirm(
          involvedParties = Set(partyA.toLf, partyB.toLf),
          actionTemplateIdsByParty = templateIds,
          topologySnapshot = snapshot,
        )
        .map { result =>
          result.isLeft shouldBe true
        }
        .failOnShutdown
    }
  }
}
