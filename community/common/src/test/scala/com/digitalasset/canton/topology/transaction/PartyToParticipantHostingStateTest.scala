// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.{FieldNotSet, UnrecognizedEnum}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, participant2, party1}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.version.{ProtoVersion, ProtocolVersionValidation}
import org.scalatest.wordspec.AnyWordSpec

class PartyToParticipantHostingStateTest extends AnyWordSpec with BaseTest {

  private val pvv = ProtocolVersionValidation.NoValidation

  private def ptp(participant2State: HostingState): PartyToParticipant =
    PartyToParticipant.tryCreate(
      party1,
      PositiveInt.one,
      Seq(
        HostingParticipant(participant1, Submission, HostingState.Active),
        HostingParticipant(participant2, Confirmation, participant2State),
      ),
    )

  private def ptpV31(
      participant2State: v31.PartyToParticipant.HostingParticipant.HostingState
  ): v31.PartyToParticipant =
    v31.PartyToParticipant(
      party = party1.toProtoPrimitive,
      threshold = 1,
      participants = Seq(
        v31.PartyToParticipant.HostingParticipant(
          participant1.uid.toProtoPrimitive,
          v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
          v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_ACTIVE,
        ),
        v31.PartyToParticipant.HostingParticipant(
          participant2.uid.toProtoPrimitive,
          v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
          participant2State,
        ),
      ),
      partySigningKeys = None,
    )

  "PartyToParticipant v31" should {
    "serialize every hosting state" in {
      Seq(
        HostingState.Nominated -> v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_NOMINATED,
        HostingState.Freezing -> v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_FREEZING,
        HostingState.Onboarding -> v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_ONBOARDING,
        HostingState.Active -> v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_ACTIVE,
      ).foreach { case (state, stateP) =>
        val mapping = ptp(state)
        val proto = mapping.toProtoPartyToParticipantV31.value

        proto shouldBe ptpV31(stateP)
        PartyToParticipant.fromProtoV31(pvv, testedTopologyLimits, proto).value shouldBe mapping
      }
    }

    "reject an unspecified hosting state" in {
      PartyToParticipant
        .fromProtoV31(
          pvv,
          testedTopologyLimits,
          ptpV31(v31.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_UNSPECIFIED),
        )
        .left
        .value shouldBe a[FieldNotSet]
    }

    "reject an unrecognized hosting state" in {
      PartyToParticipant
        .fromProtoV31(
          pvv,
          testedTopologyLimits,
          ptpV31(v31.PartyToParticipant.HostingParticipant.HostingState.Unrecognized(42)),
        )
        .left
        .value shouldBe an[UnrecognizedEnum]
    }
  }

  "PartyToParticipant v30" should {
    "represent Onboarding as the onboarding flag and Active as no flag" in {
      val mapping = ptp(HostingState.Onboarding)
      val proto = mapping.toProtoPartyToParticipantV30.value

      proto shouldBe v30.PartyToParticipant(
        party = party1.toProtoPrimitive,
        threshold = 1,
        participants = Seq(
          v30.PartyToParticipant.HostingParticipant(
            participant1.uid.toProtoPrimitive,
            v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
            None,
          ),
          v30.PartyToParticipant.HostingParticipant(
            participant2.uid.toProtoPrimitive,
            v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
            Some(v30.PartyToParticipant.HostingParticipant.Onboarding()),
          ),
        ),
        partySigningKeys = None,
      )
      PartyToParticipant.fromProtoV30(pvv, testedTopologyLimits, proto).value shouldBe mapping
    }

    "reject Nominated and Freezing" in {
      Seq(HostingState.Nominated, HostingState.Freezing).foreach { state =>
        val mapping = ptp(state)

        mapping.canBeSerializedTo(ProtoVersion(31)) shouldBe Right(())
        mapping.canBeSerializedTo(ProtoVersion(30)).left.value should include(
          "unsupported hosting states"
        )
        mapping.toProtoPartyToParticipantV30.left.value should include(
          "unsupported hosting states"
        )
      }
    }
  }
}
