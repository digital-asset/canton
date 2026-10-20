// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.ProtoDeserializationError.{InvariantViolation, UnrecognizedEnum}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.topology.DefaultTestIdentities.{participant1, participant2, party1}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
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

  private def ptpV30(
      onboarding: Boolean,
      participant2State: v30.PartyToParticipant.HostingParticipant.HostingState,
  ): v30.PartyToParticipant =
    v30.PartyToParticipant(
      party = party1.toProtoPrimitive,
      threshold = 1,
      participants = Seq(
        v30.PartyToParticipant.HostingParticipant(
          participant1.uid.toProtoPrimitive,
          v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
          None,
          v30.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_UNSPECIFIED,
        ),
        v30.PartyToParticipant.HostingParticipant(
          participant2.uid.toProtoPrimitive,
          v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
          Option.when(onboarding)(v30.PartyToParticipant.HostingParticipant.Onboarding()),
          participant2State,
        ),
      ),
      partySigningKeys = None,
    )

  private val unsupportedPV = ProtocolVersionValidation.PV(ProtocolVersion.v36)
  private val supportedPV = ProtocolVersionValidation.PV(ProtocolVersion.offPRWithSequencerChannels)

  private val nominatedP =
    v30.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_NOMINATED
  private val freezingP =
    v30.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_FREEZING
  private val unspecifiedP =
    v30.PartyToParticipant.HostingParticipant.HostingState.HOSTING_STATE_UNSPECIFIED
  private val unrecognizedP =
    v30.PartyToParticipant.HostingParticipant.HostingState.Unrecognized(42)

  private def parseV30(
      pvv: ProtocolVersionValidation,
      proto: v30.PartyToParticipant,
  ): Either[ProtoDeserializationError, PartyToParticipant] =
    PartyToParticipant.fromProtoV30(pvv, testedTopologyLimits, proto)

  "PartyToParticipant v30" should {
    "serialize every hosting state, Onboarding and Active as in earlier releases" in {
      Seq(
        HostingState.Nominated -> ptpV30(onboarding = false, nominatedP),
        HostingState.Freezing -> ptpV30(onboarding = false, freezingP),
        HostingState.Onboarding -> ptpV30(onboarding = true, unspecifiedP),
        HostingState.Active -> ptpV30(onboarding = false, unspecifiedP),
      ).foreach { case (state, expectedProto) =>
        val mapping = ptp(state)
        val proto = mapping.toProtoPartyToParticipantV30.value

        proto shouldBe expectedProto
        parseV30(supportedPV, proto).value shouldBe mapping
        parseV30(pvv, proto).value shouldBe mapping
      }
    }

    "ignore the hosting state before ProtocolVersion.offPRWithSequencerChannels" in {
      parseV30(unsupportedPV, ptpV30(onboarding = false, freezingP)).value shouldBe
        ptp(HostingState.Active)
      parseV30(unsupportedPV, ptpV30(onboarding = true, nominatedP)).value shouldBe
        ptp(HostingState.Onboarding)
      parseV30(unsupportedPV, ptpV30(onboarding = false, unrecognizedP)).value shouldBe
        ptp(HostingState.Active)
    }

    "reject a hosting state with the onboarding flag, and unrecognized hosting states, for untrusted input" in {
      Seq(supportedPV, ProtocolVersionValidation.AlwaysValidation).foreach { untrusted =>
        parseV30(untrusted, ptpV30(onboarding = true, freezingP)).left.value shouldBe
          an[InvariantViolation]
        parseV30(untrusted, ptpV30(onboarding = false, unrecognizedP)).left.value shouldBe
          an[UnrecognizedEnum]
      }
    }

    "read such hosting states in trusted input: the onboarding flag wins, unrecognized counts as unset" in {
      parseV30(pvv, ptpV30(onboarding = true, freezingP)).value shouldBe
        ptp(HostingState.Onboarding)
      parseV30(pvv, ptpV30(onboarding = true, unrecognizedP)).value shouldBe
        ptp(HostingState.Onboarding)
      parseV30(pvv, ptpV30(onboarding = false, unrecognizedP)).value shouldBe
        ptp(HostingState.Active)
    }
  }

  "TopologyTransaction.create" should {
    "reject Nominated and Freezing before ProtocolVersion.offPRWithSequencerChannels" in {
      Seq(HostingState.Nominated, HostingState.Freezing).foreach { state =>
        def create(protocolVersion: ProtocolVersion) =
          TopologyTransaction.create(
            TopologyChangeOp.Replace,
            PositiveInt.one,
            ptp(state),
            protocolVersion,
          )

        create(ProtocolVersion.v36).left.value should include(
          "The hosting states Nominated and Freezing require protocol version"
        )
        create(ProtocolVersion.offPRWithSequencerChannels).value.mapping shouldBe ptp(state)
      }
    }
  }
}
