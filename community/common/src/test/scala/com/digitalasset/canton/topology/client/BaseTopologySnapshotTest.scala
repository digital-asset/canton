// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.BaseTopologySnapshot.HostingView
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantAttributes,
  PartyToParticipant,
}
import org.scalatest.wordspec.AnyWordSpec

class BaseTopologySnapshotTest extends AnyWordSpec with BaseTest {

  private def mapping(threshold: PositiveInt, hosts: HostingParticipant*): PartyToParticipant =
    PartyToParticipant.tryCreate(party1, threshold, hosts)

  private val allSubmission = Map(
    participant1 -> ParticipantAttributes(Submission),
    participant2 -> ParticipantAttributes(Submission),
  )

  private def partyInfo(
      mapping: PartyToParticipant,
      participantAttributes: Map[ParticipantId, ParticipantAttributes] = allSubmission,
  ): Option[PartyInfo] = {
    val result =
      BaseTopologySnapshot.partyInfoFromMapping(mapping, participantAttributes, HostingView.Default)
    // TODO(#36492): W2(a): Test each view separately once partyInfoFromMapping applies the view.
    BaseTopologySnapshot.partyInfoFromMapping(
      mapping,
      participantAttributes,
      HostingView.IgnoringFreeze,
    ) shouldBe result
    result
  }

  "partyInfoFromMapping" should {
    "lower each entry's permission to the participant's synchronizer permission" in {
      val loginAfter = Some(CantonTimestamp.ofEpochSecond(1))
      partyInfo(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Submission),
          HostingParticipant(participant2, Observation),
        ),
        Map(
          participant1 -> ParticipantAttributes(Confirmation, loginAfter),
          participant2 -> ParticipantAttributes(Submission),
        ),
      ).value.participants shouldBe Map(
        participant1 -> ParticipantAttributes(Confirmation, loginAfter),
        participant2 -> ParticipantAttributes(Observation),
      )
    }

    "drop entries of participants that left the synchronizer" in {
      val ptp = mapping(
        PositiveInt.one,
        HostingParticipant(participant1, Submission),
        HostingParticipant(participant3, Submission),
      )
      partyInfo(ptp).value.participants.keySet shouldBe Set(participant1)
      partyInfo(ptp, Map(participant2 -> ParticipantAttributes(Submission))) shouldBe None
    }

    // TODO(#36492): W3: Replace with effective-confirmation-threshold tests once the threshold is
    //  derived from the hosting states.
    "pass the threshold through unchanged" in {
      partyInfo(
        mapping(
          PositiveInt.two,
          HostingParticipant(participant1, Confirmation),
          HostingParticipant(participant2, Confirmation),
        )
      ).value.threshold shouldBe PositiveInt.two
    }

    "not let ONBOARDING entries confirm" in {
      val attributes = partyInfo(
        mapping(
          PositiveInt.one,
          HostingParticipant(participant1, Confirmation, HostingState.Onboarding),
        )
      ).value.participants(participant1)
      attributes.onboarding shouldBe true
      attributes.canConfirm shouldBe false
    }

    // TODO(#36492): W2(a): Replace with tests of the view semantics: NOMINATED and FREEZING
    //  entries host in neither view, and the default view omits the party while any entry is
    //  FREEZING.
    "treat NOMINATED and FREEZING entries as ACTIVE" in {
      val active =
        partyInfo(mapping(PositiveInt.one, HostingParticipant(participant1, Confirmation))).value
      Seq(HostingState.Nominated, HostingState.Freezing).foreach { state =>
        partyInfo(
          mapping(PositiveInt.one, HostingParticipant(participant1, Confirmation, state))
        ).value shouldBe active
      }
      active.participants(participant1).canConfirm shouldBe true
    }
  }
}
