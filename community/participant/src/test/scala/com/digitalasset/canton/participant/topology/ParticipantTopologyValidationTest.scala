// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.IncompleteReassignmentData.UnassignmentEventGlobalOffset
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  party1,
  synchronizerId,
}
import com.digitalasset.canton.topology.ForceFlags
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantTopologyManagerError.InsufficientSignatoryAssigningParticipantsForParty
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

class ParticipantTopologyValidationTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val validation = new ParticipantTopologyValidation {
    override protected def loggerFactory: NamedLoggerFactory =
      ParticipantTopologyValidationTest.this.loggerFactory
  }

  "checkInsufficientSignatoryAssigningParticipantsForParty" when {
    // participant1 (Active) and participant2 are the reassigning participants of an incomplete
    // reassignment. Raising the threshold to 2 passes only if participant2's entry counts.
    def raiseThresholdTo2(
        participant2Permission: ParticipantPermission,
        participant2State: HostingState,
    ) = {
      val store = mock[ReassignmentStore]
      when(
        store.findIncomplete(
          eqTo(Offset.firstOffset),
          any[Option[NonEmpty[Set[LfPartyId]]]],
          any[NonNegativeInt],
        )(anyTraceContext)
      ).thenReturn(
        FutureUnlessShutdown.pure(
          Seq(
            IncompleteReassignmentData(
              ReassignmentId.tryCreate("00"),
              reassigningParticipants = Some(Set(participant1, participant2)),
              UnassignmentEventGlobalOffset(Offset.firstOffset),
              queryOffset = Offset.firstOffset,
            )
          )
        )
      )
      validation
        .checkInsufficientSignatoryAssigningParticipantsForParty(
          party1,
          currentThreshold = PositiveInt.one,
          nextThresholdO = Some(PositiveInt.two),
          nextHostingParticipants = Seq(
            HostingParticipant(participant1, Confirmation, HostingState.Active),
            HostingParticipant(participant2, participant2Permission, participant2State),
          ),
          ForceFlags.none,
          reassignmentStores = Map(synchronizerId -> store),
          ledgerEnd = () => None,
        )
        .value
        .futureValueUS
    }

    "the next mapping has a confirming entry" should {
      // Nominated and Freezing as released in 3.7.0; judges the end state, in which every entry is Active
      "count it if Nominated, Freezing or Active" in {
        Seq(HostingState.Nominated, HostingState.Freezing, HostingState.Active).foreach { state =>
          clue(state.toString)(raiseThresholdTo2(Confirmation, state) shouldBe Right(()))
        }
      }

      // As released: an Onboarding entry does not confirm until its onboarding completes
      "not count it if Onboarding" in {
        raiseThresholdTo2(Confirmation, HostingState.Onboarding).left.value shouldBe a[
          InsufficientSignatoryAssigningParticipantsForParty.RejectThresholdIncrease
        ]
      }
    }

    "the next mapping has an Observation entry" should {
      "not count it in any state" in {
        Seq(
          HostingState.Nominated,
          HostingState.Freezing,
          HostingState.Onboarding,
          HostingState.Active,
        ).foreach { state =>
          clue(state.toString)(
            raiseThresholdTo2(Observation, state).left.value shouldBe a[
              InsufficientSignatoryAssigningParticipantsForParty.RejectThresholdIncrease
            ]
          )
        }
      }
    }
  }
}
