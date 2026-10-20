// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState.{
  Active,
  Freezing,
  Nominated,
  Onboarding,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.SeqMap

class PartyToParticipantComputationsTest extends AnyWordSpec with BaseTest {

  private lazy val computations = new PartyToParticipantComputations(loggerFactory)

  private def participantIdFor(idx: Int) =
    ParticipantId(UniqueIdentifier.tryCreate(s"participant$idx", s"participant$idx-identity"))

  private val p1 = participantIdFor(1)
  private val p2 = participantIdFor(2)
  private val p3 = participantIdFor(3)
  private val p4 = participantIdFor(4)
  private val p5 = participantIdFor(5)

  "PartyToParticipantComputations" when {
    "computeNewPermissions" should {
      "return an error when adds and remove overlap" in {
        computations
          .computeNewPermissions(
            SeqMap(p1 -> Submission),
            adds = List((p2, Submission)),
            removes = List(p2),
          )
          .left
          .value should include(
          "Permissions for the following participant were found in adds and removes:"
        )
      }

      "return an error when trying to remove a participant which is not permissioned" in {
        computations
          .computeNewPermissions(
            SeqMap(p1 -> Submission),
            removes = List(p2),
          )
          .left
          .value shouldBe s"Cannot remove permission for participants that are not permissioned: ${Set(p2)}"

        computations
          .computeNewPermissions(
            SeqMap(p1 -> Submission),
            removes = List(p1),
          )
          .value shouldBe empty
      }

      "allow to add and remove permissions" in {
        computations
          .computeNewPermissions(
            SeqMap(p1 -> Submission, p2 -> Observation),
            adds = List((p3, Confirmation)),
            removes = List(p1),
          )
          .value shouldBe Map(p2 -> Observation, p3 -> Confirmation)
      }

      "update existing added permissions" in {
        val updated = Map(p1 -> Confirmation)
        computations
          .computeNewPermissions(SeqMap(p1 -> Submission), adds = List((p1, Confirmation)))
          .value shouldBe updated
      }

      "not change the order of existing participants" in {
        val existing = SeqMap.from(Seq(p1, p2, p3, p4).map(_ -> Submission))
        computations
          .computeNewPermissions(existing, adds = List((p5, Submission)))
          .value
          .toSeq shouldBe existing.toSeq ++ Seq(p5 -> Submission)
      }
    }

    "computeNewParticipantStates with requiresPartyToBeOnboarded equals false" should {
      "return the existing states when there are no changes" in {
        val existing = SeqMap[ParticipantId, HostingState](p1 -> Active, p2 -> Onboarding)
        computations
          .computeNewParticipantStates(
            existing,
            specifiedStates = SeqMap.empty,
          )
          .value shouldBe existing
      }

      "update the state of an existing participant and keep the others unchanged" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Onboarding, p3 -> Freezing),
            specifiedStates = SeqMap(p2 -> Active),
          )
          .value shouldBe Map(p1 -> Active, p2 -> Active, p3 -> Freezing)
      }

      "remove participants" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Onboarding),
            specifiedStates = SeqMap.empty,
            removes = List(p1),
          )
          .value shouldBe Map(p2 -> Onboarding)

        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Onboarding),
            specifiedStates = SeqMap.empty,
            removes = List(p1, p2),
          )
          .value shouldBe empty
      }

      "allow to add, specify and remove participants at the same time" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Onboarding, p3 -> Active),
            specifiedStates = SeqMap(p2 -> Active, p4 -> Nominated),
            adds = List(p4),
            removes = List(p3),
          )
          .value shouldBe Map(p1 -> Active, p2 -> Active, p4 -> Nominated)
      }

      "include added participants without a specified state as active" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active),
            specifiedStates = SeqMap(p2 -> Nominated),
            adds = List(p2, p3),
          )
          .value shouldBe Map(p1 -> Active, p2 -> Nominated, p3 -> Active)
      }

      "keep the state of re-added Nominated and Freezing participants, and make others Active" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Nominated, p2 -> Freezing, p3 -> Onboarding, p4 -> Active),
            specifiedStates = SeqMap.empty,
            adds = List(p1, p2, p3, p4),
          )
          .value shouldBe Map(p1 -> Nominated, p2 -> Freezing, p3 -> Active, p4 -> Active)
      }

      "not change the order of existing participants" in {
        val existing = SeqMap.from(Seq(p1, p2, p3, p4).map(_ -> (Onboarding: HostingState)))
        computations
          .computeNewParticipantStates(
            existing,
            specifiedStates = SeqMap(p2 -> Active, p5 -> Nominated),
            adds = List(p5),
          )
          .value
          .toSeq shouldBe Seq(
          p1 -> Onboarding,
          p2 -> Active,
          p3 -> Onboarding,
          p4 -> Onboarding,
          p5 -> Nominated,
        )
      }
    }
    "computeNewParticipantStates with requiresPartyToBeOnboarded equals true" should {
      "return an error when an added participant has an existing state other than Onboarding" in {
        Seq[HostingState](Nominated, Freezing, Active).foreach { state =>
          computations
            .computeNewParticipantStates(
              SeqMap(p1 -> state),
              specifiedStates = SeqMap.empty,
              adds = List(p1),
              requiresPartyToBeOnboarded = true,
            )
            .left
            .value shouldBe "Participants required to onboard the party already have a " +
            s"hosting state other than Onboarding: ${Set(p1)}"

          computations
            .computeNewParticipantStates(
              SeqMap(p1 -> state),
              specifiedStates = SeqMap(p1 -> Onboarding),
              adds = List(p1),
              requiresPartyToBeOnboarded = true,
            )
            .left
            .value shouldBe "Participants required to onboard the party already have a " +
            s"hosting state other than Onboarding: ${Set(p1)}"
        }
      }

      "assign Onboarding to added participants without a specified state" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active),
            specifiedStates = SeqMap.empty,
            adds = List(p2, p3),
            requiresPartyToBeOnboarded = true,
          )
          .value shouldBe Map(p1 -> Active, p2 -> Onboarding, p3 -> Onboarding)
      }

      "accept Onboarding as specified state of added participants" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active),
            specifiedStates = SeqMap(p2 -> Onboarding),
            adds = List(p2),
            requiresPartyToBeOnboarded = true,
          )
          .value shouldBe Map(p1 -> Active, p2 -> Onboarding)
      }

      "allow re-adding a participant that is already Onboarding" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Onboarding),
            specifiedStates = SeqMap.empty,
            adds = List(p2),
            requiresPartyToBeOnboarded = true,
          )
          .value shouldBe Map(p1 -> Active, p2 -> Onboarding)
      }

      "not check the states of participants that are not added" in {
        computations
          .computeNewParticipantStates(
            SeqMap(p1 -> Active, p2 -> Freezing),
            specifiedStates = SeqMap(p2 -> Active, p3 -> Onboarding),
            adds = List(p3),
            requiresPartyToBeOnboarded = true,
          )
          .value shouldBe Map(p1 -> Active, p2 -> Active, p3 -> Onboarding)
      }

      "not change the order of existing participants" in {
        val existing = SeqMap.from(Seq(p1, p2, p3).map(_ -> (Active: HostingState)))
        computations
          .computeNewParticipantStates(
            existing,
            specifiedStates = SeqMap.empty,
            adds = List(p4, p5),
            requiresPartyToBeOnboarded = true,
          )
          .value
          .toSeq shouldBe Seq(
          p1 -> Active,
          p2 -> Active,
          p3 -> Active,
          p4 -> Onboarding,
          p5 -> Onboarding,
        )
      }
    }
  }
}
