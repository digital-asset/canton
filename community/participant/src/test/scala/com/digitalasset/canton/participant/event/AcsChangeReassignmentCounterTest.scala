// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import com.daml.ledger.api.v2.reassignment.{Reassignment, ReassignmentEvent, UnassignedEvent}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.platform.index.AcsChangesReader
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{BaseTest, LfPartyId, ReassignmentCounter}
import org.scalatest.wordspec.AnyWordSpec

class AcsChangeReassignmentCounterTest extends AnyWordSpec with BaseTest {

  private lazy val alice = LfPartyId.assertFromString("Alice::1")
  private lazy val bob = LfPartyId.assertFromString("Bob::2")
  private lazy val stakeholders = Set(alice, bob)

  private lazy val contractId = ExampleTransactionFactory.suffixedId(0, 0)

  private lazy val targetSynchronizerId = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("target::synchronizer"))
  )

  private lazy val activeCounter = ReassignmentCounter(4)
  private lazy val unassignmentCounter = ReassignmentCounter(5)

  "the deactivation derived from a commit set" should {
    "name the counter the contract was active with" in {
      val commitSet = CommitSet(
        archivals = Map.empty,
        creations = Map.empty,
        unassignments = Map(
          contractId -> CommitSet.UnassignmentCommit(
            targetSynchronizerId,
            stakeholders,
            unassignmentCounter,
          )
        ),
        assignments = Map.empty,
        reassignments = Nil,
        hostedOnboardingPartiesO = None,
      )

      val acsChange = AcsChangeSupport.fromCommitSet(commitSet).acsChange(Map.empty)

      acsChange.deactivations(contractId).reassignmentCounter shouldBe activeCounter
    }
  }

  "the deactivation derived from the ledger API event" should {
    "name the counter the contract was active with" in {
      val unassignedEvent = UnassignedEvent.defaultInstance
        .withContractId(contractId.coid)
        .withReassignmentCounter(unassignmentCounter.unwrap)
        .withWitnessParties(stakeholders.toSeq)

      val reassignment = Reassignment.defaultInstance.withEvents(
        Seq(ReassignmentEvent(ReassignmentEvent.Event.Unassigned(unassignedEvent)))
      )

      val acsChange = AcsChangesReader.acsChangeOf(reassignment)

      acsChange.deactivations(contractId).reassignmentCounter shouldBe activeCounter
    }
  }
}
