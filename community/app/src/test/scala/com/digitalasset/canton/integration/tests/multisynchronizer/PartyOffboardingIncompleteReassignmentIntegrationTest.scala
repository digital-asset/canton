// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.ParticipantPermission

/** Ensures that when the last locally hosted stakeholder is offboarded, then an incomplete
  * unassigned is not indicated as incomplete anymore
  *
  * Topology:
  *   - P1, P2 connected to both da and acme
  *   - P1 and P2 both hosting Alice and Bob
  */
abstract class PartyOffboardingIncompleteReassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1_S1M1
    .addConfigTransforms(
      ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
    )
    .withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
      participants.all.dars.upload(CantonTestsPath, synchronizerId = daId)
      participants.all.dars.upload(CantonTestsPath, synchronizerId = acmeId)

      // Disable automatic assignment
      sequencer2.topology.synchronizer_parameters.propose_update(
        acmeId,
        _.copy(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
      )

      val targetPermissions = (
        PositiveInt.one,
        Set(
          (participant1.id, ParticipantPermission.Submission: ParticipantPermission),
          (participant2.id, ParticipantPermission.Submission: ParticipantPermission),
        ),
      )

      val targetTopology = Map(
        daId -> targetPermissions,
        acmeId -> targetPermissions,
      )

      val parties = PartiesAllocator(Set(participant1, participant2))(
        Seq(("alice", participant1), ("bob", participant1)),
        Map(
          "alice" -> targetTopology,
          "bob" -> targetTopology,
        ),
      )

      alice = parties.find(_.toProtoPrimitive.contains("alice")).value
      bob = parties.find(_.toProtoPrimitive.contains("bob")).value
    }

  private var alice: Party = _
  private var bob: Party = _

  "Offboarding the latest locally hosted stakeholder" should {
    "mark incomplete unassigned as not incomplete anymore" in { implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(alice, bob)

      val unassignedOffset = participant2.ledger_api.commands
        .submit_unassign(
          alice,
          Seq(LfContractId.assertFromString(iou.id.contractId)),
          daId,
          acmeId,
        )
        .events
        .loneElement
        .offset

      val aliceOffboardingOffset = clue("offboard Alice") {
        participant2.ledger_api.state.acs
          .incomplete_unassigned_of_party(alice, activeAtOffsetO = Some(unassignedOffset))
          .loneElement
          .contractId shouldBe iou.id.contractId

        participant2.topology.party_to_participant_mappings
          .propose_delta(
            alice,
            removes = Seq(participant2),
            store = acmeId,
          )

        val aliceOffboarding = participant2.ledger_api.updates
          .topology_transactions(
            completeAfter = PositiveInt.one,
            partyIds = Seq(alice),
            beginOffsetExclusive = unassignedOffset,
          )
          .loneElement
          .topologyTransaction

        aliceOffboarding.events.loneElement.event.participantAuthorizationRevoked.value.partyId shouldBe alice.toProtoPrimitive
        aliceOffboarding.offset
      }

      // Reassignment still incomplete because Bob still hosted
      participant2.ledger_api.state.acs
        .incomplete_unassigned_of_party(alice, activeAtOffsetO = Some(aliceOffboardingOffset))
        .loneElement
        .contractId shouldBe iou.id.contractId

      val bobOffboardingOffset = clue("Bob offboarding") {
        participant2.topology.party_to_participant_mappings
          .propose_delta(
            bob,
            removes = Seq(participant2),
            store = acmeId,
          )

        val bobOffboarding = participant2.ledger_api.updates
          .topology_transactions(
            completeAfter = PositiveInt.one,
            partyIds = Seq(bob),
            beginOffsetExclusive = aliceOffboardingOffset,
          )
          .loneElement
          .topologyTransaction

        bobOffboarding.events.loneElement.event.participantAuthorizationRevoked.value.partyId shouldBe bob.toProtoPrimitive
        bobOffboarding.offset
      }

      participant2.ledger_api.state.acs
        .incomplete_unassigned_of_party(bob, activeAtOffsetO = Some(bobOffboardingOffset))
        .loneElement
        .contractId shouldBe iou.id.contractId

      // Ensure ledger end progresses past bobOffboardingOffset
      participant2.health.ping(participant2)
      eventually() {
        participant2.ledger_api.state.end() should be > bobOffboardingOffset
      }

      // Reassignment not incomplete anymore
      participant2.ledger_api.state.acs
        .incomplete_unassigned_of_party(
          bob,
          activeAtOffsetO = Some(bobOffboardingOffset + 1),
        ) shouldBe empty

      // Does not change the response if querying with past offsets
      participant2.ledger_api.state.acs
        .incomplete_unassigned_of_party(alice, activeAtOffsetO = Some(aliceOffboardingOffset))
        .loneElement
        .contractId shouldBe iou.id.contractId
    }
  }
}

final class PartyOffboardingIncompleteReassignmentIntegrationTestPostgres
    extends PartyOffboardingIncompleteReassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

final class PartyOffboardingIncompleteReassignmentIntegrationTestInMemory
    extends PartyOffboardingIncompleteReassignmentIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}
