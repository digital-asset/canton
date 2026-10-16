// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  freezeParty,
  unfreezeParty,
}
import com.digitalasset.canton.integration.util.{AutomaticAssignment, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a contract of Alice and Bob is unassigned towards `da`. While Alice is frozen on
  * `da`, Bob is removed from SP on `da`. The Ledger API Revoked event makes SP's reassignment store
  * for `da` check whether the contract still has a stakeholder hosted on SP. A freeze emits no
  * Ledger API topology events, so the check reads the freeze-ignoring hosting view. Otherwise it
  * would find no host of Alice and permanently stop listing the unassignment as incomplete.
  *
  * Test goal: show that after the unfreeze, SP still lists the unassignment as incomplete for
  * Alice, and that the contract can be assigned to `da`.
  *
  * Test setup:
  *   - 3 participants, 2 synchronizers `da` and `acme`. Automatic assignment is disabled.
  *   - participant1 and SP (participant2) host Alice and Bob on both synchronizers, so that
  *     removing Bob from SP leaves Bob a host. TP (participant3) is connected to `da` only.
  *   - The unassignment happens before the freeze, because `da` rejects it while Alice is frozen.
  *   - Freeze: Alice nominates TP, which moves itself to Freezing. Unfreeze: Alice removes TP.
  */
abstract class PartyFreezeIncompleteReassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.disableAutomaticAssignment,
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*

        Seq(participant1, participant2).foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.synchronizers.connect_local(sequencer2, alias = acmeName)
          participant.dars.upload(CantonTestsPath, synchronizerId = daId)
          participant.dars.upload(CantonTestsPath, synchronizerId = acmeId)
        }
        participant3.synchronizers.connect_local(sequencer1, alias = daName)

        AutomaticAssignment.disable(Seq(sequencer1), daId)

        val targetPermissions = (
          PositiveInt.one,
          Set(
            (participant1.id, ParticipantPermission.Submission: ParticipantPermission),
            (participant2.id, ParticipantPermission.Submission: ParticipantPermission),
          ),
        )
        val targetTopology = Map(daId -> targetPermissions, acmeId -> targetPermissions)
        val parties = PartiesAllocator(Set(participant1, participant2))(
          Seq(("alice", participant1), ("bob", participant1)),
          Map("alice" -> targetTopology, "bob" -> targetTopology),
        )
        alice = parties.find(_.toProtoPrimitive.contains("alice")).value
        bob = parties.find(_.toProtoPrimitive.contains("bob")).value
      }

  private var alice: Party = _
  private var bob: Party = _

  "An unassignment towards the synchronizer on which Alice is frozen" should {
    "stay incomplete for Alice when Bob is removed from SP during the freeze" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        val sp = participant2
        val iou = IouSyntax.createIou(participant1, Some(acmeId))(alice, bob)
        val unassigned = sp.ledger_api.commands.submit_unassign(
          alice,
          Seq(LfContractId.assertFromString(iou.id.contractId)),
          acmeId,
          daId,
        )
        sp.ledger_api.state.acs
          .incomplete_unassigned_of_party(alice)
          .loneElement
          .contractId shouldBe iou.id.contractId

        val observers = Seq(participant1, sp, participant3)
        freezeParty(alice, participant1, participant3, daId, observers)

        val bobRevokedOffset = clue("Remove Bob from SP on da") {
          sp.topology.party_to_participant_mappings.propose_delta(
            bob,
            removes = Seq(sp.id),
            store = daId,
          )
          val bobRevoked = sp.ledger_api.updates
            .topology_transactions(
              completeAfter = PositiveInt.one,
              partyIds = Seq(bob),
              beginOffsetExclusive = unassigned.events.loneElement.offset,
            )
            .loneElement
            .topologyTransaction
          val revoked = bobRevoked.events.loneElement.event.participantAuthorizationRevoked.value
          revoked.participantId shouldBe sp.id.toLf
          bobRevoked.offset
        }

        unfreezeParty(alice, participant1, participant3, daId, observers)

        // The offboarding check stops listing an unassignment only at offsets after the Revoked
        // event. The unfreeze adds no update, so a ping moves the ledger end past that offset.
        sp.health.ping(sp)
        val afterUnfreezeOffset = sp.ledger_api.state.end()
        afterUnfreezeOffset should be > bobRevokedOffset
        sp.ledger_api.state.acs
          .incomplete_unassigned_of_party(alice, activeAtOffsetO = Some(afterUnfreezeOffset))
          .loneElement
          .contractId shouldBe iou.id.contractId

        sp.ledger_api.commands.submit_assign(alice, unassigned.reassignmentId, acmeId, daId)
        sp.ledger_api.state.acs.incomplete_unassigned_of_party(alice) shouldBe empty
    }
  }
}

final class PartyFreezeIncompleteReassignmentIntegrationTestPostgres
    extends PartyFreezeIncompleteReassignmentIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

final class PartyFreezeIncompleteReassignmentIntegrationTestInMemory
    extends PartyFreezeIncompleteReassignmentIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
}
