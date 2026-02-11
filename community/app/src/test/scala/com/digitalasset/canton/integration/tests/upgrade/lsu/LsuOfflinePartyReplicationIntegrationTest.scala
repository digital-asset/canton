// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}

/* This class tests the interaction of LSU and Offline Party Replication.
 *
 * Scenarios are:
 * - OffPR can be performed after LSU
 * - OffPR can be performed across LSU # TODO(i29752): implement
 *   i. Target authorizes and disconnects
 *   ii. LSU done
 *   iii. Source authorizes
 *   iv. ACS snapshot taken on source
 *   v. ACS snapshot is imported on target
 *   vi. Source reconnects and performs LSU
 * - Same as above but ii and iii are swapped. # TODO(i29752): implement
 */
final class LsuOfflinePartyReplicationIntegrationTest extends LsuBase with HasTempDirectory {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def testName: String = "lsu-offline-party-replication"

  override protected lazy val newOldSequencers = Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators = Map("mediator2" -> "mediator1")

  override protected def upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup(implicit env => defaultEnvironmentSetup())

  private val acsSnapshotFile =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  "Logical Synchronizer Upgrade" should {
    "allow Offline Party Replication after the LSU is complete" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bob = participant1.parties.enable("Bob")
      val iou = IouSyntax.createIou(participant1)(alice, bob)

      withClue("First, perform LSU") {
        performSynchronizerNodesLsu(fixture)
        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        eventually() {
          participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        }
        oldSynchronizerNodes.all.stop()
        TestUtils.waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
      }

      withClue("Alice can see the contract on participant1") {
        participant1.ledger_api.javaapi.state.acs
          .await(IouSyntax.modelCompanion)(alice)
          .id shouldBe iou.id
      }

      withClue("Second, perform Offline Party Replication") {
        // Authorize alice on participant2, with onboarding flag set.
        participant2.topology.party_to_participant_mappings.propose_delta(
          party = alice.partyId,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = fixture.newPSId,
          requiresPartyToBeOnboarded = true,
        )
        participant2.synchronizers.disconnect_all()
        alice.topology.party_to_participant_mappings.propose_delta(
          participant1,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = fixture.newPSId,
          requiresPartyToBeOnboarded = true,
        )
        val beforeExportOffset = participant1.ledger_api.state.end()

        // Physically export and import
        participant1.parties.export_party_acs(
          party = alice,
          synchronizerId = fixture.newPSId,
          targetParticipantId = participant2.id,
          beginOffsetExclusive = beforeExportOffset,
          exportFilePath = acsSnapshotFile.path.toString,
        )
        participant2.parties.import_party_acsV2(acsSnapshotFile.path.toString, fixture.newPSId)

        participant2.synchronizers.reconnect(daName)
        eventually() {
          participant2.topology.party_to_participant_mappings
            .list(fixture.newPSId, filterParty = alice.filterString)
            .loneElement
            .item
            .participants should contain(
            HostingParticipant(participant2.id, ParticipantPermission.Submission, onboarding = true)
          )
        }

        // Clear onboarding flag
        participant2.topology.party_to_participant_mappings
          .propose_delta(
            party = alice.partyId,
            adds = Seq(participant2.id -> ParticipantPermission.Submission),
            store = fixture.newPSId,
            requiresPartyToBeOnboarded = false,
          )
      }

      withClue("Alice can see the contract on participant2") {
        participant2.ledger_api.javaapi.state.acs
          .await(IouSyntax.modelCompanion)(alice)
          .id shouldBe iou.id
      }
    }
  }
}
