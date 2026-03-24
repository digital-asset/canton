// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.FlagSet
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  TestEnvironment,
}
import com.digitalasset.canton.topology.transaction.{HostingParticipant, ParticipantPermission}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{HasTempDirectory, TempFile}
import org.scalatest.Assertion

import java.time.Duration
import scala.annotation.nowarn

/** Base class for tests relating to the interaction of LSU and Offline Party Replication. */
@nowarn("msg=dead code")
abstract class LsuOfflinePartyReplicationIntegrationTest extends LsuBase with HasTempDirectory {
  override protected def testName: String = "lsu-offline-party-replication"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  protected lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)
  protected lazy val upgradeTime2: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(120)

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set("sequencer2", "sequencer3", "mediator2", "mediator3")

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 3,
        numMediators = 3,
      )
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participants.local.start()

        participants.all.synchronizers.connect(defaultSynchronizerConnectionConfig())

        participants.all.dars.upload(CantonExamplesPath)
        participant1.health.ping(participant2)

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        alice = participant1.parties.enable("Alice")
        bob = participant1.parties.enable("Bob")
      }

  protected var alice: PartyId = _
  protected var bob: PartyId = _

  protected val acsSnapshotFile: TempFile = tempDirectory.toTempFile("offpr_test_acs_snapshot.gz")

  protected def makeFixture1(implicit env: TestEnvironment): Fixture = Fixture(
    currentPsid = env.daId,
    upgradeTime = upgradeTime1,
    oldSynchronizerNodes = SynchronizerNodes(Seq(env.sequencer1), Seq(env.mediator1)),
    newSynchronizerNodes = SynchronizerNodes(Seq(env.sequencer2), Seq(env.mediator2)),
    newOldNodesResolution = Map("sequencer2" -> "sequencer1", "mediator2" -> "mediator1"),
    oldSynchronizerOwners = Set[InstanceReference](env.sequencer1, env.mediator1),
    newPV = ProtocolVersion.dev,
    newSerial = env.daId.serial.increment.toNonNegative,
  )

  protected def makeFixture2(fixture1: Fixture)(implicit env: TestEnvironment): Fixture = Fixture(
    currentPsid = fixture1.newPsid,
    upgradeTime = upgradeTime2,
    oldSynchronizerNodes = fixture1.newSynchronizerNodes,
    newSynchronizerNodes = SynchronizerNodes(Seq(env.sequencer3), Seq(env.mediator3)),
    newOldNodesResolution = Map("sequencer3" -> "sequencer2", "mediator3" -> "mediator2"),
    oldSynchronizerOwners = Set[InstanceReference](env.sequencer2, env.mediator2),
    newPV = testedProtocolVersion,
    newSerial = fixture1.newSerial.increment.toNonNegative,
  )

  protected def assertParticipantHostsParty(
      participant: ParticipantReference,
      party: PartyId,
      lsid: SynchronizerId,
      onboarding: Boolean,
  ): Assertion =
    participant.topology.party_to_participant_mappings
      .list(lsid, filterParty = party.filterString)
      .loneElement
      .item
      .participants should contain(
      HostingParticipant(participant.id, ParticipantPermission.Submission, onboarding = onboarding)
    )

  protected def performLsu(
      connectedParticipants: Seq[ParticipantReference],
      fixture: Fixture,
      upgradeTime: CantonTimestamp,
      stopOldSynchronizerNodes: Boolean = true,
  )(implicit env: FixtureParam): Unit = {
    import env.*
    fixture.newSynchronizerNodes.all.start()
    performSynchronizerNodesLsu(fixture)
    environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
    transferTraffic(
      fixture.oldSynchronizerNodes.sequencers,
      fixture.newSynchronizerNodes.sequencers,
      suppressLogs = true,
    )
    eventually() {
      environment.simClock.value.advance(Duration.ofSeconds(1))
      connectedParticipants.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
    }
    if (stopOldSynchronizerNodes) fixture.oldSynchronizerNodes.all.stop()
    waitForTargetTimeOnSequencer(
      fixture.newSynchronizerNodes.sequencers.loneElement,
      environment.clock.now,
      logger,
    )
  }

  protected def awaitClearOnboardingFlag(
      party: PartyId,
      participant: ParticipantReference,
      lsid: SynchronizerId,
      offsetAfterImport: Long,
  )(env: FixtureParam): Unit = {
    val flagStatus = participant.parties.clear_party_onboarding_flag(party, lsid, offsetAfterImport)
    val clearAtTs = inside(flagStatus) { case FlagSet(ts) => ts }
    env.environment.simClock.value.advanceTo(clearAtTs.plusSeconds(1))
  }
}

/** Test that offline party replication can be performed after LSU */
final class LsuOffPRFirstLsuThenOffPR extends LsuOfflinePartyReplicationIntegrationTest {

  "Logical synchronizer upgrade" should {
    "allow offline party replication after the LSU is complete" in { implicit env =>
      import env.*

      val fixture = makeFixture1
      val iou = IouSyntax.createIou(participant1)(alice, bob)

      performLsu(participants.all, fixture, upgradeTime1)

      withClue("perform offline party replication") {
        val offsetBeforePartyUpdate = participant1.ledger_api.state.end()

        // Authorize alice on participant2, with onboarding flag set.
        participant2.topology.party_to_participant_mappings.propose_delta(
          party = alice.partyId,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = fixture.newPsid,
          requiresPartyToBeOnboarded = true,
        )
        participant2.synchronizers.disconnect_all()
        alice.topology.party_to_participant_mappings.propose_delta(
          participant1,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = fixture.newPsid,
          requiresPartyToBeOnboarded = true,
        )

        // Physically export and import
        participant1.parties.export_party_acs(
          party = alice,
          synchronizerId = fixture.newPsid,
          targetParticipantId = participant2.id,
          beginOffsetExclusive = offsetBeforePartyUpdate,
          exportFilePath = acsSnapshotFile.path.toString,
        )
        participant2.parties.import_party_acsV2(
          fixture.newPsid,
          Some(alice),
          acsSnapshotFile.path.toString,
        )
        val offsetAfterImport = participant2.ledger_api.state.end()

        participant2.synchronizers.reconnect(daName)
        eventually() {
          participant2.topology.party_to_participant_mappings
            .list(fixture.newPsid, filterParty = alice.filterString)
            .loneElement
            .item
            .participants should contain(
            HostingParticipant(participant2.id, ParticipantPermission.Submission, onboarding = true)
          )
        }

        awaitClearOnboardingFlag(alice, participant2, fixture.newPsid.logical, offsetAfterImport)
      }

      withClue("Alice can see the contract on participant2") {
        participant2.ledger_api.javaapi.state.acs
          .await(IouSyntax.modelCompanion)(alice)
          .id shouldBe iou.id
      }
    }
  }
}

/* Test that offline party replication can be interleaved with LSU like so:
 *   a. Target authorizes and disconnects
 *   b. perform LSU
 *   c. Source authorizes
 *   d. ACS snapshot taken on source
 *   e. ACS snapshot is imported on target
 *   f. Target reconnects and performs the LSU
 *   g. Perform another LSU
 */
final class LsuOffPRInterleavedLsuBeforeSourceAuthorizesOffPR
    extends LsuOfflinePartyReplicationIntegrationTest {
  "Logical synchronizer upgrade" should {
    "allow offline party replication to be interleaved - LSU before source auth" in {
      implicit env =>
        import env.*

        val fixture = makeFixture1
        val lsid = fixture.currentPsid.logical

        IouSyntax.createIou(participant1)(alice, bob)

        withClue("Target authorizes and disconnects") {
          participant2.topology.party_to_participant_mappings.propose_delta(
            party = alice.partyId,
            adds = Seq(participant2.id -> ParticipantPermission.Submission),
            store = lsid,
            requiresPartyToBeOnboarded = true,
          )
          participant2.synchronizers.disconnect_all()
        }

        performLsu(Seq(participant1), fixture, upgradeTime1, stopOldSynchronizerNodes = false)

        val offsetBeforeSetSourceOnboarding = withClue("Source authorizes") {
          val offset = participant1.ledger_api.state.end()
          alice.topology.party_to_participant_mappings.propose_delta(
            participant1,
            adds = Seq(participant2.id -> ParticipantPermission.Submission),
            store = lsid,
            requiresPartyToBeOnboarded = true,
          )
          offset
        }

        withClue("ACS snapshot taken on source") {
          participant1.parties.export_party_acs(
            party = alice,
            synchronizerId = lsid,
            targetParticipantId = participant2.id,
            beginOffsetExclusive = offsetBeforeSetSourceOnboarding,
            exportFilePath = acsSnapshotFile.path.toString,
          )
        }

        val offsetAfterTargetImport = withClue("ACS snapshot is imported on target") {
          participant2.parties.import_party_acsV2(lsid, Some(alice), acsSnapshotFile.path.toString)
          participant2.ledger_api.state.end()
        }

        withClue("Target reconnects and clears onboarding flag") {
          participant2.synchronizers.reconnect(daName)
          eventually() {
            assertParticipantHostsParty(participant2, alice, lsid, onboarding = true)
          }

          participant1.health.ping(participant2) // Trigger synchronization

          awaitClearOnboardingFlag(alice, participant2, lsid, offsetAfterTargetImport)(env)

          eventually() {
            assertParticipantHostsParty(participant2, alice, lsid, onboarding = false)
          }
        }

        withClue("Perform another LSU") {
          performLsu(participants.all, makeFixture2(fixture), upgradeTime2)
        }
    }
  }
}

/* Test that offline party replication can be interleaved with LSU like so:
 *   a. Target authorizes and disconnects
 *   b. Source authorizes
 *   c. perform LSU
 *   d. ACS snapshot taken on source
 *   e. ACS snapshot is imported on target
 *   f. Target reconnects and performs the LSU
 *   g. Perform another LSU
 *
 * This is like the test above, but b and c are swapped.
 */
final class LsuOffPRInterleavedLsuAfterSourceAuthorizesOffPR
    extends LsuOfflinePartyReplicationIntegrationTest {
  "Logical synchronizer upgrade" should {
    "allow offline party replication to be interleaved - LSU after source auth" in { implicit env =>
      import env.*

      val fixture = makeFixture1
      val lsid = fixture.currentPsid.logical

      IouSyntax.createIou(participant1)(alice, bob)

      withClue("Target authorizes and disconnects") {
        participant2.topology.party_to_participant_mappings.propose_delta(
          party = alice.partyId,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = lsid,
          requiresPartyToBeOnboarded = true,
        )
        participant2.synchronizers.disconnect_all()
      }

      val offsetBeforeSetSourceOnboarding = withClue("Source authorizes") {
        val offset = participant1.ledger_api.state.end()
        alice.topology.party_to_participant_mappings.propose_delta(
          participant1,
          adds = Seq(participant2.id -> ParticipantPermission.Submission),
          store = lsid,
          requiresPartyToBeOnboarded = true,
        )
        offset
      }

      withClue("perform LSU") {
        performLsu(Seq(participant1), fixture, upgradeTime1, stopOldSynchronizerNodes = false)
      }

      withClue("ACS snapshot taken on source") {
        participant1.parties.export_party_acs(
          party = alice,
          synchronizerId = lsid,
          targetParticipantId = participant2.id,
          beginOffsetExclusive = offsetBeforeSetSourceOnboarding,
          exportFilePath = acsSnapshotFile.path.toString,
        )
      }

      val offsetAfterTargetImport = withClue("ACS snapshot is imported on target") {
        // TODO(#29427) - Address ongoing synchronizer upgrade "no more topology transaction after freeze"
        // Switch back to import_party_acs(V2) from repair.import_acs
        participant2.repair.import_acsV2(lsid, acsSnapshotFile.path.toString)
        participant2.ledger_api.state.end()
      }

      withClue("f. Target reconnects and clears onboarding flag") {
        participant2.synchronizers.reconnect(daName)
        eventually() {
          assertParticipantHostsParty(participant2, alice, lsid, onboarding = true)
        }

        participant1.health.ping(participant2) // Trigger synchronization

        awaitClearOnboardingFlag(alice, participant2, lsid, offsetAfterTargetImport)(env)

        eventually() {
          assertParticipantHostsParty(participant2, alice, lsid, onboarding = false)
        }
      }

      withClue("Perform another LSU") {
        performLsu(participants.all, makeFixture2(fixture), upgradeTime2)
      }
    }
  }
}
