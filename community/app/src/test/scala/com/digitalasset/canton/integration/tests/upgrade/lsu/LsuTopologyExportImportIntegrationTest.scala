// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.transaction.{LsuAnnouncement, TopologyMapping}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, TopologyManagerError}
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

/*
 * This test is used to test topology related aspects of LSU.
 */
final class LsuTopologyExportImportIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-topology"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override def configTransforms: Seq[ConfigTransform] =
    super.configTransforms ++ List(
      ConfigTransforms.disableAutoInit(Set("sequencer3", "mediator3"))
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S3M3_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "logical upgrade state cannot be queried if no upgrade is ongoing" in { implicit env =>
      assertThrowsAndLogsCommandFailures(
        env.sequencer1.topology.transactions.sequencer_lsu_state(),
        _.shouldBeCommandFailure(
          TopologyManagerError.NoLsuScheduled,
          "The operation cannot be performed because no LSU is scheduled",
        ),
      )
    }

    "the upgrade time must be sufficiently in the future" in { implicit env =>
      import env.*

      synchronizerOwners1.foreach(owner =>
        assertThrowsAndLogsCommandFailures(
          owner.topology.lsu.announcement
            .propose(
              daId.copy(serial = NonNegativeInt.one),
              CantonTimestamp.Epoch.minusSeconds(10),
            ),
          _.shouldBeCommandFailure(TopologyManagerError.InvalidUpgradeTime),
        )
      )
    }

    "work end-to-end" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()
      val newPSId = fixture.newPSId
      val newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters

      participant1.health.ping(participant1)

      // perform the synchronizer node upgrade.
      // this also announces the sequencer successors, which the participants will use
      // as the opportunity to initialize the successor synchronizer's topology ahead of the upgrade time.
      // the assertion below verifies that the topology state was indeed copied locally.
      // unfortunately, the least effort way of doing this is to assert on the log message
      // emitted by the local copy process.
      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.forLogger[DbTopologyStore[?]] && SuppressionRule.Level(Level.INFO)
      )(
        performSynchronizerNodesLsu(fixture),
        entries => {
          // all participants must log that the state was copied locally
          forAll(participants.all)(participant =>
            forExactly(1, entries) { msg =>
              msg.infoMessage should include regex (raw"Transferred \d+ topology transactions from ${fixture.currentPSId} to ${fixture.newPSId}".r)
              msg.loggerName should include(s"participant=${participant.name}")
            }
          )
        },
      )

      // validate the successor sequencer's topology state

      // We keep the announcement to check its presence on the new synchronizer
      val oldSynchronizerAnnouncement = sequencer1.topology.lsu.announcement.list().map(_.item)
      oldSynchronizerAnnouncement should have size 1

      sequencer2.synchronizer_parameters.static.get() shouldBe newStaticSynchronizerParameters

      clue("New synchronizer should filter out proposals") {
        val allProposals = sequencer2.topology.transactions.list(
          newPSId,
          proposals = true,
          timeQuery = TimeQuery.Range(None, None),
        )
        forAll(allProposals.result)(proposal => proposal.validUntil shouldBe empty)
      }

      clue(
        "New synchronizer should filter out LSU topology mappings, except for the announcement"
      ) {
        val allLsuMappings = sequencer2.topology.transactions.list(
          newPSId,
          filterMappings = TopologyMapping.Code.lsuMappings.toSeq,
          timeQuery = TimeQuery.Range(None, None),
        )
        allLsuMappings.result.map(_.mapping.code).toSet shouldBe Set(LsuAnnouncement.code)
        allLsuMappings.result.map(
          _.selectMapping[LsuAnnouncement].value.mapping
        ) shouldBe oldSynchronizerAnnouncement
        sequencer2.topology.lsu.announcement.list().map(_.item) shouldBe oldSynchronizerAnnouncement
      }

      // fetch the upgrade state from the predecessor sequencer
      val upgradeStateFromPredecessorSequencer =
        sequencer1.topology.transactions.sequencer_lsu_state()

      // fetch the participant's topology export for a later comparison
      def topologyStateThatShouldShouldSurviveTheUpgrade(psid: PhysicalSynchronizerId) =
        participant1.topology.transactions.list(
          psid,
          filterMappings = TopologyMapping.Code.mappingsIncludedInUpgrade.toSeq,
          timeQuery = TimeQuery.Range(None, None),
        )

      // we fetch both:
      // - the topology state (excluding what will be filtered in the upgrade state) via export endpoint (to compare before/after)
      // - the upgrade state before the upgrade from the participant via the upgrade endpoint (to compare with sequencer's upgrade state)

      val firstUpgradeState = topologyStateThatShouldShouldSurviveTheUpgrade(fixture.currentPSId)
      val firstUpgradeStateLsuEndpoint =
        participant1.topology.transactions.sequencer_lsu_state(synchronizer1Id)

      // advance the time past the upgrade time, so that the participant connects to the new physical synchronizer
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      // fetching the topology state after the upgrade should contain exactly the same state
      // as the upgrade state before the migration
      val secondUpgradeState = topologyStateThatShouldShouldSurviveTheUpgrade(fixture.newPSId)

      // compare the predecessor sequencer's upgrade state with the participant's predecessor upgrade state.
      // this shows that both the predecessor sequencer and the participant export the same upgrade state for fixture.currentPSId
      upgradeStateFromPredecessorSequencer shouldBe firstUpgradeStateLsuEndpoint

      // since no topology changes were made, they should be the same, from which follows that it must also be the same the sequencer's, and therefore the local copy was correct.
      firstUpgradeState shouldBe secondUpgradeState

      sequencer1.stop()
      mediator1.stop()
    }

    "prevent startup of misconfigured upgrade successor sequencers" in { implicit env =>
      import env.*

      val currentPSId =
        env.daId.copy(protocolVersion = ProtocolVersion.dev, serial = NonNegativeInt.one)

      val badFixture = Fixture(
        currentPSId = currentPSId,
        upgradeTime = upgradeTime.plusSeconds(3600),
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3)),
        newOldNodesResolution = Map("sequencer3" -> "sequencer2", "mediator3" -> "mediator2"),
        oldSynchronizerOwners = Set[InstanceReference](sequencer2, mediator2),
        newPV = ProtocolVersion.dev,
        newSerial = currentPSId.serial.increment.toNonNegative,
        overridePSId = Some(currentPSId.copy(serial = NonNegativeInt.tryCreate(3))),
      )

      // Attempt an LSU upgrade with a wrong PSId (::dev-2), which is not the one announced (::dev-3).
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        performSynchronizerNodesLsu(badFixture),
        log => {
          log.shouldBeCantonErrorCode(TopologyManagerError.InconsistentTopologySnapshot)
          log.message should include("not matching the announced upgrade successor")
        },
      )
    }
  }
}
