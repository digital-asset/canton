// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer

import java.time.Duration
import scala.concurrent.duration.*

/** This test serves as a reproducer for https://github.com/DACH-NY/canton/issues/32998. If the
  * participant disconnects/reconnects after the LSU announcement is received and before any other
  * topology transaction, then the LSU announcement will be missed.
  */
final class LsuMissedLsuAnnouncementIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-missed-announcement"

  override protected val useStaticTime: Boolean = false

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp =
    CantonTimestamp.now().plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup(hasTrafficControl = false)
      }

  "LSU announcement" should {
    "not be missed" when {
      "participant is restarted after processing the LSU announcement" in { implicit env =>
        import env.*

        participant1.health.ping(participant1)

        val fixture = fixtureWithDefaults()

        sequencer1.topology.lsu.announcement.propose(fixture.newPsid, fixture.upgradeTime)

        // wait until p1 receives the announcement
        eventually() {
          participant1.topology.lsu.announcement
            .list(daId)
            .loneElement
            .item
            .successor shouldBe fixture.synchronizerSuccessor
        }

        participant1.health.ping(participant1)

        participant1.synchronizers.disconnect_all()
        participant1.synchronizers.reconnect_all()

        clue("Migrate sequencer nodes") {
          eventually() {
            forAll(fixture.oldSynchronizerNodes.all)(
              _.topology.lsu.announcement
                .list(store = Some(fixture.currentPsid))
                .filter(_.item.successorSynchronizerId == fixture.newPsid)
                .loneElement
            )
          }

          migrateSynchronizerNodes(fixture)

          sequencer1.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer1.id,
            endpoints = sequencer2.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            successorSynchronizerId = fixture.newPsid,
          )
        }

        eventually(30.seconds) {
          environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
          participants.all.forall(_.synchronizers.is_connected(fixture.currentPsid)) shouldBe false
        }
        waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

        participant1.health.ping(participant1)
      }
    }
  }
}
