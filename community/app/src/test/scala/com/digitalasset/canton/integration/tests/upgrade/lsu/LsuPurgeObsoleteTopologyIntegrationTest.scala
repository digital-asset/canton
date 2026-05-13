// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.RequireTypes.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.participant.config.PurgeConfig
import monocle.macros.syntax.lens.*

import java.time.Duration

final class LsuPurgeObsoleteTopologyIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-purge-obsolete-topology"

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
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.lsu.purgeObsoleteTopology).replace(
            Some(
              PurgeConfig().copy(
                chunkSize = PositiveInt.tryCreate(1),
                cron = "/5 * * * * ?",
              )
            )
          )
        )
      )
      .withSetup(implicit env => defaultEnvironmentSetup())

  "Logical synchronizer upgrade" should {
    "should clean up the topology of the old psid, eventually" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      val oldTopologyStore = participant1.testing.state_inspection.syncPersistentStateManager
        .get(fixture.currentPsid)
        .value
        .topologyStore

      val alice = participant1.parties.enable("Alice")
      IouSyntax.createIou(participant1)(alice, alice).discard

      performSynchronizerNodesLsu(fixture)

      oldTopologyStore.dumpStoreContent().futureValueUS.result should not be empty

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      transferTraffic()

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
      }

      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

      eventually() {
        // The scheduler works on wall-clock, so no point advancing the simclock here.
        oldTopologyStore.dumpStoreContent().futureValueUS.result shouldBe empty
      }

      val newTopologyStore = participant1.testing.state_inspection.syncPersistentStateManager
        .get(fixture.newPsid)
        .value
        .topologyStore

      newTopologyStore.dumpStoreContent().futureValueUS.result should not be empty
    }
  }
}
