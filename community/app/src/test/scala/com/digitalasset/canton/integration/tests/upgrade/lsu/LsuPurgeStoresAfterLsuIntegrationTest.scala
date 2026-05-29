// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
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

/** Ensure physical stores are purged after LSU:
  *   - topology store
  *   - submission tracker store
  */
final class LsuPurgeStoresAfterLsuIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-purge-stores"

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
        /*
        The submission tracker store is garbage collected to we need to increase the retention
        period to prevent the data from being cleaned before the LSU.
         */
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(config.NonNegativeFiniteDuration.ofDays(1))
        )
      )
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.lsu.purgeObsoleteTopology).replace(
            Some(
              PurgeConfig().copy(
                chunkSize = PositiveInt.tryCreate(2),
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
      IouSyntax.createIou(participant1)(alice, alice, 1.0).discard

      performSynchronizerNodesLsu(fixture)

      clue("old stores are not empty") {
        oldTopologyStore.dumpStoreContent().futureValueUS.result should not be empty

        participant1.underlying.value.sync.syncPersistentStateManager
          .get(fixture.currentPsid)
          .value
          .submissionTrackerStore
          .size
          .futureValueUS shouldBe 1
      }

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      transferTraffic()

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
      }

      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

      IouSyntax.createIou(participant1)(alice, alice, 2.0).discard

      // The scheduler works on wall-clock, so no point advancing the simclock here.
      eventually() {
        oldTopologyStore.dumpStoreContent().futureValueUS.result shouldBe empty

        participant1.underlying.value.sync.syncPersistentStateManager
          .get(fixture.currentPsid)
          .value
          .submissionTrackerStore
          .size
          .futureValueUS shouldBe 0
      }

      val newTopologyStore = participant1.testing.state_inspection.syncPersistentStateManager
        .get(fixture.newPsid)
        .value
        .topologyStore

      clue("new stores are not empty") {
        newTopologyStore.dumpStoreContent().futureValueUS.result should not be empty

        participant1.underlying.value.sync.syncPersistentStateManager
          .get(fixture.newPsid)
          .value
          .submissionTrackerStore
          .size
          .futureValueUS shouldBe 1
      }
    }
  }
}
