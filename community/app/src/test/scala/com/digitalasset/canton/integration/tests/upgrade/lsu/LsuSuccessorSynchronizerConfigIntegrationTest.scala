// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import monocle.syntax.all.*

import java.time.Duration

/** This test is used to test the the propagation of synchronizer connection config changes from the
  * predecessor to the successor.
  *
  * It performs a change in the config (amplification settings) of the current synchronizer after
  * the successor config was registered and ensures that this change is eventually propagated to the
  * config of the successor.
  *
  * It uses 1 participant, 2 sequencers and 2 mediators.
  */
final class LsuSuccessorSynchronizerConfigIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-successor-synchronizer-config"

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
  override protected lazy val upgradeTime: CantonTimestamp = if (useStaticTime) {
    CantonTimestamp.Epoch.plusSeconds(30)
  } else {
    CantonTimestamp.now().plusSeconds(5)
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Changes to the predecessor synchronizer connection config" should {
    "be propagated to the successor config" in { implicit env =>
      import env.*

      participant1.health.ping(participant1)

      val fixture = fixtureWithDefaults()

      val oldPatience = getAmplificationPatience(fixture.currentPsid)
      val newPatience = oldPatience.plusSeconds(1)

      performSynchronizerNodesLsu(fixture)

      // directly changing the configuration of the successor is not allowed
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.modify(
          daName,
          _.focus(_.sequencerConnections.submissionRequestAmplification.patience)
            .replace(newPatience),
          physicalSynchronizerId = Some(fixture.newPsid),
        ),
        _.errorMessage should include(
          s"No active synchronizer $daName configured with physical synchronizer id ${fixture.newPsid}"
        ),
      )

      // initially, the successor has the same patience config as the predecessor
      eventually() {
        getAmplificationPatience(fixture.newPsid) shouldBe oldPatience
      }

      // modify the config of the predecessor
      participant1.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections.submissionRequestAmplification.patience).replace(newPatience),
      )
      // verify that the amplification patience for the predecessor synchronizer has changed
      getAmplificationPatience(fixture.currentPsid) shouldBe newPatience
      // and that the change has also been propagated to the successor
      getAmplificationPatience(fixture.newPsid) shouldBe newPatience

      environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))
      transferTraffic()

      eventually() {
        environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPsid)) shouldBe false
      }

      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

      // verify again that the patience config is as expected on the now active successor
      getAmplificationPatience(fixture.newPsid) shouldBe newPatience

      participant1.health.ping(participant1)
    }
  }

  /** Uses the synchronizer connection config store directly, because the console commands only
    * allow to work with active synchronizers.
    */
  private def getAmplificationPatience(
      psid: PhysicalSynchronizerId
  )(implicit env: TestConsoleEnvironment) =
    env.participant1.underlying.value.sync.synchronizerConnectionConfigStore
      .get(psid)
      .value
      .config
      .sequencerConnections
      .submissionRequestAmplification
      .patience
      .toConfig
}
