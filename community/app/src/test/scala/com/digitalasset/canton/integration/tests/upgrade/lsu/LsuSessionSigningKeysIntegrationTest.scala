// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
}

import java.time.Duration

final class LsuSessionSigningKeysIntegrationTest extends LsuBase {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected def testName: String = "lsu-session-signing-keys"
  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override protected def configTransforms: Seq[ConfigTransform] =
    super.configTransforms :+ ConfigTransforms.setSessionSigningKeys(
      SessionSigningKeysConfig.default
    )
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup(implicit env => defaultEnvironmentSetup())

  "Logical synchronizer upgrade" should {
    "work with session signing keys" in { implicit env =>
      import env.*

      Seq[LocalInstanceReference](participant1, sequencer1, mediator1).foreach { node =>
        withClue(s"${node.id} should have session signing keys enabled before LSU") {
          node.config.crypto.sessionSigningKeys.enabled shouldBe true
        }
      }

      val alice = participant1.parties.enable("Alice")
      val bank = participant1.parties.enable("Bank")
      val fixture = fixtureWithDefaults()

      withClue("perform LSU") {
        performSynchronizerNodesLsu(fixture)
        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        }
        oldSynchronizerNodes.all.stop()
        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
      }

      // Verify we can still successfully execute transactions.
      IouSyntax.createIou(participant1)(bank, alice)

      Seq[LocalInstanceReference](participant1, sequencer2, mediator2).foreach { node =>
        withClue(s"${node.id} should have session signing keys enabled after LSU") {
          node.config.crypto.sessionSigningKeys.enabled shouldBe true
        }
      }
    }
  }
}
