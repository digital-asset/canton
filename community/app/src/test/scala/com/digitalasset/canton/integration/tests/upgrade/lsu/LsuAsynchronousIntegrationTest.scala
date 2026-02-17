// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer

import java.util.UUID
import scala.concurrent.Future

/** This test shows that LSU is asynchronous: while all the participant have to upgrade at the same
  * synchronizer time, they can do it at different wall clock times.
  */
final class LsuAsynchronousIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-asynchronous"

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

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "work asynchronously" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      val alice = participant1.parties.enable("Alice")
      val bob = participant1.parties.enable("Bob")
      val bank = participant2.parties.enable("Bank")
      val aliceIou = IouSyntax.createIou(participant2)(bank, alice)

      // P2 is disconnected, it will perform the LSU later
      participant2.synchronizers.disconnect_all()
      performSynchronizerNodesLsu(fixture)

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      eventually() {
        participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true
        participant2.synchronizers.is_connected(fixture.newPSId) shouldBe false

        participants.all.forall(_.synchronizers.is_connected(fixture.currentPSId)) shouldBe false
      }

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)

      val commandId = UUID.randomUUID().toString
      val transferIouF = Future {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(aliceIou.id.exerciseTransfer(bob.toProtoPrimitive).commands().loneElement),
          commandId = commandId,
        )
      }

      // Wait for the request to be in-flight
      eventually() {
        participant1.underlying.value.sync
          .connectedSynchronizerForAlias(daName)
          .value
          .numberOfDirtyRequests() shouldBe 1
      }

      // When P2 reconnects...
      participant2.synchronizers.reconnect_all()

      // It performs the LSU...
      eventually() {
        participant2.synchronizers.is_connected(fixture.newPSId) shouldBe true
      }

      oldSynchronizerNodes.all.stop()

      // And can confirm the transaction
      transferIouF.futureValue.getCommandId shouldBe commandId
    }
  }
}
