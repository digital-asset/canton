// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.topology.PartyId
import monocle.macros.syntax.lens.*

/*
 * This test is used to test LSU works well when participants are restarted.
 */
final class LsuRestartIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-restart"

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

  private var iou1SignedByP2: Iou.Contract = _
  private var iou2SignedByP2: Iou.Contract = _
  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        /*
        Without that, an additional fetch_time is needed after the restart because the last known
        timestamp is considered as sufficiently recent.
         */
        participants.all.foreach(
          _.synchronizers.modify(
            daName,
            _.focus(_.timeTracker.observationLatency)
              .replace(config.NonNegativeFiniteDuration.ofMillis(0))
              .focus(_.timeTracker.patienceDuration)
              .replace(config.NonNegativeFiniteDuration.ofMillis(0)),
          )
        )

        participants.all.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        alice = participant1.parties.enable("Alice")
        bob = participant1.parties.enable("Bob")

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2))
      }

  "Logical synchronizer upgrade" should {
    "work when participants are restarted" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant1)
      iou1SignedByP2 = IouSyntax.createIou(participant2)(
        payer = participant2.adminParty,
        owner = participant1.adminParty,
      )
      iou2SignedByP2 = IouSyntax.createIou(participant2)(
        payer = participant2.adminParty,
        owner = participant1.adminParty,
      )

      performSynchronizerNodesLsu(fixture)

      participants.local.stop()

      participant1.start() // restarted before the upgrade time
      participant1.synchronizers.reconnect_all()

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      participant2.start() // restarted after the upgrade time
      participant2.synchronizers.reconnect_all()

      eventually() {
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }
      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)

    // Do not add anything here as the specific test case relies on "fresh" state for P1 and P2
    }

    /** Transfer an Iou across a restart:
      *   - P2 is taken offline
      *   - P1 submits the transfer
      *   - Transfer can reach phase 3 but P2 confirmation is missing
      *   - P1 is stopped
      *   - P1 and P2 are restarted and connected to the synchronizer
      *   - Transfer succeeds
      *
      * @param iou
      *   Signed by P2, owned by P1 admin party
      */
    def transferAcrossRestart(iou: Iou.Contract, newOwner: PartyId)(implicit
        env: TestConsoleEnvironment
    ): Iou.Contract = {
      import env.*

      // P2 is offline so that it does not confirm
      participant2.stop()

      // transfer to P1
      logger.info(s"Transferring ${iou.id.contractId} to $newOwner")
      participant1.ledger_api.javaapi.commands.submit_async(
        Seq(participant1.adminParty),
        Seq(
          iou.id
            .exerciseTransfer(newOwner.toProtoPrimitive)
            .commands()
            .loneElement
        ),
      )

      // Wait for the command to reach phase 3
      eventually() {
        participant1.underlying.value.sync
          .connectedSynchronizerForAlias(daName)
          .value
          .numberOfDirtyRequests() shouldBe 1
      }

      participant1.stop()

      participants.local.start()
      participants.all.synchronizers.reconnect_all()

      // Transfer to P1 should succeed
      participant1.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(
        newOwner,
        predicate = _.data.owner == newOwner.toProtoPrimitive,
      )
    }

    "restart after the upgrade time once a transaction is in-flight" in { implicit env =>
      // This checks that it works when no activity happened before on the physical synchronizer
      transferAcrossRestart(iou1SignedByP2, alice)

      // This checks that it works when activity happened before on the physical synchronizer
      transferAcrossRestart(iou2SignedByP2, bob)
    }
  }
}
