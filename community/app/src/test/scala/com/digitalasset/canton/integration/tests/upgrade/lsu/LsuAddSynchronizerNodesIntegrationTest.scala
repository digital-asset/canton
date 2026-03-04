// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{MediatorReference, SequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.topology.{ForceFlag, PhysicalSynchronizerId}

import java.time.Duration

/*
This class tests that we can successfully run an LSU in the case of a topology where synchronizer nodes are
added to the synchronizer after initialization. Steps are:
 * bootstrap with s1, m1
 * add s2, m2
 * do lsu
 * add s5, m5
 */
final class LsuAddSynchronizerNodesIntegrationTest extends LsuBase with OnboardsNewSequencerNode {

  override protected def testName: String = "lsu-add-synchronizer-nodes"

  override protected val bftSequencerPlugin: Option[UseBftSequencer] =
    Some(
      new UseBftSequencer(
        loggerFactory,
        MultiSynchronizer.tryCreate(
          Set("sequencer1", "sequencer2"),
          // s3 is the successor of s1, s4 is the successor of s2
          Set("sequencer3", "sequencer4", "sequencer5"),
        ),
      )
    )

  bftSequencerPlugin.foreach(registerPlugin)

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] =
    Map("mediator3" -> "mediator1", "mediator4" -> "mediator2")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S5M5_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect(defaultSynchronizerConnectionConfig())

        participants.all.dars.upload(CantonExamplesPath)

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)
      }

  private def addNewSV(
      psid: PhysicalSynchronizerId,
      newSequencer: SequencerReference,
      newMediator: MediatorReference,
      existingSequencer: SequencerReference,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    onboardNewSequencer(
      synchronizerId = psid,
      newSequencer = newSequencer,
      existingSequencer = existingSequencer,
      synchronizerOwners = Set(existingSequencer),
    )

    existingSequencer.topology.transactions.load(
      newMediator.topology.transactions.identity_transactions(),
      psid,
      ForceFlag.AlienMember,
    )

    // existingSequencer is one of the synchronizer owner
    existingSequencer.topology.mediators.propose_delta(
      psid,
      group = NonNegativeInt.zero,
      updateThreshold = Some(PositiveInt.two),
      adds = List(newMediator.id),
    )

    newMediator.setup.assign(
      synchronizerId = psid,
      sequencerConnections = newSequencer,
    )
  }

  "Logical synchronizer upgrade" should {
    "is compatible with adding new synchronizer nodes" in { implicit env =>
      import env.*

      val alice = participant1.parties.enable("Alice")
      val bank = participant1.parties.enable("Bank")

      IouSyntax.createIou(participant1)(bank, alice, amount = 1.0).discard

      addNewSV(
        psid = daId,
        newSequencer = sequencer2,
        newMediator = mediator2,
        existingSequencer = sequencer1,
      )

      participant1.health.ping(participant1)

      val sequencerConnectionsToS1S2 = SequencerConnections.tryMany(
        connections = Seq(sequencer1, sequencer2),
        sequencerTrustThreshold = PositiveInt.two,
        sequencerLivenessMargin = NonNegativeInt.zero,
        submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
        sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
      )
      participant1.synchronizers.modify(
        daName,
        _.copy(sequencerConnections = sequencerConnectionsToS1S2),
      )

      // disconnect and reconnect is needed for the new sequencer connections to be used
      participant1.synchronizers.disconnect_all()
      participant1.synchronizers.reconnect_all()

      IouSyntax.createIou(participant1)(bank, alice, amount = 2.0).discard

      oldSynchronizerNodes =
        SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2))
      newSynchronizerNodes =
        SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4))

      // LSU
      val fixture = fixtureWithDefaults()
      performSynchronizerNodesLsu(fixture)
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      transferTraffic()
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPSId)) shouldBe false
      }
      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer3, environment.clock.now, logger)

      IouSyntax.createIou(participant1)(bank, alice, amount = 3.0).discard

      addNewSV(
        psid = fixture.newPSId,
        newSequencer = sequencer5,
        newMediator = mediator5,
        existingSequencer = sequencer3,
      )

      participant1.health.ping(participant1)
    }
  }
}
