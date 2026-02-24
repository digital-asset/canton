// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes

import java.time.Duration

/** This test ensures that traffic transfers works for LSU even if sequencer nodes are restarted.
  *
  * Topology:
  *   - P1 connected to S1
  *   - P2 connected to S2
  *
  * This test:
  *   - Generates purchased/consumed traffic by performing some activity on the predecessor
  *   - Performs an LSU (S1->S3, S2->S4)
  *   - S3 is restarted before traffic is set
  *   - S4 is restarted after traffic is set and before any subscription
  *
  * Instances:
  *   - Old synchronizer: sequencer1, sequencer2
  *   - New synchronizer: sequencer3, sequencer4
  */
abstract class LsuTrafficTransferRestartIntegrationTest
    extends LsuBase
    with LsuTrafficManagement
    with TrafficBalanceSupport {

  override protected def testName: String = "lsu-traffic-transfer-crash-recovery"

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(
          S2M1(synchronizerOwnersOverride =
            Some(Seq[InstanceReference](env.sequencer1, env.mediator1))
          )
        )
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup(connectParticipants = false)
        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))
        participant2.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))

        participants.all.dars.upload(CantonExamplesPath)

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator2))

        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            // Enable traffic control
            trafficControl = Some(trafficControlParameters),
            // "Deactivate" ACS commitments to not consume traffic in the background
            reconciliationInterval = config.PositiveDurationSeconds.ofDays(365),
          ),
          mustFullyAuthorize = true,
        )

        // Fill up base rate
        environment.simClock.value.advance(Duration.ofSeconds(1))
      }

  "Traffic transfer during an LSU" should {

    "create and assert non-empty traffic state on the sequencer" in { implicit env =>
      import env.*

      val participantTopUpAmount = PositiveLong.tryCreate(500_000L)
      val mediatorTopUpAmount = PositiveLong.tryCreate(250_000L)

      initialTrafficPurchase(
        Map(
          participant1 -> participantTopUpAmount,
          mediator1 -> mediatorTopUpAmount,
        ),
        sequencer1,
      )

      // The top-up only becomes active on the next sequencing timestamp.
      // Sufficiently many pings to consume extra traffic are made.
      (1 to 4).foreach(_ => participant1.health.ping(participant1.id))

      clue("check nodes traffic state on sequencer1") {
        eventually() {
          nodeSeesExtraTrafficPurchased(
            sequencer1,
            Map(
              participant1.id -> participantTopUpAmount.value,
              mediator1.id -> mediatorTopUpAmount.value,
            ),
          )
        }
      }

      val trafficState = participant1.traffic_control.traffic_state(daId)
      trafficState.extraTrafficPurchased.value shouldBe 500000L
      trafficState.extraTrafficRemainder should be < 500000L
      trafficState.baseTrafficRemainder.value should be < (maxBaseTrafficAmount)
      trafficState.serial shouldBe Some(PositiveInt.tryCreate(1))
    }

    "perform an LSU and ensure traffic setting works with restarts" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant1)

      performSynchronizerNodesLsu(fixture)
      mediator2.stop()

      // Generate some non-trivial base traffic remainder state
      environment.simClock.value.advanceTo(upgradeTime.minusMillis(20L))
      participant1.health.ping(participant2)

      // Since p2 and m2 are stopped, we ensure that the restart of s4 happens before any subscription
      participant2.stop()

      /*
      Progressing the clock past upgrade time triggers LSU for p1 (p2 is disconnected).
      P1 fails to connect to the successor because traffic is not set yet on s3.
       */
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      val oldTrafficState = eventuallyGetTraffic(sequencer1)

      sequencer3.stop()
      sequencer3.start()

      sequencer3.traffic_control.set_lsu_state(oldTrafficState)
      sequencer4.traffic_control.set_lsu_state(oldTrafficState)

      sequencer4.stop()
      sequencer4.start()

      participant2.start()
      participant2.synchronizers.reconnect_all()

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
      }

      mediator2.start()
      participant1.health.ping(participant2)
    }
  }
}

final class LsuReferenceTrafficTransferRestartIntegrationTest
    extends LsuTrafficTransferRestartIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
}

// TODO(#16789) Re-enable test once dynamic onboarding (traffic control) is supported for DA BFT
//final class LsuBftOrderingTrafficTransferRestartTest
//  extends LsuTrafficTransferRestartIntegrationTest {
//    registerPlugin(
//      new UseBftSequencer(
//        loggerFactory,
//        MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
//      )
//    )
//}
