// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt, PositiveLong}
import com.digitalasset.canton.console.{InstanceReference, LocalSequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.TrafficBalanceSupport
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.synchronizer.sequencer.traffic.LsuTrafficState
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.time.Duration

/** This test ensures that the traffic purchased for a member that is not registered yet is
  * transferred by an LSU.
  *
  * Topology:
  *   - P1 connected to S1
  *   - P2 connected to S2
  *   - P3 is only connected after the LSU, to the new synchronizer
  *
  * This test:
  *   - Purchases traffic for P3 on the predecessor, before P3 is registered
  *   - Performs an LSU (S1->S3, S2->S4)
  *   - Onboards P3 on the successor and checks that it got the traffic purchased
  *
  * Instances:
  *   - Old synchronizer: sequencer1, sequencer2
  *   - New synchronizer: sequencer3, sequencer4
  */
final class LsuTrafficPrePurchaseIntegrationTest extends LsuBase with TrafficBalanceSupport {

  override protected def testName: String = "lsu-traffic-pre-purchase"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val prePurchasedAmount = NonNegativeLong.tryCreate(300_000L)
  private val prePurchaseSerial = PositiveInt.one

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S4M2_Config
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

        defaultEnvironmentSetup(
          connectParticipants = false,
          // Done below
          changeDynamicSynchronizerParameters = false,
        )
        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))
        participant2.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))

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

  private def trafficStateAtUpgrade(sequencer: LocalSequencerReference)(implicit
      env: TestConsoleEnvironment
  ): TrafficState =
    sequencer.underlying.value.sequencer.sequencer
      .getTrafficStateAt(env.participant3.id.member, upgradeTime.immediateSuccessor)
      .futureValueUS
      .value
      .value

  "Traffic purchased for a member that is not registered" should {
    "be transferred by an LSU" onlyRunWithOrGreaterThan ProtocolVersion.v37 in { implicit env =>
      import env.*

      clue("purchase traffic for the member on the old synchronizer") {
        initialTrafficPurchase(
          Map(
            participant1 -> PositiveLong.tryCreate(500_000L),
            mediator1 -> PositiveLong.tryCreate(250_000L),
          ),
          sequencer1,
        )

        sendTopUp(participant3.id, prePurchasedAmount, sequencer1, Some(prePurchaseSerial))

        // Ensure that both sequencers have processed the purchase
        participant1.health.ping(participant2)
      }

      val fixture = fixtureWithDefaults()

      val (oldTrafficStateSeq1, oldTrafficStateSeq2) =
        clue("perform the LSU and get the traffic state from the old sequencers") {
          performSynchronizerNodesLsu(fixture)

          // Note: due to CantonBFT advancing block times 1ms per block, keep the buffer before upgradeTime not be too small
          environment.simClock.value.advanceTo(upgradeTime.minusMillis(100L))
          participant1.health.ping(participant2)

          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          (eventuallyGetTraffic(sequencer1), eventuallyGetTraffic(sequencer2))
        }

      clue("the member is part of the traffic state") {
        val trafficState = LsuTrafficState
          .fromByteString(testedProtocolVersion, oldTrafficStateSeq1)
          .valueOrFail("Failed to parse traffic state from sequencer1")
          .membersTraffic
          .get(participant3.id.member)
          .value
        trafficState.extraTrafficPurchased shouldBe prePurchasedAmount
        trafficState.extraTrafficConsumed shouldBe NonNegativeLong.zero
        trafficState.serial shouldBe Some(prePurchaseSerial)

        oldTrafficStateSeq2 shouldBe oldTrafficStateSeq1
      }

      clue("set the traffic state on the new sequencers") {
        sequencer3.traffic_control.set_lsu_state(oldTrafficStateSeq1)
        sequencer4.traffic_control.set_lsu_state(oldTrafficStateSeq2)

        eventually(maxPollInterval = 100.millis, timeUntilSuccess = 40.seconds) {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          Seq(participant1, participant2).forall(
            _.synchronizers.is_connected(fixture.newPsid)
          ) shouldBe true
        }

        waitForTargetTimeOnSequencer(sequencer3, environment.clock.now, logger)
      }

      clue("the traffic state of the member is the same on the old and new sequencers") {
        val oldTrafficState = trafficStateAtUpgrade(sequencer1)
        oldTrafficState.extraTrafficPurchased shouldBe prePurchasedAmount
        oldTrafficState.serial shouldBe Some(prePurchaseSerial)

        forAll(Seq(sequencer2, sequencer3, sequencer4)) { sequencer =>
          trafficStateAtUpgrade(sequencer) shouldBe oldTrafficState
        }
      }

      oldSynchronizerNodes.all.stop()

      clue("the traffic purchased is available once the member is onboarded") {
        participant3.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer3))

        eventually() {
          val trafficState = participant3.traffic_control.traffic_state(daId)
          trafficState.extraTrafficPurchased shouldBe prePurchasedAmount
          trafficState.serial shouldBe Some(prePurchaseSerial)
        }

        participant3.health.ping(participant1)
      }
    }
  }
}
