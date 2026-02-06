// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt.{one, zero}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion.{v34, v35}

/*
 * This test validates whether a given physical synchronizer id is accepted or rejected when
 * attempting the upgrade announcement.
 */
sealed abstract class LsuSuccessorIntegrationTest(
    currAndNextSerialAndPV: ((NonNegativeInt, ProtocolVersion), (NonNegativeInt, ProtocolVersion))
) extends LsuBase {
  val ((currSerial, currPV), (nextSerial, nextPV)) = currAndNextSerialAndPV

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override protected lazy val testedProtocolVersion: ProtocolVersion = currPV

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        // Set the synchronizer's initial serial and PV
        new NetworkBootstrapper(
          S1M1.copy(
            staticSynchronizerParameters =
              S1M1.staticSynchronizerParameters.copy(protocolVersion = currPV, serial = currSerial)
          )
        )
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
        env.participant1.health.ping(env.participant2.id)
      }
}

sealed abstract class LsuSuccessorAcceptedIntegrationTest(
    // ((currSerial, currPV), (nextSerial, nextPV))
    currAndNextSerialAndPV: ((NonNegativeInt, ProtocolVersion), (NonNegativeInt, ProtocolVersion))
) extends LsuSuccessorIntegrationTest(currAndNextSerialAndPV) {

  override protected def testName: String =
    s"lsu-psid-accepted-from-${currSerial}_$currPV-to-${nextSerial}_$nextPV"

  "Logical synchronizer upgrade" should {
    s"succeed for (serial=$currSerial, pv=$currPV) -> (serial=$nextSerial, pv=$nextPV)" in {
      implicit env =>
        import env.*

        val fixture =
          fixtureWithDefaults(newPVOverride = Some(nextPV), newSerialOverride = Some(nextSerial))

        performSynchronizerNodesLsu(fixture)

        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

        eventually() {
          participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        }

        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)
    }
  }
}

sealed abstract class LsuSuccessorRejectedIntegrationTest(
    // ((currSerial, currPV), (nextSerial, nextPV))
    currAndNextSerialAndPV: ((NonNegativeInt, ProtocolVersion), (NonNegativeInt, ProtocolVersion))
) extends LsuSuccessorIntegrationTest(currAndNextSerialAndPV) {

  override protected def testName: String =
    s"lsu-psid-rejected-from-${currSerial}_$currPV-to-${nextSerial}_$nextPV"

  "Logical synchronizer upgrade announcement" should {
    s"fail for (serial=$currSerial, pv=$currPV) -> (serial=$nextSerial, pv=$nextPV)" in {
      implicit env =>
        val fixture =
          fixtureWithDefaults(newPVOverride = Some(nextPV), newSerialOverride = Some(nextSerial))

        loggerFactory.assertLogs(
          assertThrows[CommandFailure] {
            fixture.oldSynchronizerOwners.foreach(
              _.topology.lsu.announcement
                .propose(fixture.newPSId, fixture.upgradeTime)
            )
          },
          _.message should include("successor id is not greater than current synchronizer id"),
        )
    }
  }
}

// If the elements change in opposite directions, serial takes priority.
final class LsuSuccessorSerialUpPVDownIntegrationTest
    extends LsuSuccessorAcceptedIntegrationTest((zero, v35) -> (one, v34))
final class LsuSuccessorSerialDownPVUpIntegrationTest
    extends LsuSuccessorRejectedIntegrationTest((one, v34) -> (zero, v35))
