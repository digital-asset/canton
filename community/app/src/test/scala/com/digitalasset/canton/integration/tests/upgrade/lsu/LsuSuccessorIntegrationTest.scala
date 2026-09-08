// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.SynchronizerLimits
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt.{one, zero}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Outcome

import java.time.Duration

/*
 * This test validates whether a given physical synchronizer id is accepted or rejected when
 * attempting the upgrade announcement.
 */
sealed abstract class LsuSuccessorIntegrationTest extends LsuBase {

  def currentSerial: NonNegativeInt
  def currentPV: ProtocolVersion
  def successorSerial: NonNegativeInt
  def successorPV: ProtocolVersion

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

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        // Set the synchronizer's initial serial and PV
        new NetworkBootstrapper(
          S1M1.copy(
            staticSynchronizerParameters = S1M1.staticSynchronizerParameters.copy(
              protocolVersion = currentPV,
              serial = currentSerial,
              synchronizerLimits = SynchronizerLimits.defaultFor(currentPV),
            )
          )
        )
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransforms(ConfigTransforms.enableAlphaVersionSupport*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
        env.participant1.health.ping(env.participant2.id)
      }
}

sealed abstract class LsuSuccessorAcceptedIntegrationTest extends LsuSuccessorIntegrationTest {

  override protected def testName: String =
    s"lsu-psid-accepted-from-${currentSerial}_$currentPV-to-${successorSerial}_$successorPV"

  "Logical synchronizer upgrade" should {
    s"succeed for (serial=$currentSerial, pv=$currentPV) -> (serial=$successorSerial, pv=$successorPV)" in {
      implicit env =>
        import env.*

        val fixture =
          fixtureWithDefaults(
            newPVOverride = Some(successorPV),
            newSerialOverride = Some(successorSerial),
          )

        performSynchronizerNodesLsu(fixture)

        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        }

        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
    }
  }
}

sealed abstract class LsuSuccessorRejectedIntegrationTest extends LsuSuccessorIntegrationTest {

  override protected def testName: String =
    s"lsu-psid-rejected-from-${currentSerial}_$currentPV-to-${successorSerial}_$successorPV"

  "Logical synchronizer upgrade announcement" should {
    s"fail for (serial=$currentSerial, pv=$currentPV) -> (serial=$successorSerial, pv=$successorPV)" in {
      implicit env =>
        val fixture =
          fixtureWithDefaults(
            newPVOverride = Some(successorPV),
            newSerialOverride = Some(successorSerial),
          )

        loggerFactory.assertLogs(
          assertThrows[CommandFailure] {
            fixture.oldSynchronizerOwners.foreach(
              _.topology.lsu.announcement.propose(fixture.newPsid, fixture.upgradeTime)
            )
          },
          _.message should include("successor id is not greater than current synchronizer id"),
        )
    }
  }
}

// If the elements change in opposite directions, serial takes priority.
final class LsuSuccessorSerialUpPVDownIntegrationTest extends LsuSuccessorAcceptedIntegrationTest {

  override val currentPV: ProtocolVersion = testedProtocolVersion
  override val currentSerial: NonNegativeInt = zero
  override val successorPV: ProtocolVersion = currentPV.previousSupported.getOrElse(currentPV)
  override val successorSerial: NonNegativeInt = one

  override def withFixture(test: OneArgTest): Outcome =
    if (currentPV == successorPV) {
      cancel(
        s"Skipping test because there's no supported PV lower than the tested protocol version $testedProtocolVersion."
      )
    } else {
      super.withFixture(test)
    }
}

final class LsuSuccessorSerialDownPVUpIntegrationTest extends LsuSuccessorRejectedIntegrationTest {

  override val currentSerial: NonNegativeInt = one
  override val currentPV: ProtocolVersion = testedProtocolVersion
  override val successorSerial: NonNegativeInt = zero
  override val successorPV: ProtocolVersion = currentPV.nextSupported.getOrElse(currentPV)

  override def withFixture(test: OneArgTest): Outcome =
    if (currentPV == successorPV) {
      cancel(
        s"Skipping test because there's no supported PV higher than the tested protocol version $testedProtocolVersion."
      )
    } else {
      super.withFixture(test)
    }
}
