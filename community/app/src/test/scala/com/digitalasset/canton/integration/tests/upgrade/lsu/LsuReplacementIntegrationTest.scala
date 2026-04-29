// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration
import scala.annotation.nowarn

/** The goal is to ensure that an LSU can be replaced by another one.
  */
@nowarn("msg=dead code")
abstract class LsuReplacementIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-replacement"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2"), Set("sequencer3")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  protected lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)
  protected def upgradeTime2: CantonTimestamp

  protected var fixture1: Fixture = _
  protected var fixture2: Fixture = _

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set("sequencer2", "sequencer3", "mediator2", "mediator3")

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 3,
        numMediators = 3,
      )
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        fixture1 = Fixture(
          currentPsid = daId,
          upgradeTime = upgradeTime1,
          oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
          newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
          newOldNodesResolution = Map("sequencer2" -> "sequencer1", "mediator2" -> "mediator1"),
          oldSynchronizerOwners = synchronizerOwners1,
          newPV = ProtocolVersion.dev,
          newSerial = daId.serial.increment.toNonNegative,
        )

        fixture2 = Fixture(
          currentPsid = daId,
          upgradeTime = upgradeTime2,
          oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
          newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3)),
          newOldNodesResolution = Map("sequencer3" -> "sequencer1", "mediator3" -> "mediator1"),
          oldSynchronizerOwners = synchronizerOwners1,
          newPV = ProtocolVersion.dev,
          newSerial = fixture1.newSerial.increment.toNonNegative,
        )

        daId should not be fixture1.newPsid
        fixture1.newPsid should not be fixture2.newPsid

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        participants.local.start()
        fixture1.oldSynchronizerNodes.all.start()

        participants.all.synchronizers.connect(defaultSynchronizerConnectionConfig())

        participant1.health.ping(participant1)
      }
}

final class LsuReplacementSameTimeIntegrationTest extends LsuReplacementIntegrationTest {
  override protected def upgradeTime2: CantonTimestamp = upgradeTime1

  "Logical synchronizer upgrade" should {
    "allow new LSU to replace the previously announced (same upgrade time)" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      fixture1.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture1)

      // announce a second LSU shortly before the upgrade time
      clock.advanceTo(upgradeTime1.minusSeconds(5))
      fixture2.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture2)

      clock.advanceTo(upgradeTime2.immediateSuccessor)
      transferTraffic(Some(fixture2))
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participant1.synchronizers.is_connected(fixture2.newPsid) shouldBe true
      }

      waitForTargetTimeOnSequencer(sequencer3, environment.now, logger)
      participant1.health.ping(participant1)
    }
  }
}

final class LsuReplacementEarlierTimeIntegrationTest extends LsuReplacementIntegrationTest {
  override protected def upgradeTime2: CantonTimestamp = upgradeTime1.minusSeconds(10)

  "Logical synchronizer upgrade" should {
    "allow an LSU to be replaced by another earlier LSU" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      fixture1.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture1)

      // announce a second LSU earlier than the first one
      fixture2.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture2)
      fixture1.newSynchronizerNodes.all.stop() // initial successors are not needed anymore

      clock.advanceTo(upgradeTime2.immediateSuccessor)
      transferTraffic(Some(fixture2))
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participant1.synchronizers.is_connected(fixture2.newPsid) shouldBe true
      }
      fixture1.oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer3, environment.now, logger)
      participant1.health.ping(participant1)

      // Move past original upgrade time
      clock.advanceTo(upgradeTime1.plusSeconds(3600))
      waitForTargetTimeOnSequencer(sequencer3, environment.now, logger)
      participant1.health.ping(participant1)
    }
  }
}

final class LsuReplacementLaterTimeIntegrationTest extends LsuReplacementIntegrationTest {
  override protected def upgradeTime2: CantonTimestamp = upgradeTime1.plusSeconds(15)

  "Logical synchronizer upgrade" should {
    "allow an LSU to be replaced by another later LSU" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      fixture1.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture1)

      // announce a second LSU later than the first one
      fixture2.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture2)
      fixture1.newSynchronizerNodes.all.stop()

      // Move time past initial upgrade time
      clock.advanceTo(upgradeTime1.plusSeconds(1))
      waitForTargetTimeOnSequencer(sequencer1, environment.clock.now, logger)

      // participant should still be connected to the first synchronizer
      participant1.health.ping(participant1)
      participant1.synchronizers.is_connected(fixture1.currentPsid) shouldBe true

      // Second LSU is done
      clock.advanceTo(upgradeTime2.immediateSuccessor)
      transferTraffic(Some(fixture2))
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participant1.synchronizers.is_connected(fixture2.newPsid) shouldBe true
      }

      fixture1.oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer3, environment.now, logger)
      participant1.health.ping(participant1)
    }
  }
}
