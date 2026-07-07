// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.CommitmentSendDelay
import com.digitalasset.canton.config.RequireTypes.NonNegativeProportion
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.Active
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

/** This test is a regression test for https://github.com/DACH-NY/canton/issues/32432
  *
  * It checks if that pruning happens during LSU (after the successor is marked as active and before
  * the first connection to the successor), then the participant can still connect to the successor.
  *
  * Scenario:
  *
  *   - Create and Archive an Iou.
  *   - Progress time and wait for ACS commitments to be exchanged so that we have a clean
  *     timestamp.
  *   - Move time to upgrade time so that PN performs the LSU and ensures it does not connect to the
  *     successor (this is done by not transferring traffic yet).
  *   - Prune the PN.
  *   - Transfer traffic.
  *   - PN should connect to the successor.
  */
final class LsuPruningDuringLsuIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-pruning-during-lsu"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(86400)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              Some(NonNegativeProportion.zero),
              Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(1.minutes.toJava)
      )
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  private def noOutstandingCommitments(
      p: LocalParticipantReference,
      ts: CantonTimestamp,
  )(implicit env: TestConsoleEnvironment): CantonTimestamp =
    p.underlying.value.sync.syncPersistentStateManager
      .get(env.daId)
      .value
      .acsCommitmentStore
      .noOutstandingCommitments(ts)
      .futureValueUS
      .value

  "Pruning after a logical synchronizer upgrade" should {
    "work correctly" in { implicit env =>
      import env.*
      val alice = participant1.parties.enable("Alice")
      val bank = participant1.parties.enable("Bank")

      val fixture = fixtureWithDefaults()

      performSynchronizerNodesLsu(fixture)

      // Activity before LSU
      participant1.health.ping(participant1)

      val tempIou = IouSyntax.createIou(participant1)(bank, alice, 1.0)
      IouSyntax.archive(participant1)(tempIou, bank)

      // Ensuring that ACS commitments are exchanged so that pruning can happen
      val pruningTs = CantonTimestamp.ofEpochSecond(300)
      environment.simClock.value.advanceTo(pruningTs.immediateSuccessor)
      participant1.health.ping(participant1)
      eventually() {
        noOutstandingCommitments(participant1, pruningTs) shouldBe pruningTs
      }

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      // p1 performs the upgrade but cannot yet connect to the synchronizer because traffic was not transferred
      eventually() {
        participant1.underlying.value.sync.synchronizerConnectionConfigStore
          .get(fixture.newPsid)
          .value
          .status shouldBe Active
      }

      // Prune p1
      val offset = participant1.pruning.find_safe_offset(beforeOrAt = pruningTs.toInstant).value
      participant1.pruning.prune(offset)

      // Transfer traffic so that p1 connects to the synchronizer
      transferTraffic()

      eventually() {
        participant1.synchronizers.is_connected(fixture.newPsid) shouldBe true
      }

      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

      participant1.health.ping(participant1)
    }
  }
}
