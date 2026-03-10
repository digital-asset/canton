// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.ledger.javaapi.data.Transaction
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax.createIou
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
}
import com.digitalasset.canton.topology.{Party, SynchronizerId}

import java.time.Duration
import scala.jdk.CollectionConverters.*

/** Tests that we can effectively look up the offset for a timestamp across LSUs, specifically:
  *   - find_highest_offset_by_timestamp
  *   - get_offset_by_time
  *   - find_safe_offset
  */
class LsuOffsetByTimestampIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-offset-by-timestamp"

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
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(300)

  private val maxDedupDuration = Duration.ofSeconds(1)
  private var pruningTimeout: Duration = _

  override protected def configTransforms: Seq[ConfigTransform] =
    super.configTransforms ++ Seq(
      ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration)
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M2_Config
      .withNetworkBootstrap(implicit env => new NetworkBootstrapper(EnvironmentDefinition.S1M1))
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()

        val seqDynParams = env.sequencer1.topology.synchronizer_parameters
          .get_dynamic_synchronizer_parameters(env.daId)
        pruningTimeout = (seqDynParams.reconciliationInterval.asJava
          .plus(seqDynParams.confirmationResponseTimeout.asJava)
          .plus(seqDynParams.mediatorReactionTimeout.asJava))
          .plus(maxDedupDuration)
      }

  private def createAndArchive(
      participant: LocalParticipantReference,
      party: Party,
      lsid: SynchronizerId,
  ): Transaction = {
    val iou = createIou(participant, Some(lsid))(party, party, 42)
    val archiveCmd = iou.id.exerciseArchive().commands().asScala.toSeq
    participant.ledger_api.javaapi.commands.submit(Seq(party), archiveCmd, Some(lsid))
  }

  "Logical synchronizer upgrade" should {
    "be compatible with timestamp -> offset lookups" in { implicit env =>
      import env.*
      val fixture = fixtureWithDefaults()

      val alice = participant1.parties.testing.enable("Alice")
      val lsid = daId.logical

      environment.simClock.value.advance(Duration.ofSeconds(5))

      val txnPreLsu = createAndArchive(participant1, alice, lsid)

      val safeOffsetPreLsu = withClue("should be able to get offsets before LSU") {
        environment.simClock.value.advance(pruningTimeout)
        val safeOffset =
          participant1.pruning.find_safe_offset(environment.clock.now.toInstant).value
        safeOffset should be <= txnPreLsu.getOffset.toLong
        safeOffset
      }

      withClue("perform LSU") {
        performSynchronizerNodesLsu(fixture)
        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe true
        }
        environment.simClock.value.advance(Duration.ofSeconds(1))
        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
      }

      withClue("check we can lookup offsets from before the LSU") {
        participant1.parties.find_highest_offset_by_timestamp(
          lsid,
          txnPreLsu.getRecordTime,
        ) shouldBe txnPreLsu.getOffset

        participant1.pruning.get_offset_by_time(txnPreLsu.getRecordTime) shouldBe Some(
          txnPreLsu.getOffset
        )

        participant1.pruning
          .find_safe_offset(environment.clock.now.toInstant)
          .value should be <= txnPreLsu.getOffset.toLong
      }

      val txnPostLsu = createAndArchive(participant1, alice, lsid)

      withClue("check we can lookup offsets from after the LSU") {
        participant1.parties.find_highest_offset_by_timestamp(
          lsid,
          txnPostLsu.getRecordTime,
        ) shouldBe txnPostLsu.getOffset

        participant1.pruning.get_offset_by_time(txnPostLsu.getRecordTime) shouldBe Some(
          txnPostLsu.getOffset
        )

        environment.simClock.value.advance(pruningTimeout)
        val safeOffset =
          participant1.pruning.find_safe_offset(environment.clock.now.toInstant).value
        safeOffset should be >= safeOffsetPreLsu
        safeOffset should be <= txnPostLsu.getOffset.toLong
      }
    }
  }
}
