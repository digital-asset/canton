// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbStorageSingle
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.DefaultSegmentLength
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{HasExecutionContext, config}

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

class LsuSequencerRestartWallClockIntegrationTest extends LsuBase with HasExecutionContext {

  override protected def testName: String = "lsu-sequencer-restart"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  private val nonZeroTopologyChangeDelay =
    config.NonNegativeFiniteDuration.ofMillis(250)
  private val timeToUpgrade = 60.seconds

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected val useStaticTime: Boolean = false
  override protected lazy val upgradeTime: CantonTimestamp = {
    val currentTime = CantonTimestamp.now()
    currentTime.plus(timeToUpgrade.toJava).addMicros(-currentTime.microsOverSecond())
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1.withTopologyChangeDelay(nonZeroTopologyChangeDelay))
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "work even when new sequencers are restarted before the LSU upgrade time " +
      "with post-ordering behind by 1 epoch or more" in { implicit env =>
        import env.*

        participant1.health.ping(participant2)

        val fixture = fixtureWithDefaults(topologyChangeDelay = nonZeroTopologyChangeDelay)
        performSynchronizerNodesLsu(fixture)

        val seq1Delay = sequencer1.synchronizer_parameters.static.get().topologyChangeDelay
        val seq2Delay = sequencer2.synchronizer_parameters.static.get().topologyChangeDelay
        logger.info(
          s"sequencer1 topologyChangeDelay: $seq1Delay, sequencer2 topologyChangeDelay: $seq2Delay"
        )
        seq1Delay shouldBe nonZeroTopologyChangeDelay
        seq2Delay shouldBe nonZeroTopologyChangeDelay

        clue("Order some epochs") {
          // Ensure that all relevant modules in the CantonBFT ordering component (i.e., consensus AND output)
          //  are in an ordering epoch > 0 after the restart, so that pending topology changes are checked
          val sampledEpochNumber = sequencer2.bft.get_ordering_topology().currentEpoch
          eventually() {
            sequencer2.bft.get_ordering_topology().currentEpoch should be > sampledEpochNumber + 5
          }
        }

        clue("Restart the new sequencer in the critical state") {
          logger.info(s"LSU upgrade time: $upgradeTime, current time: ${environment.clock.now}")
          upgradeTime should be > environment.clock.now // Delay the upgrade time further if this flakes

          val currentEpoch = sequencer2.bft.get_ordering_topology().currentEpoch
          sequencer2.stop()

          implicit val closeContext: CloseContext = CloseContext(this)

          // Rollback post-ordering by a couple of epochs, but not so far back that it is processing blocks in epoch 0
          ResourceUtil.withResource(tryCreateStorage(sequencer2)) { storage =>
            import storage.api.*

            def queryCurrentBlockHeight(): Long =
              storage
                .query(
                  sql"""
                SELECT max(height)
                FROM seq_block_height
              """.as[Long],
                  "getCurrentBlockHeight",
                )
                .futureValueUS
                .headOption
                .getOrElse(-1L)

            val currentBlockHeight = queryCurrentBlockHeight()
            logger.info(
              s"Current post-ordering block height: $currentBlockHeight, current epoch: $currentEpoch"
            )
            val segmentLength = DefaultSegmentLength.length.value
            // Ensure we are beyond the first epoch; epoch length = segment length (only 1 sequencer)
            currentBlockHeight should be > segmentLength // Produce more epochs above if this flakes

            if (currentBlockHeight / segmentLength == currentEpoch) {
              // Rollback post-ordering by 1 epoch
              val rollbackBlockHeight = currentBlockHeight - segmentLength
              logger.info(
                "Need to rollback post-ordering so that it is at least 1 epoch behind ordering, " +
                  s"rollback height: $rollbackBlockHeight"
              )
              val deleteResult = storage
                .update(
                  sql"""
                delete from seq_block_height
                where height > $rollbackBlockHeight
              """.asUpdate,
                  "rollbackBlockHeight",
                )
                .futureValueUS

              logger.info(s"Deleted blocks: $deleteResult")
              deleteResult shouldBe segmentLength
            }

            val newBlockHeight = queryCurrentBlockHeight()
            logger.info(s"New post-ordering block height: $newBlockHeight")
          }

          sequencer2.start()
          sequencer2.health.wait_for_running()
        }

        clue("Transfer traffic from the old sequencer to the new one after the upgrade time") {
          transferTraffic(timeUntilSuccess = timeToUpgrade.plus(20.seconds))
        }

        clue(
          "Participants are connected to the new sequencer and not the old one after the upgrade time"
        ) {
          eventually(timeUntilSuccess = timeToUpgrade.plus(20.seconds)) {
            participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
            participants.all.forall(
              _.synchronizers.is_connected(fixture.currentPsid)
            ) shouldBe false
          }
        }

        clue(
          "The old synchronizer is switched off and the new synchronizer is able to process requests " +
            "from the participants after the upgrade time"
        ) {
          oldSynchronizerNodes.all.stop()
          participant1.health.ping(participant2)
        }
      }
  }

  private def tryCreateStorage(
      localInstance: LocalInstanceReference
  )(implicit env: TestConsoleEnvironment, closeContext: CloseContext): DbStorageSingle =
    DbStorageSingle.tryCreate(
      config = localInstance.config.storage.asInstanceOf[DbConfig],
      clock = env.environment.clock,
      scheduler = None,
      connectionPoolForParticipant = false,
      logQueryCost = None,
      metrics = CommonMockMetrics.dbStorage,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
}
