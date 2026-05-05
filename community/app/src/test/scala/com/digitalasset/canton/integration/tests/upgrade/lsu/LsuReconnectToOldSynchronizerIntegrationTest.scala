// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.synchronizer.block.update.BlockChunkProcessor
import com.digitalasset.canton.time.PositiveFiniteDuration
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

/** This test validates that participants can disconnect from and reconnect to the old synchronizer
  * after the upgrade time.
  *
  * This test doesn't actually perform an LSU.
  */
final class LsuReconnectToOldSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        // Ensure PNs stay connected to the old synchronizer
        _.focus(_.parameters.lsu.automaticallyPerformLsu).replace(false)
      )
    )

  "reconnect to the old synchronizer" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.health.ping(participant1)

    // announce the synchronizer upgrade
    val successorPsid = daId.incrementSerial
    val upgradeTime = environment.clock.now + PositiveFiniteDuration.tryOfSeconds(15)
    synchronizerOwners1.foreach { node =>
      node.topology.lsu.announcement.propose(successorPsid, upgradeTime, daId)
    }

    clue("awaiting announcement to become active") {
      eventually() {
        val announcement = participant1.topology.lsu.announcement.list(daId).loneElement
        announcement.item.upgradeTime shouldBe upgradeTime
        announcement.context.validFrom should be < upgradeTime.toInstant
      }
    }

    clue(s"waiting for time to pass until after $upgradeTime") {
      participant1.testing.await_synchronizer_time(daId, upgradeTime)
    }

    clue("disconnecting from synchronizer") {
      participant1.synchronizers.disconnect_all()
    }

    // reconnecting should work
    clue("reconnecting from synchronizer") {
      participant1.synchronizers.reconnect_all()
    }

    // attempt to trigger a time proof, but verify that the event is dropped in BlockChunkProcessor
    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.Level(Level.ERROR) || (
        SuppressionRule.forLogger[BlockChunkProcessor] && SuppressionRule.Level(Level.INFO)
      )
    )(
      the[CommandFailure] thrownBy participant1.testing.fetch_synchronizer_time(
        daId,
        timeout = NonNegativeDuration.ofSeconds(10),
      ) shouldBe a[CommandFailure],
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (
            _.infoMessage should include(
              "Dropping Send event after upgrade time"
            ),
            "skip event log",
          ),
          (
            _.errorMessage should include("DEADLINE_EXCEEDED"),
            "command times out",
          ),
        ),
        // catch all for other INFO logs from BlockChunkProcessor
        mayContain = Seq(_.loggerName should include(classOf[BlockChunkProcessor].getSimpleName)),
      ),
    )

  }

}
