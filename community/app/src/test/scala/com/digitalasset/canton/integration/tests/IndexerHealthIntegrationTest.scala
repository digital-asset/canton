// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{Healthy, Unhealthy}
import com.digitalasset.canton.integration.ConfigTransforms.updateAllParticipantConfigs_
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.logging.{SuppressingLogger, SuppressionRule}
import monocle.syntax.all.toAppliedFocusOps
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt

class IndexerHealthIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.addConfigTransform(
      updateAllParticipantConfigs_(
        _.focus(_.parameters.ledgerApiServer.indexer.queueRecoveryRetryMinWaitMillis)
          .replace(
            1000
          ) // This reduces a risk of error log message escaping between two log supres blocks. It cannot be too big, as in the second test case we are waiting for repeated crash messages
      )
    )

  override val loggerFactory: SuppressingLogger = SuppressingLogger(
    testClass = getClass,
    pollTimeout = 5.seconds,
  ) // Increase timeout for waiting for a log line as we make retries less frequent

  registerPlugin(new UseH2(loggerFactory))

  "indexer status is healthy when idle after start" in { implicit env =>
    import env.*

    always() {
      participant1.ledger_api.state.end() shouldBe (0L)
    }

    eventually() {
      participant1.underlying.value.sync.ledgerApiIndexer.asEval.value.indexerHealth
        .currentHealth() shouldEqual (Healthy)
    }

    participant1.ledger_api.state.end() shouldBe (0L) // Just to be sure that nothing was indexed
  }

  "indexer status flips to unhealthy when it enters a crashloop" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.health.ping(
      participant1
    ) // Make sure something is synchronized, so synchronizer index is updated to current time

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.ERROR))(
      participant1.underlying.value.sync.ledgerApiIndexer.asEval.value
        .enqueue(SequencerIndexMoved(daId, CantonTimestamp.Epoch))
        .discard,
      logEntries => {
        logEntries.count(logEntry =>
          logEntry.message.contains("An internal error has occurred.") &&
            logEntry.throwable.value.getMessage
              .contains("Monotonicity violation detected: record time decreases from")
        ) should be >= (2)
      },
    )

    // Note: this gap in error suppression may cause flakiness, see the config transform for info

    loggerFactory.suppressWarningsAndErrors { // Crash loop still produces errors

      always(durationOfSuccess = 5.seconds) {
        participant1.underlying.value.sync.ledgerApiIndexer.asEval.value.indexerHealth
          .currentHealth() shouldEqual (Unhealthy)
      }

      participant1.stop() // Stop before end of suppress block to prevent more error logs
    }
  }

}
