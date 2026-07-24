// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{HealthStatus, Healthy, Unhealthy}
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.logging.SuppressionRule
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt

class IndexerHealthIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  registerPlugin(new UseH2(loggerFactory))

  def assertHealth(expected: HealthStatus)(implicit env: FixtureParam): Assertion =
    env.participant1.underlying.value.sync.ledgerApiIndexer.asEval.value.indexerHealth
      .currentHealth() shouldEqual expected

  "indexer status is healthy when idle after start" in { implicit env =>
    import env.*

    always()(participant1.ledger_api.state.end() shouldBe 0L)

    eventually()(assertHealth(Healthy))

    participant1.ledger_api.state.end() shouldBe 0L // Just to be sure that nothing was indexed
  }

  "indexer status flips to unhealthy when it enters a crash loop" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.health.ping(
      participant1
    ) // Make sure something is synchronized, so synchronizer index is updated to current time

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.ERROR))(
      {
        // Enqueue a backdated update
        participant1.underlying.value.sync.ledgerApiIndexer.asEval.value
          .enqueue(SequencerIndexMoved(daId, CantonTimestamp.Epoch))
          .discard

        // Participant will become unhealthy, and remain unhealthy do to monotonicity violation
        eventuallyForever(durationOfSuccess = 5.seconds)(assertHealth(Unhealthy))

        // Stop the participant to prevent more error logs from being produced, which would make the test flaky
        participant1.stop()
      },
      logEntries => {
        logEntries.count(logEntry =>
          logEntry.message.contains("An internal error has occurred.") &&
            logEntry.throwable.value.getMessage
              .contains("Monotonicity violation detected: record time decreases from")
        ) should be >= 2 // At least two errors to ensure we are in crash loop
      },
    )

  }

}
