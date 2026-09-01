// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.data.ComponentHealthState
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{HealthStatus, Healthy, Unhealthy}
import com.digitalasset.canton.integration.plugins.UseH2
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.participant.state.Update.SequencerIndexMoved
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.topology.Party
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt

@UnstableTest // TODO(i35213): Remove once the test is stable again
class IndexerHealthIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  var party1: Party = _

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  registerPlugin(new UseH2(loggerFactory))

  def assertHealth(expected: HealthStatus)(implicit env: FixtureParam): Assertion =
    env.participant1.underlying.value.sync.ledgerApiIndexer.asEval.value.indexerHealth
      .currentHealth() shouldEqual expected

  "indexer status is healthy when idle after start" in { implicit env =>
    import env.*

    always()(participant1.ledger_api.state.end() shouldBe 0L)

    eventually() {
      assertHealth(Healthy)
      participant1.health.status.trySuccess.components
        .find(_.name == LedgerApiIndexer.healthComponentName)
        .value
        .state shouldBe (ComponentHealthState.Ok())
    }

    participant1.ledger_api.state.end() shouldBe 0L // Just to be sure that nothing was indexed
  }

  "indexer should go healthy after participant restart" in { implicit env =>
    import env.*

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      participant1.stop(),
      logEntries =>
        logEntries.exists(e =>
          e.level == Level.INFO && e.message.contains(
            "'ledger api indexer' is now in state Failed(Component is closed). Previous state was Ok()" // It's not possible to catch this thorugh participant.health.status as it requires a running participant
          )
        ) should be(true),
    )

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      participant1.start(),
      logEntries =>
        logEntries.exists(e =>
          e.level == Level.INFO && e.message.contains("'ledger api indexer' is now in state Ok().")
        ) should be(true),
    )

  }

  "indexer status flips to degraded when repair is started" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    val party1 = participant1.parties.testing.enable("party1", synchronizer = daName)
    participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)

    val contract = IouSyntax.createIou(participant1)(party1, party1)
    IouSyntax.archive(participant1)(contract, party1)
    participant1.synchronizers.disconnect(daName)

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      participant1.repair.purge(daName, List(contract.id.toLf), true),
      logEntries =>
        { // All states checked below are short-lived and it's impossible to catch them reliably with health status endpoint
          val initializingRepairMsg = logEntries
            .find(e =>
              e.level == Level.INFO && e.message.contains(
                "'ledger api indexer' is now in state Failed(Initializing repair indexer)." // Not checking on previous state, because it might be either OK or component is closed depending on how fast normal indexer closes.
              )
            )
            .value
          val degradedMsg = logEntries
            .find(e =>
              e.level == Level.INFO && e.message.contains(
                "'ledger api indexer' is now in state Degraded(Repair indexer is running). Previous state was Failed(Initializing repair indexer)."
              )
            )
            .value
          val initializingNormalMsg = logEntries
            .find(e =>
              e.level == Level.INFO && e.message.contains(
                "'ledger api indexer' is now in state Failed(Initializing indexer). Previous state was Degraded(Repair indexer is running)."
              )
            )
            .value
          val normalInitializedMsg = logEntries
            .find(e =>
              e.level == Level.INFO && e.message.contains(
                "'ledger api indexer' is now in state Ok(). Previous state was Failed(Initializing indexer)."
              )
            )
            .value

          val initializedRepariMsgIndex = logEntries.indexOf(initializingRepairMsg)
          val degradedMsgIndex = logEntries.indexOf(degradedMsg)
          val initializingNormalMsgIndex = logEntries.indexOf(initializingNormalMsg)
          val normalInitializedMsgIndex = logEntries.indexOf(normalInitializedMsg)

          // Make sure the log messages are in the correct order
          initializedRepariMsgIndex should be < degradedMsgIndex
          degradedMsgIndex should be < initializingNormalMsgIndex
          initializingNormalMsgIndex should be < normalInitializedMsgIndex
        },
    )
  }

  "indexer status flips to unhealthy when it enters a crash loop" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.health.ping(
      participant1
    ) // Make sure something is synchronized, so synchronizer index is updated to current time

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
      {
        // Enqueue a backdated update
        participant1.underlying.value.sync.ledgerApiIndexer.asEval.value
          .enqueue(SequencerIndexMoved(daId, CantonTimestamp.Epoch))
          .discard

        // Participant will become unhealthy, and remain unhealthy do to monotonicity violation
        eventuallyForever(durationOfSuccess = 5.seconds) {
          assertHealth(Unhealthy)
          participant1.health.status.trySuccess.components
            .find(_.name == LedgerApiIndexer.healthComponentName)
            .value
            .state shouldBe ComponentHealthState.Failed(
            ComponentHealthState.UnhealthyState(Some("Initializing indexer"))()
          )
        }

        // Stop the participant to prevent more error logs from being produced, which would make the test flaky
        participant1.stop()
      },
      logEntries => {
        logEntries.count(logEntry =>
          logEntry.message.contains("An internal error has occurred.") &&
            logEntry.throwable.value.getMessage
              .contains("Monotonicity violation detected: record time decreases from")
        ) should be >= 2 // At least two errors to ensure we are in crash loop
        logEntries.exists(e =>
          e.level == Level.INFO && e.message.contains(
            "'ledger api indexer' is now in state Failed(Initializing indexer). Previous state was Ok()"
          )
        ) should be(true)
      },
    )

  }

}
