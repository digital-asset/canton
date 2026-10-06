// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveDurationSeconds}
import com.digitalasset.canton.integration.ConfigTransforms.updateAllParticipantConfigs_
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import monocle.macros.syntax.lens.*

trait LedgerApiIterativePruningTest extends CommunityIntegrationTest with SharedEnvironment {

  private val transactionTolerance = NonNegativeFiniteDuration.ofSeconds(2)

  // single participant environment to focus on ledger api server pruning rather than acs canton commitments
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateMaxDeduplicationDurations(transactionTolerance.asJava),
        updateAllParticipantConfigs_(
          _.focus(_.ledgerApi.indexService.contractPruningBatchSize)
            .replace(PositiveInt.tryCreate(3))
            .focus(_.parameters.batching.maxPruningBatchSize)
            .replace(PositiveInt.tryCreate(50))
        ),
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)

        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.copy(
            confirmationResponseTimeout = transactionTolerance,
            mediatorReactionTimeout = transactionTolerance,
            reconciliationInterval = PositiveDurationSeconds(transactionTolerance.asJava),
          ),
        )
      }

  "pruning in small iterations work correctly" in { implicit env =>
    import env.*

    // create 126 contracts
    logger.info("Started bong")
    participant1.testing.bong(Set(participant1), levels = 5)
    val ledgerEndAfterBong = participant1.ledger_api.state.end()
    logger.info(s"Bong finished, ledger end: $ledgerEndAfterBong")

    // ensure we have 126 contracts
    participant1.testing.state_inspection.contractCount.futureValueUS shouldBe 126

    // ensure we can prune the bong contracts
    eventually() {
      participant1.health.ping(participant1)
      participant1.pruning.find_safe_offset().value should be >= ledgerEndAfterBong
    }
    logger.info(s"Pruning is possible at $ledgerEndAfterBong")

    val contractCountAsPrunable = participant1.testing.state_inspection.contractCount.futureValueUS

    // pruning bong contracts
    loggerFactory.assertLogsSeq(SuppressionRule.LoggerNameContains("PostgresEventStorageBackend"))(
      within = participant1.pruning.prune(ledgerEndAfterBong),
      assertion = logEntries => {
        // the 50 offset range specified for pruning batch size should give 5 iterations of pruning
        logEntries.count(_.message.contains("Start pruning Index DB events.")) shouldBe 5
        // contract pruning set to 3 contracts per iteration, giving about 45 iterations total, majority should be with the maximum 3
        logEntries.count(_.message.contains("Finished pruning of 3 contracts.")) should be > (30)
      },
    )

    // contracts are removed from the contract store (please note this is not strictly needed as DB integrity checker is also checking the contract store for pruneable contracts)
    contractCountAsPrunable - participant1.testing.state_inspection.contractCount.futureValueUS shouldBe 126
  }
}

// Please note: Postgres test unlocks additional checks in integrity checker, which also has assertions related to pruning making this test have more assertions
class LedgerApiIterativePruningTestPostgres extends LedgerApiIterativePruningTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
