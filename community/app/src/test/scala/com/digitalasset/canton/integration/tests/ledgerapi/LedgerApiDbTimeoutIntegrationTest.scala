// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import anorm.SqlStringInterpolation
import com.digitalasset.canton.config
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.ConfigTransforms.updateAllParticipantConfigs_
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.daml.lf.value.Value.ContractId
import monocle.macros.syntax.lens.*
import org.slf4j.event

import scala.concurrent.duration.DurationInt

trait LedgerApiDbTimeoutIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with DbLockingSupport {
  import scala.language.reflectiveCalls

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
        updateAllParticipantConfigs_(
          _.focus(_.parameters.ledgerApiServer.indexer.postgresDataSource.networkTimeout)
            .replace(Some(config.NonNegativeFiniteDuration(java.time.Duration.ofMillis(2500))))
            .focus(_.parameters.ledgerApiServer.indexer.contractReadRowDbLockTimeout)
            .replace(config.PositiveFiniteDuration.ofMillis(2500))
            .focus(_.parameters.ledgerApiServer.indexer.queueRecoveryRetryMinWaitMillis)
            .replace(5000)
        ),
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonTestsPath, synchronizerId = Some(daId))
        participants.all.dars.upload(CantonTestsPath, synchronizerId = Some(acmeId))
      }

  "jdbc network timeout should yield the correct canton error" in { implicit env =>
    import env.*

    val (_, c1cid) = createContract(participant1, daId)
    val c1ContractId = ContractId.assertFromString(c1cid)
    val c1InternalContractId =
      participant1.testing.state_inspection.internalContractIdOf(c1ContractId).value

    def c1Active = participant1.ledger_api.state.acs
      .active_contracts_of_party(participant1.id.adminParty)
      .count(_.createdEvent.value.contractId == c1cid)
      .==(1)

    c1Active shouldBe true

    // unassign
    val unassign = participant1.ledger_api.commands.submit_unassign(
      submitter = participant1.adminParty,
      contractIds = Seq(c1ContractId),
      source = daId,
      target = acmeId,
    )
    logger.info("Unassign submitted")

    c1Active shouldBe false

    // issue write lock on participant1 for C1, this will block the Indexer at ingestion of the following event on the contract
    val lock = withConnectionForTest(participant1)(
      testFunction = writeLockContract(participant1, c1InternalContractId)
    )
    logger.info("C1 locked")

    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.LoggerNameContains("ParallelIndexerSubscription") && SuppressionRule.Level(
        event.Level.ERROR
      )
    )(
      within = {
        // reassign C1 to acme, this should be blocked on Indexing the assignment because of the lock above
        participant1.ledger_api.commands.submit_assign_async(
          submitter = participant1.adminParty,
          reassignmentId = unassign.reassignmentId,
          source = daId,
          target = acmeId,
        )
        logger.info("Reassignment of C1 started")
      },
      assertion = logs => {
        val logMessages = logs.map(_.message)
        logMessages.size shouldBe 1
        logMessages.forall(_.contains("INDEX_DB_LOCK_TIMEOUT_ERROR")) shouldBe true
        logMessages.forall(
          _.contains(
            "Acquisition of DB Lock timed out (timeout config: \"indexer-config.contract-read-row-db-lock-timeout\": 2500 ms). Lock description: read row lock of contract table"
          )
        ) shouldBe true
      },
      maxPollInterval = 100.millis,
    )
    c1Active shouldBe false
    lock.commitAndClose()
    logger.info("C1 unlocked")

    // issue exclusive lock on activate table to block insert
    val exclusiveAccessLockOnActivationTable = withConnectionForTest(participant1)(
      testFunction =
        SQL"LOCK TABLE lapi_events_activate_contract IN ACCESS EXCLUSIVE MODE".execute()(_).discard
    )
    logger.info(
      "lapi_events_activate_contract locked in ACCESS EXCLUSIVE MODE (preventing insert after indexer restart)"
    )

    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.LoggerNameContains("DbDispatcher") && SuppressionRule.Level(event.Level.ERROR)
    )(
      within = (),
      assertion = logs => {
        val logMessages = logs.map(_.message)
        logMessages.size shouldBe 1
        logMessages.forall(_.contains("INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR")) shouldBe true
      },
      maxPollInterval = 100.millis,
    )
    exclusiveAccessLockOnActivationTable.commitAndClose()
    logger.info("lapi_events_activate_contract unlocked")
    eventually()(c1Active shouldBe true)
  }

}

class LedgerApiDbTimeoutIntegrationTestPostgres extends LedgerApiDbTimeoutIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
