// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config
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
            .focus(_.parameters.ledgerApiServer.indexer.queueRecoveryRetryMinWaitMillis)
            .replace(600 * 1000)
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

    // unassign
    val unassign = participant1.ledger_api.commands.submit_unassign(
      submitter = participant1.adminParty,
      contractIds = Seq(c1ContractId),
      source = daId,
      target = acmeId,
    )
    logger.info("Unassign submitted")

    // issue write lock on participant1 for C1, this will block the Indexer at ingestion of the following event on the contract
    val lock = withConnectionForTest(participant1)(
      testFunction = writeLockContract(participant1, c1InternalContractId)
    )
    logger.info("C1 locked")

    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.LoggerNameContains("DbDispatcher") && SuppressionRule.Level(event.Level.ERROR)
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
        logMessages.forall(_.contains("INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR")) shouldBe true
      },
    )
    lock.commitAndClose()
  }

}

class LedgerApiDbTimeoutIntegrationTestPostgres extends LedgerApiDbTimeoutIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
