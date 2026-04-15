// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.{FeatureFlag, LocalParticipantReference}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.repair.RepairServiceIntegrationTest
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.AchsState
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

sealed trait AchsRepairIntegrationTest extends RepairServiceIntegrationTest {

  override protected def cantonTestsPath: String = CantonExamplesPath

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair)
      )

  private val achsLogRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("InitializeParallelIngestion") &&
      SuppressionRule.LevelAndAbove(Level.INFO)

  private def getAchsStateFromCache(
      participantRef: LocalParticipantReference
  ): AchsState =
    participantRef.underlying.value.sync.ledgerApiIndexer
      .asEval(TraceContext.empty)
      .value
      .inMemoryState
      .achsStateCache
      .get()

  private def getAchsStateFromDb(
      participantRef: LocalParticipantReference
  ): Option[AchsState] = {
    val ledgerApiStore =
      participantRef.underlying.value.sync.participantNodePersistentState.value.ledgerApiStore
    val storageBackendFactory = ledgerApiStore.ledgerApiDbSupport.storageBackendFactory
    val parameterStorageBackend =
      storageBackendFactory.createParameterStorageBackend(ledgerApiStore.stringInterningView)
    ledgerApiStore.ledgerApiDbSupport.dbDispatcher
      .executeSql(
        DatabaseMetrics.ForTesting("fetchACHSState")
      )(parameterStorageBackend.fetchACHSState)(
        LoggingContextWithTrace.empty
      )
      .futureValue
  }

  private def assertAchsStateIsAtLeast(actual: AchsState, expected: AchsState): Unit = {
    actual.validAt should be >= expected.validAt
    actual.lastPointers.lastRemoved should be >= expected.lastPointers.lastRemoved
    actual.lastPointers.lastPopulated should be >= expected.lastPointers.lastPopulated
  }

  "ACHS is not cleared during repair" in { implicit env =>
    import env.*

    val initialIous = 30

    withParticipantsInitialized { (alice, bob) =>
      withSynchronizerConnected(daName) {
        IouSyntax.createIous(participant1, alice, alice, 1 to initialIous)
      }

      // wait for participant1's ACHS to be populated
      eventually() {
        val achsState = getAchsStateFromCache(participant1)
        achsState.validAt should be > 0L
        achsState.lastPointers.lastRemoved shouldBe achsState.validAt
        achsState.lastPointers.lastPopulated should be > 0L
      }

      val achsDbStateBeforeRepair = getAchsStateFromDb(participant1).value
      val achsStateBeforeRepair = getAchsStateFromCache(participant1)
      assertAchsStateIsAtLeast(achsStateBeforeRepair, achsDbStateBeforeRepair)

      // create 30 contracts on participant2/acme to use as repair contracts.
      val repairContractCount = 30
      val repairContracts = (1 to repairContractCount).map { _ =>
        createContractInstance(participant2, acmeId, alice, bob)
      }

      // During repair: assert that existing ACHS state is not cleared, and
      // that normal indexer resumes from existing ACHS state after repair.
      val achsClearedLogMessage = "ACHS is disabled, clearing existing ACHS data"
      val achsResumingLogMessage = "ACHS resuming from existing state"

      loggerFactory.assertLogsSeq(achsLogRule)(
        participant1.repair.add(daId, testedProtocolVersion, repairContracts),
        logEntries => {
          // Verify ACHS data was NOT cleared during repair
          val achsClearedLogs =
            logEntries.filter(_.message.contains(achsClearedLogMessage))
          withClue(
            s"ACHS data should NOT be cleared during repair, but '$achsClearedLogMessage' was found. " +
              s"All ACHS-related log entries:\n${logEntries.map(_.message).mkString("\n")}."
          ) {
            achsClearedLogs shouldBe empty
          }

          // Verify normal indexer resumed from existing ACHS state after repair
          val achsResumingLogs =
            logEntries.filter(_.message.contains(achsResumingLogMessage))
          withClue(
            s"Expected '$achsResumingLogMessage' log after repair. " +
              s"All ACHS-related log entries:\n${logEntries.map(_.message).mkString("\n")}."
          ) {
            achsResumingLogs should not be empty
          }
        },
      )

      eventually() {
        // verify that the DB ACHS state was not cleared during repair
        val achsDbStateAfterRepair = getAchsStateFromDb(participant1).value

        val achsStateAfterRepair = getAchsStateFromCache(participant1)
        assertAchsStateIsAtLeast(achsStateAfterRepair, achsDbStateAfterRepair)

        // ACHS state should have advanced after repair
        achsStateAfterRepair.validAt should be > achsStateBeforeRepair.validAt
        achsDbStateAfterRepair.validAt should be > achsDbStateBeforeRepair.validAt
      }
    }
  }
}

final class AchsRepairIntegrationTestPostgres extends AchsRepairIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))
}
