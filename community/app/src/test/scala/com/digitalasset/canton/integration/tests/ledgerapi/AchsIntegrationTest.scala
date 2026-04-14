// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, SuppressionRule}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.AchsState
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil.withResource
import org.slf4j.event.Level

trait AchsIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }

  private val achsLogRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("InitializeParallelIngestion") && SuppressionRule
      .LevelAndAbove(Level.INFO)

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

  private def stopAllNodes(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participants.all.synchronizers.disconnect(daName)
    nodes.local.stop()
  }

  private def startAllNodes(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    nodes.local.start()
    participants.all.synchronizers.reconnect_all()
  }

  /** Create a new environment with ACHS enabled or disabled, preserving DB state. */
  private def restart(
      oldEnv: TestConsoleEnvironment,
      enableAchs: Boolean,
  )(f: TestConsoleEnvironment => Unit): Unit = {
    stopAllNodes(oldEnv)
    val achsTransform =
      if (enableAchs) ConfigTransforms.enableAchs
      else ConfigTransforms.disableAchs
    val newEnv = manualCreateEnvironmentWithPreviousState(
      oldEnv.actualConfig,
      _ => achsTransform(oldEnv.actualConfig),
    )
    withResource(newEnv) { env =>
      handleStartupLogs(startAllNodes(env))
      f(env)
    }
  }

  "ACHS resumes from existing state after participant restart" in { implicit env =>
    import env.*

    // Do some pings to ensure that the ACHS is initialized
    for (_ <- 1 to 10) {
      participant1.health.ping(participant1)
    }

    // Wait for ACHS to be populated
    eventually() {
      val achsState = getAchsStateFromCache(participant1)
      achsState.validAt should be > 0L
      achsState.lastPointers.lastPopulated should be > 0L
    }

    // Capture ledger end and ACS for all parties before restart
    val offset = participant1.ledger_api.state.end()
    val acsBeforeRestart = participant1.ledger_api.state.acs.of_all(activeAtOffsetO = Some(offset))

    val resumingLogMessage = "ACHS resuming from existing state"

    loggerFactory.assertLogsSeq(achsLogRule)(
      {
        participant1.stop()
        participant1.start()
        participant1.synchronizers.reconnect_all()
      },
      logEntries => {
        val achsResumingLogs =
          logEntries.filter(_.message.contains(resumingLogMessage))
        withClue(
          s"Expected to find '$resumingLogMessage' log message after restart. " +
            s"All ACHS-related log entries: ${logEntries.map(_.message).mkString("\n")}"
        ) {
          achsResumingLogs should not be empty
        }
      },
    )

    val acsAfterRestart =
      participant1.ledger_api.state.acs.of_all(activeAtOffsetO = Some(offset))

    acsAfterRestart should contain theSameElementsAs acsBeforeRestart
  }

  "ACHS data is cleared when restarting with ACHS disabled" in { implicit env =>
    import env.*
    // Wait for ACHS to be populated
    eventually() {
      val achsState = getAchsStateFromCache(participant1)
      achsState.validAt should be > 0L
      achsState.lastPointers.lastPopulated should be > 0L
    }

    val achsDbStateBeforeDisable = getAchsStateFromDb(participant1)
    achsDbStateBeforeDisable shouldBe defined
    achsDbStateBeforeDisable.value.validAt should be > 0L

    val offsetBeforeDisable = participant1.ledger_api.state.end()
    val acsBeforeDisable =
      participant1.ledger_api.state.acs.of_all(activeAtOffsetO = Some(offsetBeforeDisable))

    // Restart with ACHS disabled — expect ACHS data to be cleared
    restart(env, enableAchs = false) { newEnv =>
      import newEnv.*

      // Verify ACHS state in cache is zeroed out (disabled)
      val achsStateAfterDisable = getAchsStateFromCache(participant1)
      achsStateAfterDisable.validAt shouldBe 0L
      achsStateAfterDisable.lastPointers.lastRemoved shouldBe 0L
      achsStateAfterDisable.lastPointers.lastPopulated shouldBe 0L

      // Verify ACHS state was cleared from DB
      val achsDbStateAfterDisable = getAchsStateFromDb(participant1)
      achsDbStateAfterDisable shouldBe None

      // Verify ACS is still intact
      val acsAfterDisable =
        participant1.ledger_api.state.acs.of_all(activeAtOffsetO = Some(offsetBeforeDisable))
      acsAfterDisable should contain theSameElementsAs acsBeforeDisable

      // Do some more pings to generate data while ACHS is disabled
      for (_ <- 1 to 10) {
        participant1.health.ping(participant1)
      }

      val offsetBeforeReEnable = participant1.ledger_api.state.end()
      val acsBeforeReEnable =
        participant1.ledger_api.state.acs.of_all(activeAtOffsetO = Some(offsetBeforeReEnable))

      // Restart with ACHS re-enabled — expect ACHS to be re-created from scratch
      restart(newEnv, enableAchs = true) { reEnabledEnv =>
        import reEnabledEnv.*

        // Wait for ACHS to be populated again
        eventually() {
          val achsStateReEnabled = getAchsStateFromCache(participant1)
          achsStateReEnabled.validAt should be > 0L
          achsStateReEnabled.lastPointers.lastPopulated should be > 0L
        }

        // Verify ACHS state is back in DB
        val achsDbStateReEnabled = getAchsStateFromDb(participant1)
        achsDbStateReEnabled shouldBe defined
        achsDbStateReEnabled.value.validAt should be > 0L

        // Verify ACS is still intact
        val acsAfterReEnable =
          participant1.ledger_api.state.acs
            .of_all(activeAtOffsetO = Some(offsetBeforeReEnable))
        acsAfterReEnable should contain theSameElementsAs acsBeforeReEnable
      }
    }
  }
}

class AchsIntegrationTestPostgres extends AchsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
