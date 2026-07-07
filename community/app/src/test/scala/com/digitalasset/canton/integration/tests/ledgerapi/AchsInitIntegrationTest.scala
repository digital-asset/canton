// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong}
import com.digitalasset.canton.examples.java.iou.Dummy
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.util.ResourceUtil.withResource
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

trait AchsInitIntegrationTest extends CommunityIntegrationTest with IsolatedEnvironments {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.disableAchs,
        ConfigTransforms.disableAdditionalConsistencyChecks,
      )

  private val achsLogRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("InitializeParallelIngestion") && SuppressionRule
      .LevelAndAbove(Level.INFO)

  private def stopAllNodes(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participants.all.synchronizers.disconnect(daName)
    nodes.local.stop()
  }

  "ACHS initialization is interrupted when participant is shut down during startup" when {
    "shutdown is triggered via participant.stop()" in { implicit env =>
      runAchsInitInterruptionTest { participant =>
        participant.stop()
      }
    }

    "shutdown is triggered via participant.close()" in { implicit env =>
      runAchsInitInterruptionTest { participant =>
        // Bypass Nodes.stopAndWait and close the bootstrap directly. This
        // exercises the isClosing-driven cancellation path (no
        // cancelInitializationHint() call): OnShutdownRunner.close() flips
        // isClosing as its first action, which the ACHS init pipe observes
        // via externalShutdownSignal.
        val bootstrap = participant.consoleEnvironment.environment.participants
          .getStarting(participant.name)
          .getOrElse(fail(s"participant ${participant.name} is not in StartingUp state"))
        bootstrap.close()
      }
    }
  }

  private def runAchsInitInterruptionTest(
      triggerShutdown: com.digitalasset.canton.console.LocalParticipantReference => Unit
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)

    participant1.dars.upload(CantonTestsPath)

    val alice = participant1.parties.enable("Alice")

    // Generate a lot of ACHS work (while ACHS is disabled) so that the
    // eventual ACHS initialization, with `initParallelism = 1` and
    // `initAggregationThreshold = 1`, takes a meaningful amount of time and
    // can be reliably interrupted by a shutdown mid-init.
    val createAndArchiveDummy =
      new Dummy(alice.toProtoPrimitive)
        .createAnd()
        .exerciseArchive()
        .commands
        .asScala
        .toSeq
    val commandsPerTx = 100
    val txCount = 20
    for (i <- 1 to txCount) {
      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        (1 to commandsPerTx).flatMap(_ => createAndArchiveDummy),
        commandId = s"setup-dummy-$i",
      )
    }

    val slowInitAchsConfig = AchsConfig(
      validAtDistanceTarget = NonNegativeLong.tryCreate(10L),
      lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(5L),
      aggregationThreshold = 5L,
      initParallelism = NonNegativeInt.tryCreate(1),
      initAggregationThreshold = 1L,
    )
    val slowInitAchsTransform: ConfigTransform =
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.ledgerApiServer.indexer.achsConfig)
          .replace(Some(slowInitAchsConfig))
      )
    // Force manual start so we drive participant startup ourselves and can race
    // a shutdown against ACHS initialization.
    val manualStartTransform: ConfigTransform =
      c => c.copy(parameters = c.parameters.copy(manualStart = true))

    stopAllNodes(env)
    val newEnv = manualCreateEnvironmentWithPreviousState(
      env.actualConfig,
      _ => manualStartTransform(slowInitAchsTransform(env.actualConfig)),
    )
    withResource(newEnv) { achsEnv =>
      import achsEnv.*

      sequencer1.start()
      mediator1.start()

      val interruptedMessage = "ACHS snapshot initialization interrupted by shutdown request"
      loggerFactory.assertLogsSeq(achsLogRule)(
        {
          // Start the participant in the background, start() blocks on full init,
          // which in turn blocks on ACHS initialization completing.
          val startF = Future(participant1.start())

          eventually(60.seconds, maxPollInterval = 100.millis) {
            loggerFactory.fetchRecordedLogEntries.exists(
              _.message.contains("Initializing ACHS snapshot")
            ) shouldBe true
          }

          triggerShutdown(participant1)
          scala.util.Try(Await.result(startF, 5.minute))
        },
        logEntries => {
          val interruptedLogs =
            logEntries.filter(_.message.contains(interruptedMessage))
          withClue(
            s"Expected '$interruptedMessage' log message during shutdown. " +
              s"All ACHS-related log entries:\n${logEntries.map(_.message).mkString("\n")}"
          ) {
            interruptedLogs should not be empty
          }
        },
      )
    }
  }
}

class AchsInitIntegrationTestPostgres extends AchsInitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
