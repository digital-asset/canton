// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.util.MultiSynchronizerFeatureFlag
import com.digitalasset.canton.integration.{
  BaseIntegrationTest,
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.logging.SuppressionRule
import org.slf4j.event.Level

import scala.sys.process.Process

class MultisyncExampleIntegrationTest
    extends BaseIntegrationTest
    with IsolatedEnvironments
    with CommunityIntegrationTest {

  private val multisyncTestResources: File =
    "community" / "app" / "src" / "test" / "resources" / "examples" / "14-multisync"

  // Synchronizer aliases matching the sandbox bootstrap naming convention (with hyphens)
  private val sync1Name = SynchronizerAlias.tryCreate("synchronizer-1")
  private val sync2Name = SynchronizerAlias.tryCreate("synchronizer-2")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .addConfigTransforms(
        ConfigTransforms.enableInteractiveSubmissionTransforms
      )
      .prependConfigTransform(ConfigTransforms.enableHttpLedgerApi)
      .withNetworkBootstrap { implicit env =>
        import env.*
        // Bootstrap two synchronizers with sandbox-style names (synchronizer-1, synchronizer-2)
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            sync1Name,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          ),
          NetworkTopologyDescription(
            sync2Name,
            synchronizerOwners = Seq(sequencer2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer2),
            mediators = Seq(mediator2),
          ),
        )
      }
      .withSetup { implicit env =>
        import env.*

        // Connect participant1 and participant2 to both synchronizers (like sandbox/pebblebox)
        participant1.synchronizers.connect_local(sequencer1, alias = sync1Name)
        participant1.synchronizers.connect_local(sequencer2, alias = sync2Name)
        participant2.synchronizers.connect_local(sequencer1, alias = sync1Name)
        participant2.synchronizers.connect_local(sequencer2, alias = sync2Name)

        // Connect participant3 only to synchronizer-2 (like sidebox — only sees S2)
        participant3.synchronizers.connect_local(sequencer2, alias = sync2Name)

        // Enable multi-synchronizer feature flag on all participants for their connected synchronizers
        val s1Id = getInitializedSynchronizer(sync1Name).physicalSynchronizerId.logical
        val s2Id = getInitializedSynchronizer(sync2Name).physicalSynchronizerId.logical
        MultiSynchronizerFeatureFlag.enable(Seq(participant1, participant2), s1Id)
        MultiSynchronizerFeatureFlag.enable(Seq(participant1, participant2, participant3), s2Id)
      }

  // Expected error message patterns from scenario 4 (missing party) and scenario 5 (missing vetting)
  private val expectedErrorPatterns: Seq[String] = Seq(
    "not active on the target synchronizer", // Scenario 4: party not hosted on target
    "has not vetted", // Scenario 5: package not vetted on target
  )

  "run the multisync TypeScript scenarios" in { implicit env =>
    import env.*

    // Suppress only ERROR logs that match expected failure patterns from scenario 4 and 5,
    loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.ERROR))(
      {
        val v1Port = participant1.config.httpLedgerApi.port.unwrap.toString
        val v2Port = participant2.config.httpLedgerApi.port.unwrap.toString
        val v3Port = participant3.config.httpLedgerApi.port.unwrap.toString

        val darPath = ModelTestsPath

        val processLogger: BufferedProcessLogger = mkProcessLogger()

        // Install npm dependencies
        val installExitCode = Process(
          Seq("npm", "install", "--loglevel", "error"),
          cwd = multisyncTestResources.toJava,
        ).!(processLogger)

        if (installExitCode != 0) {
          fail(s"npm install failed (exit code $installExitCode):\n\n${processLogger.output()}")
        }

        // Generate code and compile TypeScript
        val buildExitCode = Process(
          Seq("npm", "run", "build"),
          cwd = multisyncTestResources.toJava,
        ).!(processLogger)

        if (buildExitCode != 0) {
          fail(s"npm run build failed (exit code $buildExitCode):\n\n${processLogger.output()}")
        }

        val exitCode = Process(
          Seq("npm", "run", "scenario"),
          cwd = multisyncTestResources.toJava,
          "V1_PORT" -> v1Port,
          "V2_PORT" -> v2Port,
          "V3_PORT" -> v3Port,
          "DAR_PATH" -> darPath,
        ).!(processLogger)

        val output = processLogger.output()

        if (exitCode != 0) {
          fail(s"Multisync scenarios failed (exit code $exitCode):\n\n$output")
        }

        // Verify no error logs were emitted by any scenario
        output should not include ("✗")

        // Scenario 1: Allocations (uploads DAR to all participants and synchronizers)
        output should include("Scenario 1 complete!")
        output should include("P1 allocated")
        output should include("DAR uploaded on V1 for S1")
        output should include("DAR uploaded on V1 for S2")

        // Scenario 2: Manual reassignment
        output should include("Scenario 2 complete!")
        output should include("Unassigned, reassignmentId")
        output should include("Assigned to S2")

        // Scenario 3: Automatic reassignment
        output should include("Scenario 3 complete!")
        output should include("Auto-reassignment succeeded")
        output should include("after auto-reassignment")

        // Scenario 4: Failed reassignment — missing party
        output should include("Scenario 4 complete!")
        output should include("Unassign correctly rejected")
        output should include("not active on the target synchronizer")

        // Scenario 5: Failed reassignment — missing vetting (unvets then re-vets package on S2)
        output should include("Scenario 5 complete!")
        output should include("Unvetted on V1/S2")
        output should include("has not vetted")
        output should include("Re-vetted on V1/S2")

        output should include("All scenarios complete!")
      },
      { suppressedErrors =>
        // Verify that each suppressed error matches one of the expected patterns
        forEvery(suppressedErrors) { entry =>
          assert(
            expectedErrorPatterns.exists(pattern => entry.message.contains(pattern)),
            s"Unexpected ERROR log detected: ${entry.message}",
          )
        }

        // Verify that the Canton server actually emitted the expected errors
        forEvery(expectedErrorPatterns) { pattern =>
          assert(
            suppressedErrors.exists(_.message.contains(pattern)),
            s"Expected an ERROR log containing '$pattern' but none was found among ${suppressedErrors.size} suppressed entries",
          )
        }
      },
    )
  }
}
