// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.docs

import better.files.File
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import monocle.macros.syntax.lens.*

/** Base class for testing that the configuration files in the documentation can be parsed
  * successfully and, optionally, that the environment can be started successfully.
  *
  * @param configFile
  *   the path to the configuration file to test.
  * @param disableLapiVerificationForParticipants
  *   a set of participant names for which LAPI verification is disabled because the participants
  *   are either remote or not started.
  * @param startNodes
  *   whether to start the nodes in the environment after parsing the configuration. For
  *   configurations that use KMS or Postgres, we recommend setting this to false to avoid
  *   generating KMS keys automatically or to avoid conflicts with other tests.
  */
abstract class DocsCantonNetworkConfigTest(
    configFile: String,
    disableLapiVerificationForParticipants: Set[String] = Set.empty,
    startNodes: Boolean = true,
) extends CommunityIntegrationTest
    with IsolatedEnvironments {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(File(configFile))
      .focus(_.testingConfig.participantsWithoutLapiVerification)
      .replace(disableLapiVerificationForParticipants)
      .setManualStart(!startNodes)

  "Configuration Reference" should {
    if (startNodes) {
      "parse config and start environment successfully" in { _ =>
        succeed
      }
    } else {
      "parse config successfully" in { _ =>
        succeed
      }
    }
  }

}

class DocsCantonNetworkConfigurationReferenceScalaTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-appdev_configuration_reference.conf",
      disableLapiVerificationForParticipants = Set("participant1"),
      startNodes = false, // Do not start nodes because configuration uses Postgres
    )

class DocsCantonNetworkScriptingConfigScalaTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-global-synchronizer_scripting.conf",
      disableLapiVerificationForParticipants = Set("myparticipant"),
    )

class DocsCantonNetworkConfigurationConfigScalaTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-global-synchronizer_configuration.conf",
      disableLapiVerificationForParticipants = Set("participant"),
      startNodes = false, // Do not start nodes because configuration uses KMS
    )

class DocsCantonNetworkPerformanceOptimizationConfigScalaTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-global-synchronizer_performance_optimization.conf"
    )

class DocsCantonNetworkGlobalSynchronizerReferenceCantonConsoleReferenceConfigTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-global-synchronizer_canton_console_reference.conf",
      disableLapiVerificationForParticipants = Set("participant"),
    )

class DocsCantonNetworkGlobalSynchronizerReferenceCantonConsoleCliConfigTest
    extends DocsCantonNetworkConfigTest(
      "community/app/src/test/resources/documentation-snippets/docs-cn-global-synchronizer_cli_tools_canton_console.conf",
      disableLapiVerificationForParticipants = Set("participant"),
    )
