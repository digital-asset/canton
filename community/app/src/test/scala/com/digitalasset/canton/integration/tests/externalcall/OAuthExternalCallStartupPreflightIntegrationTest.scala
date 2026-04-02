// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}

import java.nio.file.{Files, Path}

sealed trait OAuthExternalCallStartupPreflightIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

  private lazy val missingTrustCollectionFile: Path = {
    val missingFile = Files.createTempFile("external-call-oauth-startup-missing-trust", ".crt")
    Files.deleteIfExists(missingFile)
    missingFile
  }

  override def environmentDefinition: EnvironmentDefinition = {
    val startupMockServerPort = MockExternalCallServer.findFreePort()
    val startupObservedBase = EnvironmentDefinition.P2S1M1_Manual.withManualStart
      .withSetup { _ =>
        mockServer = new MockExternalCallServer(
          port = startupMockServerPort,
          loggerFactory = loggerFactory,
          useTls = true,
          tokenEndpointPath = Some(tokenEndpointPath),
        )
        mockServer.start()
      }
      .withTeardown { _ =>
        shutdownMockServer()
      }

    externalCallEnvironmentDefinition(
      base = startupObservedBase,
      startParticipantsInSetup = false,
    )
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableOAuthExternalCallExtension(
          extensionId = "startup-missing-trust-ext",
          port = startupMockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = missingTrustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
  }

  "oauth external call startup local preflight" should {

    "fail participant startup on invalid local OAuth trust material before any startup HTTP" in {
      implicit env =>
        import env.*

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          the[CommandFailure] thrownBy participant1.start(),
          logs => {
            logs.exists(_.message.contains("Extension startup local preflight failed")) shouldBe true
            logs.exists(_.message.contains("startup-missing-trust-ext")) shouldBe true
          },
        )

        verifyTokenCallCount(0)
        mockServer.getTotalCallCount shouldBe 0
    }
  }
}

class OAuthExternalCallStartupPreflightIntegrationTestH2
    extends OAuthExternalCallStartupPreflightIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class OAuthExternalCallStartupPreflightIntegrationTestPostgres
    extends OAuthExternalCallStartupPreflightIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
