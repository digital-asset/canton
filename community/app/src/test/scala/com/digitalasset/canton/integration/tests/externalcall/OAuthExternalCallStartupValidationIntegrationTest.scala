// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*

import java.nio.file.{Files, Path}

sealed trait OAuthExternalCallStartupValidationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

  private lazy val missingOauthPrivateKeyFile: Path = {
    val missingFile = Files.createTempFile("external-call-oauth-startup-missing-key", ".der")
    Files.deleteIfExists(missingFile)
    missingFile
  }

  override def environmentDefinition: EnvironmentDefinition = {
    val startupObservedBase = EnvironmentDefinition.P2S1M1_Manual.withSetup { _ =>
      mockServer = new MockExternalCallServer(
        port = mockServerPort,
        loggerFactory = loggerFactory,
        useTls = true,
        tokenEndpointPath = Some(tokenEndpointPath),
      )
      mockServer.start()
    }

    externalCallEnvironmentDefinition(startupObservedBase)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.extensionSettings.validateExtensionsOnStartup)
            .replace(true)
            .focus(_.parameters.engine.extensionSettings.failOnExtensionValidationError)
            .replace(true)
        ),
        enableOAuthExternalCallExtension(
          extensionId = "startup-validation-ext",
          port = mockServerPort,
          privateKeyFile = missingOauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
  }

  "oauth external call startup validation behavior" should {

    "preserve existing startup validation semantics without introducing OAuth-specific startup gating" in {
      _ =>
      verifyTokenCallCount(0)
      mockServer.getTotalCallCount shouldBe 0
    }
  }
}

class OAuthExternalCallStartupValidationIntegrationTestH2
    extends OAuthExternalCallStartupValidationIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class OAuthExternalCallStartupValidationIntegrationTestPostgres
    extends OAuthExternalCallStartupValidationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
