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
import monocle.macros.syntax.lens.*

sealed trait OAuthExternalCallStartupValidationIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

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
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.engine.extensionSettings.validateExtensionsOnStartup)
            .replace(true)
            .focus(_.parameters.engine.extensionSettings.failOnExtensionValidationError)
            .replace(true)
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.engine.extensionSettings.validateExtensionsOnStartup)
            .replace(true)
            .focus(_.parameters.engine.extensionSettings.failOnExtensionValidationError)
            .replace(false)
        ),
        enableOAuthExternalCallExtension(
          extensionId = "startup-validation-strict-ext",
          port = startupMockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
        enableOAuthExternalCallExtension(
          extensionId = "startup-validation-lenient-ext",
          port = startupMockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant2",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
  }

  "oauth external call startup validation behavior" should {

    "perform OAuth startup remote validation by acquiring a token and then sending an authenticated resource validation request" in {
      implicit env =>
        import env.*

        resetMockServer()
        mockServer.setTokenSuccessHandler(accessToken = "startup-token", expiresIn = 120L)
        mockServer.setHandler("_health") { request =>
          request.mode shouldBe "validation"
          request.headers
            .get("Authorization")
            .flatMap(_.headOption) shouldBe Some("Bearer startup-token")
          ExternalCallResponse.ok("startup-ok")
        }

        participant1.start()

        verifyTokenCallCount(1)
        verifyCallCount("_health", 1)
    }

    "continue startup after invalid startup remote validation when failOnExtensionValidationError is false" in {
      implicit env =>
        import env.*

        resetMockServer()
        mockServer.setTokenErrorHandler(503, "issuer unavailable")

        participant2.start()

        verifyTokenCallCount(1)
        mockServer.getTotalCallCount shouldBe 0
    }

    "fail startup after invalid startup remote validation when failOnExtensionValidationError is true" in {
      implicit env =>
        import env.*

        resetMockServer()
        mockServer.setTokenErrorHandler(503, "issuer unavailable")

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          the[CommandFailure] thrownBy participant1.start(),
          logs => {
            logs.exists(_.message.contains("Extension startup remote validation failed")) shouldBe true
            logs.exists(_.message.contains("OAuth token acquisition failed")) shouldBe true
          },
        )

        verifyTokenCallCount(1)
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
