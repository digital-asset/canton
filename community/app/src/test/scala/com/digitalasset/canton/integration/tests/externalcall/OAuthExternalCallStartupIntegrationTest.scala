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

sealed trait OAuthExternalCallStartupIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

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
        enableOAuthExternalCallExtension(
          extensionId = "startup-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
  }

  "oauth external call startup behavior" should {

    "avoid token and resource HTTP during participant startup and extension-manager construction" in {
      _ =>
      verifyTokenCallCount(0)
      mockServer.getTotalCallCount shouldBe 0
    }
  }
}

class OAuthExternalCallStartupIntegrationTestH2 extends OAuthExternalCallStartupIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class OAuthExternalCallStartupIntegrationTestPostgres
    extends OAuthExternalCallStartupIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
