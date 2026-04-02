// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

sealed trait OAuthEchoModeExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ExternalCallIntegrationTestBase
    with MockServerSetup
    with OAuthExternalCallTestFiles {

  override def environmentDefinition: EnvironmentDefinition =
    externalCallEnvironmentDefinition(EnvironmentDefinition.P2S1M1_Manual)
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.extensionSettings.echoMode).replace(true)
        ),
        enableOAuthExternalCallExtension(
          extensionId = "echo-mode-ext",
          port = mockServerPort,
          privateKeyFile = oauthPrivateKeyFile,
          trustCollectionFile = trustCollectionFile,
          participantName = "participant1",
          tokenEndpointPath = tokenEndpointPath,
        ),
      )
      .withSetup { implicit env =>
        import env.*

        mockServer = new MockExternalCallServer(
          port = mockServerPort,
          loggerFactory = loggerFactory,
          useTls = true,
          tokenEndpointPath = Some(tokenEndpointPath),
        )
        mockServer.start()

        participant1.synchronizers.connect_local(sequencer1, daName)
        participant2.synchronizers.connect_local(sequencer1, daName)
        participant1.dars.upload(externalCallTestDarPath)
        participant2.dars.upload(externalCallTestDarPath)
        alice = participant1.parties.enable("alice")
      }

  private def createExternalCallContract()(implicit env: FixtureParam) = {
    import env.*
    val createTx = participant1.ledger_api.javaapi.commands.submit(
      Seq(alice),
      new E.ExternalCallContract(
        alice.toProtoPrimitive,
        java.util.List.of(),
      ).create.commands.asScala.toSeq,
    )
    JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id
  }

  "oauth external call echo mode" should {

    "short-circuit token and resource HTTP while still completing the business request" in {
      implicit env =>
        import env.*

        resetMockServer()
        val contractId = createExternalCallContract()

        val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          contractId.exerciseCallExternal(
            "echo-mode-ext",
            "echo",
            "00000000",
            toHex("oauth-echo-mode"),
          ).commands.asScala.toSeq,
        )

        exerciseTx.getUpdateId should not be empty
        verifyTokenCallCount(0)
        mockServer.getTotalCallCount shouldBe 0
      }
  }
}

class OAuthEchoModeExternalCallIntegrationTestH2 extends OAuthEchoModeExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class OAuthEchoModeExternalCallIntegrationTestPostgres
    extends OAuthEchoModeExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
