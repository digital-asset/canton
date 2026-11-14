// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.config
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.UseBftSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt

/** Trivial test which can be used as a first end to end test */
trait ApiInfoIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Make synchronizer connection fail faster
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          _.focus(_.maxConnectionRetryDelay).replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        ),
        ConfigTransforms.setSequencerInfoTimeout(2.second),
      )

  "port misconfiguration" should {
    "raise meaningful error message" in { implicit env =>
      import env.*

      val adminPort = sequencer1.config.adminApi.port
      val apiExpected =
        s"provides '${CantonGrpcUtil.ApiName.AdminApi}', expected '${CantonGrpcUtil.ApiName.SequencerPublicApi}'"
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers
          .connect(daName, s"http://localhost:$adminPort", manualConnect = true),
        logEntry => { // Connection warning
          logEntry.loggerName should include(daName.unwrap)
          logEntry.warningMessage should (include("Validation failure") and include(apiExpected))
        },
        _.commandFailureMessage should ( // Command error
          include(s"localhost:$adminPort") and
            include(apiExpected) and
            include("This message indicates a possible mistake in configuration") and
            include(s"please check node connection settings")
        ),
      )
    }
  }
}

class ApiInfoIntegrationTestInMemory extends ApiInfoIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseBftSequencer(loggerFactory))

}
