// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import com.digitalasset.canton.integration.plugins.{UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.performance.scenarios.CantonTesting
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt

class CantonTestingIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numSequencers = 1,
        numMediators = 2,
        numParticipants = 2,
        withRemote = true,
      )
      .addConfigTransforms(
        ConfigTransforms.enableReplicatedMediators(),
        ConfigTransforms.enableReplicatedParticipants(),
        ConfigTransforms.disableAdditionalConsistencyChecks,
      )
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "remoteParticipant1",
            "remoteParticipant2",
          )
        )
      )
      .withManualStart

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    UseSharedStorage.forMediators(
      "mediator1",
      Seq("mediator2"),
      loggerFactory,
    )
  )

  registerPlugin(
    UseSharedStorage.forParticipants(
      "participant1",
      Seq("participant2"),
      loggerFactory,
    )
  )

  "canton-testing startup scripts" should {
    "start the synchronizer" in { implicit env =>
      CantonTesting.runSynchronizers(sequencerName = "sequencer1", mediatorName = "mediator1")
    }
    "start the ha synchronizer nodes" in { implicit env =>
      CantonTesting.runSynchronizersSec(
        sequencerName = "remoteSequencer1",
        mediatorName = "remoteMediator1",
        mediatorSecName = "mediator2",
      )
    }
    "start the participants" in { implicit env =>
      CantonTesting.runParticipants(
        sequencerHost = "localhost",
        sequencerPort = env.sequencer1.config.publicApi.port.unwrap.toString,
      )
    }

    "start participant sec" in { implicit env =>
      CantonTesting.runParticipantsSec(awaitParticipants =
        _.remote.active
      ) // doesn't really do anything
    }

    "run perf runners" in { implicit env =>
      CantonTesting.runPerformanceRunners(
        maxTestDuration = 10.seconds,
        selectParticipants = _.remote.active,
        numAssetsPerIssuer = 20,
        batchSize = 3,
        payloadSize = 0,
        repositoryRoot = ".",
      )
    }

  }

}
