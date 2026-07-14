// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.{SequencerErrors, SubmissionRequestType}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.IndividualThroughputCapConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import monocle.macros.syntax.lens.*

class ThroughputCapIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_ { config =>
          config.sequencer match {
            case external: SequencerConfig.External =>
              config
                .focus(_.sequencer)
                .replace(
                  external
                    .focus(_.block.throughputCap.strict)
                    .replace(true)
                    .focus(_.block.throughputCap.enabled)
                    .replace(true)
                    .focus(_.block.throughputCap.delayedActivation)
                    .replace(false)
                    .focus(_.block.throughputCap.messages.confirmationRequest.globalTpsCap)
                    .replace(PositiveDouble.tryCreate(0.0001))
                )
            case _ =>
              throw new IllegalArgumentException("Not implemented for this type of sequencer")
          }
        }
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.dars.upload(CantonExamplesPath)

      }

  "throughput caps block submission request" in { implicit env =>
    import env.*

    val alice = participant1.parties.enable("Alice")
    // succeeds
    IouSyntax.createIou(participant1)(alice, alice)

    // fails
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      IouSyntax.createIou(participant1)(alice, alice),
      // TODO(#33973) This should not be logged four times!
      //   I can see at least two warnings, one info and the console error.
      //   One warning is fine, console error is fine. Others should not be logged again.
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.errorMessage should include(SequencerErrors.Overloaded.id),
    )

  }

  "modify caps" in { implicit env =>
    import env.*

    sequencer1.traffic_control.set_throughput_cap(
      SubmissionRequestType.ConfirmationRequest,
      IndividualThroughputCapConfig(
        globalTpsCap = PositiveDouble.tryCreate(60) // plenty
      ),
    )

    sequencer1.traffic_control.set_throughput_cap(
      SubmissionRequestType.ConfirmationResponse,
      IndividualThroughputCapConfig(
        globalTpsCap = PositiveDouble.tryCreate(0.001) // second will be rejected
      ),
    )

    val bob = participant1.parties.enable("Bob")
    // succeeds
    IouSyntax.createIou(participant1)(bob, bob)

  }

  "throughput caps block submission response" in { implicit env =>
    import env.*
    val bob = participant1.parties.find("Bob")
    // fails

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      IouSyntax.createIou(participant1)(
        bob,
        bob,
        optTimeout = Some(config.NonNegativeDuration.ofSeconds(5)),
      ),
      // TODO(#33973) This should not be logged four times!
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.errorMessage should include("DEADLINE_EXCEEDED"),
    )

  }
}
