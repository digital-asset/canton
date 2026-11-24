// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.{SequencerErrors, SubmissionRequestType}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.ThroughputCapByMessageTypeConfig
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig
import monocle.macros.syntax.lens.*

class ThroughputCapIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_ { config =>
          config.sequencer match {
            case cfg: SequencerConfig.BftSequencer =>
              config
                .focus(_.sequencer)
                .replace(
                  cfg
                    .focus(_.block.throughputCap.strict)
                    .replace(true)
                    .focus(_.block.throughputCap.enabled)
                    .replace(true)
                    .focus(_.block.throughputCap.messages)
                    .modify(messages =>
                      messages.updated(
                        "confirmation-request",
                        messages("confirmation-request").copy(
                          globalTpsCap = PositiveDouble.tryCreate(0.0001)
                        ),
                      )
                    )
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
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.errorMessage should include(SequencerErrors.Overloaded.id),
    )

  }

  "modify caps" in { implicit env =>
    import env.*

    sequencer1.traffic_control.set_throughput_cap(
      "confirmation-request",
      ThroughputCapByMessageTypeConfig(
        requestType = SubmissionRequestType.ConfirmationRequest,
        globalTpsCap = PositiveDouble.tryCreate(60), // plenty
      ),
    )

    sequencer1.traffic_control.set_throughput_cap(
      "confirmation-response",
      ThroughputCapByMessageTypeConfig(
        requestType = SubmissionRequestType.ConfirmationResponse,
        globalTpsCap = PositiveDouble.tryCreate(0.001), // second will be rejected
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
      _.warningMessage should include(SequencerErrors.Overloaded.id),
      _.errorMessage should include("DEADLINE_EXCEEDED"),
    )

  }

  "manage independent named cap settings through the API" in { implicit env =>
    import env.*
    val trafficControl = sequencer1.traffic_control
    val original = trafficControl.list_throughput_caps()
    val setting = ThroughputCapByMessageTypeConfig(
      requestType = SubmissionRequestType.ConfirmationRequest,
      observationPeriodSeconds = PositiveInt.tryCreate(120),
    )
    trafficControl.set_throughput_cap("requests-long-window", setting)
    trafficControl.get_throughput_cap("requests-long-window") shouldBe Some(setting)
    trafficControl.list_throughput_caps() shouldBe original.updated("requests-long-window", setting)
    trafficControl.update_throughput_cap(
      "requests-long-window",
      _.copy(observationPeriodSeconds = PositiveInt.tryCreate(30)),
    )
    trafficControl
      .get_throughput_cap("requests-long-window")
      .value
      .observationPeriodSeconds
      .value shouldBe 30
    trafficControl.unset_throughput_cap("requests-long-window")
    trafficControl.get_throughput_cap("requests-long-window") shouldBe None
    trafficControl.list_throughput_caps() shouldBe original
  }
}
