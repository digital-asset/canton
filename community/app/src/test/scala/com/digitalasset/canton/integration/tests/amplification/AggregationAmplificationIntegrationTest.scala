// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.amplification

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise

/** Test that early rejection of aggregations are correctly handled
  *
  * We reduce the message load on the sequencer by filtering the aggregations at submission time,
  * producing synchronous rejects.
  *
  * This test checks that the rejection is properly propagated and handled at the integration level.
  */
class AggregationAmplificationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .addConfigTransforms(
        ConfigTransforms.updateMediatorConfig("mediator1")(
          // send verdict immediately (we'll hold it back in the sequencer)
          _.focus(_.parameters.delayedVerdictSender.livenessMargin)
            .replace(NonNegativeInt.one)
            .focus(_.parameters.delayedVerdictSender.delay)
            .replace(config.NonNegativeFiniteDuration.ofMillis(250))
        ),
        ConfigTransforms.updateMediatorConfig("mediator2")(
          // no delayed verdict sender for m2
          _.focus(_.parameters.delayedVerdictSender.enabled).replace(false)
        ),
      )
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(
          S2M2.copy(mediatorThreshold = PositiveInt.one)
        )
      }
      .withSetup { implicit env =>
        import env.*

        mediator1.sequencer_connection.modify_connections { old =>
          SequencerConnections.tryMany(
            old.connections,
            old.sequencerTrustThreshold,
            old.sequencerLivenessMargin,
            SubmissionRequestAmplification(
              PositiveInt.tryCreate(5),
              config.NonNegativeFiniteDuration.ofSeconds(1), // don't send immediately
            ),
            old.sequencerConnectionPoolDelays,
          )
        }

      }

  "validate that mediators properly react to already exist synchronous rejections" in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local_bft(sequencers.all, daName)
      participant1.dars.upload(CantonExamplesPath)
      val alice = participant1.parties.enable("alice", synchronizer = daName)
      val promise = Promise[Unit]()
      val mediatorResponseCounter = new AtomicInteger(0)
      sequencers.local.foreach { sequencerRef =>
        val sequencer = getProgrammableSequencer(sequencerRef.name)
        sequencer.setPolicy_("delay verdict of one mediator") { submissionRequest =>
          if (submissionRequest.sender == mediator1.id) {
            // hold back the first response from mediator1
            if (mediatorResponseCounter.getAndIncrement() == 0) {
              SendDecision.HoldBack(promise.future)
            } else {
              // fail
              logger.error(
                "Received a second response from the same mediator but the mediator should have aborted the retries!"
              )
              SendDecision.Process
            }
          } else {
            SendDecision.Process
          }
        }
      }
      // we don't want to see a warning or an error in the logs
      loggerFactory.assertLogs(SuppressionRule.LevelAndAbove(Level.WARN))({
        IouSyntax.createIou(participant1)(alice, alice)
        promise.success(())
        Threading.sleep(5000)
      })
      assert(mediatorResponseCounter.get() == 1, "Expected only one response from the mediator")

  }

}
