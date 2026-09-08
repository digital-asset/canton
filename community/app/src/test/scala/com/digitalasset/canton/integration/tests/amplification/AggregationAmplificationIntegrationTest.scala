// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.amplification

import com.digitalasset.canton.admin.api.client.data.SubmissionRequestAmplification
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
import com.digitalasset.canton.sequencing.protocol.SubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  Sequencer,
}
import com.digitalasset.canton.topology.{MediatorId, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
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
        mediator1.sequencer_connection.modify_connections(
          _.withSubmissionRequestAmplification(
            SubmissionRequestAmplification(
              PositiveInt.tryCreate(5),
              config.NonNegativeFiniteDuration.ofSeconds(1), // don't send immediately
            )
          )
        )
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

  "validate that participants properly react to already exists sync rejections" onlyRunWithOrGreaterThan
    ProtocolVersion.v36 in { implicit env =>
      import env.*

      val seq1 = getProgrammableSequencer(sequencer1.name)
      val seq2 = getProgrammableSequencer(sequencer2.name)

      val numRequests = new AtomicInteger(0)
      val numResponses = new AtomicInteger(0)

      val delayRequestSync = Promise[Unit]()
      val delayResponseSync = Promise[Unit]()
      val delayRequestAsync = Promise[Unit]()
      val delayResponseAsync = Promise[Unit]()

      // the following test will validate that both request and responses are properly amplified,
      // but deal with the synchronous rejections properly.
      //
      // however, the amplification will only happen if the synchronous request is processed.
      // therefore, we need to perform a bit of a dance between holding back on the post processing
      // versus synchronous part.
      //
      // we receive r1 => we process it sync, but block async
      // we receive r1' => we hold it back sync but release the r1 held back async
      // we receive v1 => we process it sync, but block async, we also release r1' held sync
      // we receive v1' => we hold it back sync, but release v1 held back async
      // we receive m1 => we release v1' held back sync
      def sendPolicy(sequencer: Sequencer, name: String)(
          submissionRequest: SubmissionRequest
      ): SendDecision = {
        logger.debug(s"Custom Policy on $name received $submissionRequest")
        if (submissionRequest.sender == participant1.id) {
          if (submissionRequest.isConfirmationRequest) {
            numRequests.getAndIncrement() match {
              case 0 =>
                sequencer.applyPostProcessingLockForTesting(delayRequestAsync.future)
                SendDecision.Process
              case 1 =>
                delayRequestAsync.trySuccess(())
                SendDecision.HoldBack(delayRequestSync.future)
              case _ =>
                logger.error("Unexpected confirmation request from participant1")
                SendDecision.Process
            }
          } else if (submissionRequest.isConfirmationResponse) {
            numResponses.getAndIncrement() match {
              case 0 =>
                sequencer.applyPostProcessingLockForTesting(delayResponseAsync.future)
                delayRequestSync.trySuccess(())
                SendDecision.Process
              case 1 =>
                delayResponseAsync.trySuccess(())
                SendDecision.HoldBack(delayResponseSync.future)
              case _ =>
                logger.error("Unexpected confirmation response from participant1")
                SendDecision.Process
            }
          } else SendDecision.Process
        } else if (
          // if the submission request is a verdict, release the response
          submissionRequest.sender.code == MediatorId.Code && submissionRequest.batch.allMembers
            .map(_.code)
            .contains(ParticipantId.Code)
        ) {
          delayResponseSync.trySuccess(())
          SendDecision.Process
        } else SendDecision.Process
      }

      seq1.setPolicy_("trigger amplification on s1")(
        sendPolicy(sequencer1.underlying.value.sequencer.sequencer, "sequencer1")(_)
      )
      seq2.setPolicy_("trigger amplification on s2")(
        sendPolicy(sequencer2.underlying.value.sequencer.sequencer, "sequencer2")(_)
      )

      // modify synchronizer connection so we amplify the confirmation response immediately
      val cfg = participant1.synchronizers.list_registered().loneElement._1
      val alias = cfg.synchronizerAlias
      participant1.synchronizers.modify(
        alias,
        cfg =>
          cfg.copy(sequencerConnections = {
            cfg.sequencerConnections
              .withSubmissionRequestAmplification(
                SubmissionRequestAmplification(
                  PositiveInt.tryCreate(5), // amplify immediately
                  config.NonNegativeFiniteDuration.Zero,
                )
              )
              .withSequencerTrustThreshold(
                sequencerTrustThreshold =
                  PositiveInt.two // make sure that we've read from both sequencers so that the aggregation state is in sync
              )
              .value
          }),
      )
      participant1.synchronizers.disconnect_all()
      participant1.synchronizers.reconnect(alias)

      // we don't want to see a warning or an error in the logs
      val alice = participant1.parties.find("alice")
      loggerFactory.assertLogs(SuppressionRule.LevelAndAbove(Level.WARN))({
        IouSyntax.createIou(participant1)(alice, alice)
        Threading.sleep(5000)
      })

      assertResult(2)(numRequests.get())
      assertResult(2)(numResponses.get())

    }

}
