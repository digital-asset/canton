// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.SubmissionRequestAmplification
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.MaxSequencingTimeTooFar
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

trait DeliverErrorIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      // Use a sim clock so that we don't have to worry about reaction timeouts
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.disableSubmissionChecksForTesting).replace(true)
        )
      )

  override val defaultParticipant: String = "participant1"

  "participants can handle a DeliverError" in { implicit env =>
    import env.*

    // If one of the recipients of a batch is not known to the sequencer,
    // the sequencer will return a deliver error

    List(participant1, participant2).foreach { p =>
      p.synchronizers.connect_local(sequencer1, daName)
      p.synchronizers.modify(
        daName,
        _.withSubmissionRequestAmplification(
          SubmissionRequestAmplification(
            factor = PositiveInt.two,
            patience = com.digitalasset.canton.config.NonNegativeFiniteDuration.ofSeconds(1),
          )
        ),
      )
      p.synchronizers.disconnect_all()
      p.synchronizers.reconnect_all()
      p.dars.upload(CantonExamplesPath)
    }

    val participant1Id = participant1.id
    val sequencer = getProgrammableSequencer(sequencer1.name)

    val alice =
      participant1.parties.testing.enable("Alice", synchronizeParticipants = Seq(participant2))
    val bob =
      participant2.parties.testing.enable("Bob", synchronizeParticipants = Seq(participant1))

    val syncCrypto = participant1.underlying.value.sync.syncCrypto

    sequencer.setPolicy_("add an invalid ") { submissionRequest =>
      submissionRequest.sender match {
        case `participant1Id` if submissionRequest.isConfirmationRequest =>
          val modifiedSubmission = submissionRequest
            .focus(_.maxSequencingTime)
            .replace(
              // default is max 6 minutes, so setting to 10 minutes will mean that the sequencer will reject it
              // however, as we disabled the "submission side" checks, this will bounce on the post ordering side
              // with a reject
              env.environment.clock.now.plusSeconds(600)
            )
          // We now must recreate a correct signature of the sender
          val signedModifiedRequest =
            signModifiedSubmissionRequest(
              modifiedSubmission,
              syncCrypto.tryForSynchronizer(daId, staticSynchronizerParameters1),
              Some(environment.now),
            )
          SendDecision.Replace(signedModifiedRequest)

        case _ => SendDecision.Process
      }
    }

    val iouCommand = new Iou(
      alice.toProtoPrimitive,
      bob.toProtoPrimitive,
      new Amount(1.toBigDecimal, "snack"),
      List.empty.asJava,
    ).create.commands.asScala.toSeq
    val expectedErrorCode = MaxSequencingTimeTooFar.id

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.ledger_api.javaapi.commands.submit(Seq(alice), iouCommand),
      _.warningMessage should include("Submission was rejected by the sequencer at"),
      _.errorMessage should (include("Request failed for participant1") and include(
        expectedErrorCode
      )),
    )

    logger.info("received a deliver error for the submission")

    // Make sure that both participants are still alive
    sequencer.resetPolicy()
    participant1.health.ping(participant2)
  }
}

class DeliverErrorBftOrderingIntegrationTestPostgres extends DeliverErrorIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
