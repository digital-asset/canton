// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  UnassignedWrapper,
}
import com.digitalasset.canton.admin.api.client.data.TrafficControlParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters

import java.util.UUID
import scala.jdk.CollectionConverters.*

import util.chaining.*

/** Ensure that a submission done by an external party can trigger automatic reassignments. Check
  * that traffic cost of automatic reassignments can be correlated to the submitted command.
  *
  * Limitation: EPN=CPN (in other words, the submitting participant is a stakeholder of the contract
  * to be reassigned).
  */
final class AutomaticReassignmentExternalPartyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1_S1M1
    .addConfigTransforms(
      ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
    )
    .withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
      participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)

      val generousTrafficControlParameters: TrafficControlParameters =
        TrafficControlParameters(
          maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(1_000_000_000_000L),
          readVsWriteScalingFactor =
            InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
          // Enough to bootstrap the synchronizer and connect the participant after 1 second
          maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(1L),
          setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
          enforceRateLimiting = true,
          baseEventCost = NonNegativeLong.tryCreate(1L),
          freeConfirmationResponses = true,
        )

      // Enable traffic
      synchronizerOwners1.foreach(
        _.topology.synchronizer_parameters.propose_update(
          daId,
          _.copy(trafficControl = Some(generousTrafficControlParameters)),
        )
      )

      synchronizerOwners2.foreach(
        _.topology.synchronizer_parameters.propose_update(
          acmeId,
          _.copy(trafficControl = Some(generousTrafficControlParameters)),
        )
      )
    }

  "Automatic reassignment" should {
    /*
    Scenario:
    - Create contract on da
    - Submit the archival on acme
    - Ensure reassignment is done
     */
    "work with external parties" in { implicit env =>
      import env.*

      val aliceE =
        participant1.parties.testing.external.enable("AliceE", synchronizer = Some(daName))
      participant1.parties.testing.also_enable(aliceE, acmeName)

      val iou = participant1.ledger_api.javaapi.commands
        .submit(
          Seq(aliceE),
          IouSyntax.testIou(aliceE.partyId, aliceE.partyId).create().commands().asScala.toSeq,
          synchronizerId = Some(daId),
        )
        .pipe(tx => JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement)

      val initialLedgerEnd = participant1.ledger_api.state.end()

      val archiveIouCmd = iou.id.exerciseArchive().commands().loneElement

      val submissionId = "iou_archival_submission_id"
      val commandId = UUID.randomUUID().toString

      participant1.ledger_api.javaapi.commands
        .submit(
          Seq(aliceE),
          Seq(archiveIouCmd),
          // prescribe the other synchronizer
          synchronizerId = Some(acmeId),
          submissionId = submissionId,
          commandId = commandId,
        )

      val reassignmentUpdates = participant1.ledger_api.updates
        .reassignments(
          partyIds = Set(aliceE),
          completeAfter = PositiveInt.two,
          beginOffsetExclusive = initialLedgerEnd,
        )

      val archiveUpdate = participant1.ledger_api.updates
        .transactions(
          partyIds = Set(aliceE),
          completeAfter = PositiveInt.one,
          beginOffsetExclusive = initialLedgerEnd,
        )
        .loneElement

      val completions = participant1.ledger_api.completions.list(
        partyId = aliceE,
        atLeastNumCompletions = 3, // unassign, assign, archival
        beginOffsetExclusive = initialLedgerEnd,
      )

      val unassigned =
        reassignmentUpdates.collect { case unassigned: UnassignedWrapper => unassigned }.loneElement

      val assigned = reassignmentUpdates.collect { case assigned: AssignedWrapper =>
        assigned
      }.loneElement

      withClue("check on reassignment events") {
        unassigned.events.loneElement.contractId shouldBe iou.id.contractId
        unassigned.source shouldBe daId.logical.toProtoPrimitive
        unassigned.target shouldBe acmeId.logical.toProtoPrimitive

        assigned.reassignmentId shouldBe unassigned.reassignmentId
        assigned.source shouldBe daId.logical.toProtoPrimitive
        assigned.target shouldBe acmeId.logical.toProtoPrimitive
      }

      withClue("checks around traffic correlation") {
        val updates = reassignmentUpdates :+ archiveUpdate

        // If `id` contains a `-`, forget the last `-` and what comes after
        def truncateSubmissionId(id: String): String = {
          val i = id.lastIndexOf("-")
          if (i < 0) id else id.substring(0, i)
        }

        updates should have size 3
        completions should have size 3

        // It is possible to correlate transaction and automatic reassignments
        forAll(updates)(_.commandId.value shouldBe commandId)
        forAll(completions)(_.commandId shouldBe commandId)
        forAll(completions)(c => truncateSubmissionId(c.submissionId) shouldBe submissionId)

        completions.map(_.submissionId).toSet should have size 3 // every submission id is unique

        // Traffic is paid
        forAll(updates)(_.paidTrafficCost.value should be > 0L)
        forAll(completions)(_.paidTrafficCost should be > 0L)

        // Updates and completions match
        forAll(updates.zip(completions)) { case (update, completion) =>
          update.updateId shouldBe completion.updateId
          update.commandId.value shouldBe completion.commandId
        }
      }
    }
  }
}
