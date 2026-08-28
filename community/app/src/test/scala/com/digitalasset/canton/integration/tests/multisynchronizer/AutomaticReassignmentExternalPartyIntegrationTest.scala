// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.{
  AssignedWrapper,
  UnassignedWrapper,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*

import util.chaining.*

/** Ensure that a submission done by an external party can trigger automatic reassignments.
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

      val initialLedgerEnd = participant1.ledger_api.state.end()

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

      val archiveIouCmd = iou.id.exerciseArchive().commands().loneElement

      participant1.ledger_api.javaapi.commands
        .submit(
          Seq(aliceE),
          Seq(archiveIouCmd),
          // prescribe the other synchronizer
          synchronizerId = Some(acmeId),
        )

      val updates = participant1.ledger_api.updates
        .reassignments(
          partyIds = Set(aliceE),
          completeAfter = PositiveInt.two,
          beginOffsetExclusive = initialLedgerEnd,
        )

      val unassigned =
        updates.collect { case u: UnassignedWrapper => u }.loneElement.events.loneElement
      val assigned = updates.collect { case u: AssignedWrapper => u }.loneElement.events.loneElement

      unassigned.contractId shouldBe iou.id.contractId
      unassigned.source shouldBe daId.logical.toProtoPrimitive
      unassigned.target shouldBe acmeId.logical.toProtoPrimitive

      assigned.reassignmentId shouldBe unassigned.reassignmentId
      assigned.source shouldBe daId.logical.toProtoPrimitive
      assigned.target shouldBe acmeId.logical.toProtoPrimitive
    }
  }
}
