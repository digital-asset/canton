// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.util.chaining.*

final class SubmissionIdIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

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

  "Submission ids that are too long" should {
    "be rejected" in { implicit env =>
      import env.*

      val alice =
        participant1.parties.enable("Alice", synchronizer = Some(daName))
      participant1.parties.testing.also_enable(alice, acmeName)

      def runScenario(targetSynchronizer: PhysicalSynchronizerId, submissionId: String) = {
        val iou = participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            IouSyntax.testIou(alice.partyId, alice.partyId).create().commands().asScala.toSeq,
            synchronizerId = Some(daId),
          )
          .pipe(tx => JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(tx).loneElement)

        val archiveIouCmd = iou.id.exerciseArchive().commands().loneElement

        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            Seq(archiveIouCmd),
            synchronizerId = Some(targetSynchronizer),
            submissionId = submissionId,
          )
      }

      runScenario(acmeId, "short-submission-id")

      val r222 = Random.alphanumeric.take(222).toList.mkString("")
      val r221 = r222.drop(1)

      r222 should have size 222
      r221 should have size 221

      // 221 is below the limit
      runScenario(acmeId, r221)

      // 222 is above the limit when there are automatic reassignments
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        runScenario(acmeId, r222),
        _.shouldBeCantonErrorCode(TransactionRoutingError.MalformedInputErrors.InvalidSubmissionId),
      )

      // 222 is below the limit when there are no automatic reassignments
      runScenario(daId, r222)
    }
  }
}
