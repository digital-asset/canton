// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.SynchronizerRouterIntegrationTestSetup.{
  createAggregate,
  createSingle,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

final class AutomaticReassignmentDuplicateSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1_S1M1
    .addConfigTransforms(
      ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
      ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
    )
    .withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
      participant1.dars.upload(CantonTestsPath, synchronizerId = acmeId)
    }

  "a duplicate submission" should {
    "be rejected before its contracts are reassigned" in { implicit env =>
      import env.*

      val party1 = participant1.parties.enable("party1", synchronizer = daName)
      participant1.parties.enable(party1.identifier.unwrap, synchronizer = acmeName)

      val single = createSingle(party1, Some(synchronizer1Id), participant1, party1).id
      val aggregate = createAggregate(participant1, party1, Seq(single), Some(synchronizer2Id))

      // The only confirmation requests on synchronizer 1 from here on are unassignments
      val unassignmentRequests = new AtomicInteger()
      val unassignmentReceived = Promise[Unit]()
      val unassignmentReleased = Promise[Unit]()
      getProgrammableSequencer(sequencer1.name).setPolicy_("hold back the unassignment requests") {
        request =>
          if (request.isConfirmationRequest) {
            unassignmentRequests.incrementAndGet()
            unassignmentReceived.trySuccess(())
            SendDecision.HoldBack(unassignmentReleased.future)
          } else SendDecision.Process
      }

      val ledgerEndBeforeExercise = participant1.ledger_api.state.end()

      def submit(): Unit = participant1.ledger_api.javaapi.commands.submit_async(
        actAs = Seq(party1),
        commands = aggregate.id.exerciseCountAll().commands.asScala.toSeq,
        synchronizerId = Some(synchronizer2Id),
        commandId = "duplicate-submission",
      )

      submit()
      unassignmentReceived.future.futureValue

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submit(),
        _.errorMessage should include(ConsistencyErrors.SubmissionAlreadyInFlight.id),
      )

      unassignmentReleased.success(())

      participant1.ledger_api.completions.list(
        partyId = party1,
        atLeastNumCompletions = 3, // unassign, assign, archival
        beginOffsetExclusive = ledgerEndBeforeExercise,
      ) should have size 3
      unassignmentRequests.get() shouldBe 1
    }
  }
}
