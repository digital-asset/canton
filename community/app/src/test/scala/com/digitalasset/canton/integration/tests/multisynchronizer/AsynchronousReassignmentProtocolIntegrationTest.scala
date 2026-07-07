// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multisynchronizer.AsynchronousReassignmentProtocolIntegrationTest.DelayFirstConfirmationResponsePolicy
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicyWithoutTraceContext,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.{BaseTest, SynchronizerAlias, config}

import scala.collection.mutable
import scala.concurrent.Promise

/** This test checks that the reassignment protocol processing runs asynchronously: if two
  * (un)assignments are sent consecutively and if the confirmation response of the first one is
  * blocked, then the processing of the second one can be done.
  *
  * For both the unassignment and assignment:
  *   - we create two different Iou contracts
  *   - we submit asynchronously the unassignment/assignments of the two contracts
  *   - we hold back the first confirmation response from Alice to delay the first
  *     unassignment/assignment
  *   - we check on the reassignment store that the second unassignment/assignment is processed
  *     before the first one
  *   - we allow the first confirmation request to go through
  */
final class AsynchronousReassignmentProtocolIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = acmeId)

        // Disable automatic assignment so that we really control it
        def disableAutomaticAssignment(
            sequencer: LocalSequencerReference
        ): Unit =
          sequencer.topology.synchronizer_parameters
            .propose_update(
              sequencer.synchronizer_id,
              _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
            )

        disableAutomaticAssignment(sequencer1)
        disableAutomaticAssignment(sequencer2)

        val alice = "alice"
        val bob = "bob"
        // Enable alice on other participants, on all synchronizers
        PartiesAllocator(participants.all.toSet)(
          Seq(alice -> participant1, bob -> participant1),
          Map(
            alice -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            ),
            bob -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            ),
          ),
        )

        programmableSequencers.put(daName, getProgrammableSequencer(sequencer1.name))
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))
      }

  "unassignment protocol processing is asynchronous" in { implicit env =>
    import env.*
    val aliceId = "alice".toPartyId(participant1)
    val bobId = "bob".toPartyId(participant1)

    val iou1 = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId, 1.0)
    val iou2 = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId, 1.0)

    val policy = new DelayFirstConfirmationResponsePolicy()
    programmableSequencers(daName).setPolicy_("delay first confirmation response")(policy.policy)

    val ledgerEnd = participant1.ledger_api.state.end()

    participant1.ledger_api.commands
      .submit_unassign_async(
        aliceId,
        Seq(LfContractId.assertFromString(iou1.id.contractId)),
        daId,
        acmeId,
      )

    policy.firstConfirmationRequestSent.future.futureValue

    participant1.ledger_api.commands
      .submit_unassign_async(
        bobId,
        Seq(LfContractId.assertFromString(iou2.id.contractId)),
        daId,
        acmeId,
      )

    val reassignmentStore = participant1.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore

    eventually() {
      val reassignmentIds = reassignmentStore.listInFlightReassignmentIds().futureValueUS

      val entries =
        reassignmentIds.map(id => reassignmentStore.findReassignmentEntry(id).futureValueUS.value)
      // make sure that Bob's unassignment completed before Alice's one
      entries.loneElement.unassignmentData.map(_.submitterMetadata.submitter) should contain(
        bobId.toLf
      )
    }
    policy.deliverConfirmationRequestSent.trySuccess(())

    // the entries are still published following sequencer order
    val bobOffset = participant1.ledger_api.completions.list(bobId, 1, ledgerEnd).map(_.offset)

    val aliceOffset = participant1.ledger_api.completions.list(aliceId, 1, ledgerEnd).map(_.offset)

    aliceOffset.head should be < bobOffset.head
  }

  "assignment protocol processing is asynchronous" in { implicit env =>
    import env.*

    val aliceId = "alice".toPartyId(participant1)
    val bobId = "bob".toPartyId(participant1)

    val iou1 = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)
    val iou2 = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

    val unassign1 = participant1.ledger_api.commands
      .submit_unassign(
        aliceId,
        Seq(LfContractId.assertFromString(iou1.id.contractId)),
        daId,
        acmeId,
      )
      .reassignmentId

    val unassign2 = participant1.ledger_api.commands
      .submit_unassign(bobId, Seq(LfContractId.assertFromString(iou2.id.contractId)), daId, acmeId)
      .reassignmentId

    val policy = new DelayFirstConfirmationResponsePolicy()
    programmableSequencers(acmeName).setPolicy_("delay first confirmation response")(policy.policy)

    participant1.ledger_api.commands
      .submit_assign_async(
        aliceId,
        unassign1,
        daId,
        acmeId,
      )

    policy.firstConfirmationRequestSent.future.futureValue

    participant1.ledger_api.commands
      .submit_assign_async(
        bobId,
        unassign2,
        daId,
        acmeId,
      )

    val aliceReassignmentId = ReassignmentId.tryCreate(unassign1)
    val bobReassignmentId = ReassignmentId.tryCreate(unassign2)

    val reassignmentStore = participant1.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore

    eventually() {
      val aliceEntry =
        reassignmentStore.findReassignmentEntry(aliceReassignmentId).futureValueUS.value
      val bobEntry = reassignmentStore.findReassignmentEntry(bobReassignmentId).futureValueUS.value

      // make sure that Bob's unassignment completed before Alice's one
      bobEntry.assignmentTs should not be empty
      aliceEntry.assignmentTs shouldBe empty
    }

    policy.deliverConfirmationRequestSent.trySuccess(())
    eventually() {
      val aliceEntry =
        reassignmentStore.findReassignmentEntry(aliceReassignmentId).futureValueUS.value
      aliceEntry.assignmentTs should not be empty
    }

  }
}

object AsynchronousReassignmentProtocolIntegrationTest {
  class DelayFirstConfirmationResponsePolicy() {
    /*
     Completes when the first confirmation request is sent by P1.
     Allows to not submit the second (un)assignment too early
     */
    val firstConfirmationRequestSent: Promise[Unit] = Promise[Unit]()
    // is completed when the first confirmation request should be delivered
    val deliverConfirmationRequestSent: Promise[Unit] = Promise[Unit]()

    val policy: SendPolicyWithoutTraceContext = { submissionRequest =>
      if (isConfirmationResponse(submissionRequest)) {
        if (!firstConfirmationRequestSent.isCompleted) {
          firstConfirmationRequestSent.trySuccess(())
          SendDecision.HoldBack(deliverConfirmationRequestSent.future)
        } else SendDecision.Process

      } else SendDecision.Process
    }
  }
}
