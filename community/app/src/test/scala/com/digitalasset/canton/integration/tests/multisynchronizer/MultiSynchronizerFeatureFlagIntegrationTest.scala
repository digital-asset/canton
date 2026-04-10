// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  MultiSynchronizerFeatureFlag,
  PartiesAllocator,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.MultiSynchronizerIsNotEnabled
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

/** We test the Unsafe-Multi-synchronizer topology feature flag.
  *
  * The topology used:
  *   - P1, P2, P3 are connected to S1
  *   - P1, P2, P4 are connected to S2
  *   - Alice is hosted on P1, P3
  *   - Bob is hosted on P2, P4
  *
  * We can test that during the submission we check that all participants hosting a stakeholder have
  * the required flag.
  *
  * We also check that the request is ignored on participants that are not enabling the flag, if a
  * malicious participant has sent the request bypassing the submission validation.
  */
final class MultiSynchronizerFeatureFlagIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {
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

  private var aliceId: PartyId = _
  private var bobId: PartyId = _
  private var maliciousP1OnDa: MaliciousParticipantNode = _
  private var maliciousP1OnAcme: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        val participantsConnectedToDa = Seq(participant1, participant2, participant3)
        val participantsConnectedToAcme = Seq(participant1, participant2, participant4)

        participantsConnectedToDa.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
        participantsConnectedToAcme.foreach(
          _.synchronizers.connect_local(sequencer2, alias = acmeName)
        )
        participantsConnectedToDa.foreach(
          _.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)
        )
        participantsConnectedToAcme.foreach(
          _.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = acmeId)
        )
        val alice = "alice"
        val bob = "bob"

        PartiesAllocator(participants.all.toSet)(
          Seq(alice -> participant1, bob -> participant2),
          Map(
            alice -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant3, Submission),
              )),
              acmeId -> (PositiveInt.one, Set(
                (participant1, Submission)
              )),
            ),
            bob -> Map(
              daId -> (PositiveInt.one, Set((participant2, Submission))),
              acmeId -> (PositiveInt.one, Set(
                (participant2, Submission),
                (participant4, Submission),
              )),
            ),
          ),
        )

        aliceId = alice.toPartyId(participant1)
        bobId = bob.toPartyId(participant2)

        maliciousP1OnDa = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )

        maliciousP1OnAcme = MaliciousParticipantNode(
          participant1,
          acmeId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  "Unassignment: MultiSynchronizer topology feature flag should be enabled on the source synchronizer" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.commands
          .submit_unassign(
            submitter = aliceId,
            contractIds = Seq(iou.id.toLf),
            source = daId,
            target = acmeId,
          ),
        _.errorMessage should include(
          MultiSynchronizerIsNotEnabled(
            Set(participant1.id, participant3.id, participant2.id),
            daId,
          ).message
        ),
      )
      IouSyntax.archive(participant1, Some(daId))(iou, aliceId)
  }

  "Unassignment: MultiSynchronizer topology feature flag should be enabled on the target synchronizer" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant3),
        daId.logical,
      )
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.commands
          .submit_unassign(
            submitter = aliceId,
            contractIds = Seq(iou.id.toLf),
            source = daId,
            target = acmeId,
          ),
        _.errorMessage should include(
          MultiSynchronizerIsNotEnabled(
            Set(participant1.id, participant2.id, participant4.id),
            acmeId,
          ).message
        ),
      )

      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant4),
        acmeId.logical,
      )

      participant1.ledger_api.commands
        .submit_reassign(
          submitter = aliceId,
          contractIds = Seq(iou.id.toLf),
          source = daId,
          target = acmeId,
        )
      val contractReassigned = participant1.ledger_api.javaapi.state.acs
        .await(Iou.COMPANION)(aliceId, synchronizerFilter = Some(acmeId))

      contractReassigned.id shouldBe iou.id

      IouSyntax.archive(participant1, Some(acmeId))(iou, aliceId)
  }

  "Unassignment: should not process unassignment requests if the multi-synchronizer feature flag is not enabled" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

      val iouId = iou.id.contractId
      val iouContratInstance = participant1.testing
        .acs_search(daName, exactId = iouId, limit = PositiveInt.one)
        .loneElement

      val helpers = ReassignmentDataHelpers(
        contract = iouContratInstance,
        sourceSynchronizer = Source(daId),
        targetSynchronizer = Target(acmeId),
        pureCrypto = participant1.crypto.pureCrypto,
        targetTimestamp = Target(participant1.testing.fetch_synchronizer_time(acmeId)),
      )
      val unassignmentTree = helpers
        .fullUnassignmentTree(
          aliceId.toLf,
          participant1,
          MediatorGroupRecipient(NonNegativeInt.zero),
        )(reassigningParticipants = Set(participant1.id, participant2.id))

      MultiSynchronizerFeatureFlag.enable(Seq(participant1), daId.logical)
      MultiSynchronizerFeatureFlag.disable(Seq(participant2, participant3), daId.logical)

      val offsetBeforeSubmit = participant1.ledger_api.state.end()

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
        within = maliciousP1OnDa.submitUnassignmentRequest(unassignmentTree).value,
        logs =>
          forExactly(2, logs)(
            _.message should include(
              "Received Unassignment but multi-synchronizer feature flag is not set. Ignoring the request."
            )
          ),
      )

      // Make sure everything is still healthy
      participant1.health.ping(participant2.id, synchronizerId = Some(daId))
      participant1.health.ping(participant3.id, synchronizerId = Some(daId))

      // contract has not left the source synchronizer
      val contractId = participant1.ledger_api.javaapi.state.acs
        .await(Iou.COMPANION)(aliceId, synchronizerFilter = Some(daId))

      // make sure that the confirming participant still rejects the unassignment
      participant1.ledger_api.completions
        .list(
          aliceId,
          userId = "tests",
          atLeastNumCompletions = 1,
          beginOffsetExclusive = offsetBeforeSubmit,
        )
        .loneElement
        .status
        .value
        .message should include(
        MultiSynchronizerIsNotEnabled(Set(participant3, participant2), daId).message
      )

      contractId.id shouldBe iou.id
      IouSyntax.archive(participant1, Some(daId))(iou, aliceId)
  }

  "Assignment: MultiSynchronizer topology feature flag should be enabled on the target synchronizer." onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)
      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant3),
        daId.logical,
      )
      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant4),
        acmeId.logical,
      )

      val reassignmentId = participant1.ledger_api.commands
        .submit_unassign(
          submitter = aliceId,
          contractIds = Seq(iou.id.toLf),
          source = daId,
          target = acmeId,
        )
        .reassignmentId

      MultiSynchronizerFeatureFlag.disable(
        Seq(participant1, participant2, participant4),
        acmeId.logical,
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.commands
          .submit_assign(
            submitter = aliceId,
            reassignmentId = reassignmentId,
            source = daId,
            target = acmeId,
          ),
        _.errorMessage should include(
          MultiSynchronizerIsNotEnabled(
            Set(participant1.id, participant2.id, participant4.id),
            acmeId,
          ).message
        ),
      )

      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant4),
        acmeId.logical,
      )
      participant1.ledger_api.commands
        .submit_assign(
          submitter = aliceId,
          reassignmentId = reassignmentId,
          source = daId,
          target = acmeId,
        )

      val contractReassigned = participant1.ledger_api.javaapi.state.acs
        .await(Iou.COMPANION)(aliceId, synchronizerFilter = Some(acmeId))

      contractReassigned.id shouldBe iou.id
      IouSyntax.archive(participant1, Some(acmeId))(iou, aliceId)
  }

  "Assignment: should not process assignment requests if the multi-synchronizer feature flag is not enabled" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
    implicit env =>
      import env.*

      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant3),
        daId.logical,
      )
      MultiSynchronizerFeatureFlag.enable(
        Seq(participant1, participant2, participant4),
        acmeId.logical,
      )
      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

      val reassignmentId = participant1.ledger_api.commands
        .submit_unassign(
          submitter = aliceId,
          contractIds = Seq(iou.id.toLf),
          source = daId,
          target = acmeId,
        )
        .reassignmentId

      // so the assignment should fail
      MultiSynchronizerFeatureFlag.enable(Seq(participant1), acmeId.logical)
      MultiSynchronizerFeatureFlag.disable(
        Seq(participant2, participant4),
        acmeId.logical,
      )

      val unassignmentData = participant1.underlying.value.sync.syncPersistentStateManager
        .get(acmeId)
        .value
        .reassignmentStore
        .lookup(ReassignmentId.tryCreate(reassignmentId))
        .failOnShutdown
        .futureValue

      val offsetBeforeSubmit = participant1.ledger_api.state.end()
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
        within = maliciousP1OnAcme
          .submitAssignmentRequest(aliceId.toLf, unassignmentData)
          .value,
        logs =>
          forExactly(2, logs)(
            _.message should include(
              "Received Assignment but multi-synchronizer feature flag is not set. Ignoring the request."
            )
          ),
      )

      // make sure that the confirming participant still rejects the unassignment
      participant1.ledger_api.completions
        .list(
          aliceId,
          userId = "CantonConsole",
          atLeastNumCompletions = 1,
          beginOffsetExclusive = offsetBeforeSubmit,
        )
        .loneElement
        .status
        .value
        .message should include(
        MultiSynchronizerIsNotEnabled(Set(participant2, participant4), acmeId).message
      )

      // Make sure everything is still healthy
      participant1.health.ping(participant2.id, synchronizerId = Some(acmeId))
      participant1.health.ping(participant4.id, synchronizerId = Some(acmeId))
  }
}
