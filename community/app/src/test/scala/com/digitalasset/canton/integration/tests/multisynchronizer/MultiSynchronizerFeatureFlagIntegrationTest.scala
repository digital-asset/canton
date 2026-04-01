// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.MultiSynchronizerIsNotEnabled
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission

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
      }

  "Unassignment: MultiSynchronizer topology feature flag should be enabled on the source synchronizer" in {
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

  "Unassignment: MultiSynchronizer topology feature flag should be enabled on the target synchronizer for" in {
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

  "Assignment: MultiSynchronizer topology feature flag should be enabled on the target synchronizer." in {
    implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1, Some(daId))(aliceId, bobId)

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
}
