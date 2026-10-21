// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.awaitHostingStates
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission

/** Test scenario: participant2 is added as an Onboarding host of Alice and then removed without
  * ever becoming Active. The removal emits a Revoked Ledger API topology event for a participant
  * that never received an Added event, so no ACS digest ever counted Alice as hosted on
  * participant2.
  *
  * Test goal: show that the ACS digest consistency check still passes on both participants
  * afterwards, i.e. that the digest processors do not subtract what was never added.
  *
  * Test setup:
  *   - 2 participants, 1 synchronizer.
  *   - Alice is hosted on participant1, Bob on participant2.
  *   - An IOU between Alice and Bob, so participant1 and participant2 share a contract of Alice.
  */
final class RevokedWithoutAddedAcsCommitmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var alice: Party = _
  private var bob: Party = _

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)

        alice =
          participant1.parties.testing.enable("Alice", synchronizeParticipants = Seq(participant2))
        bob =
          participant2.parties.testing.enable("Bob", synchronizeParticipants = Seq(participant1))
      }

  /** Runs the ACS digest consistency check on both participants at a checkpoint at or after
    * `since`, and asserts that it finds no inconsistency. The check reports inconsistencies only as
    * warnings, so the assertion is on the logs.
    */
  private def assertAcsDigestsConsistent(
      since: CantonTimestamp
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    Seq(participant1, participant2).foreach { participant =>
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        eventually() {
          // Produces events after `since`, so that the digests are checkpointed past it.
          participant.health.ping(participant)
          participant.commitments.run_digest_consistency_check(daId)
          val status = eventually() {
            val status = participant.commitments.digest_consistency_check_status(daId)
            status.isRunning shouldBe false
            status
          }
          withClue(s"Consistency check of ${participant.name} ran before $since: ") {
            status.startTimestamp.value should be >= since
          }
        },
        entries => entries shouldBe empty,
      )
    }
  }

  "The ACS digest consistency check" should {
    "pass after participant2 is removed while Onboarding for Alice" in { implicit env =>
      import env.*

      val iou = IouSyntax.createIou(participant1)(alice, bob)
      eventually() {
        participant2.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      }

      // Adding a participant needs both the participant's and the party's signature. The party
      // signs through participant1 if it is local, or with its own key if it is external.
      participant2.topology.party_to_participant_mappings.propose_delta(
        party = alice,
        adds = Seq(participant2.id -> ParticipantPermission.Confirmation),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )
      alice.topology.party_to_participant_mappings.propose_delta(
        node = participant1,
        adds = Seq(participant2.id -> ParticipantPermission.Confirmation),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )
      awaitHostingStates(
        alice,
        daId,
        Seq(participant1, participant2),
        Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Onboarding),
      )

      alice.topology.party_to_participant_mappings.propose_delta(
        node = participant1,
        removes = Seq(participant2.id),
        store = daId,
      )
      val removedAt = awaitHostingStates(
        alice,
        daId,
        Seq(participant1, participant2),
        Map(participant1.id -> HostingState.Active),
      )

      assertAcsDigestsConsistent(since = removedAt)
    }
  }
}
