// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - Participant3 (target) is empty, and the target participant for onboarding Alice
  *
  * Test Goal: Verifies that the target participant cannot submit on behalf of an onboarding party.
  *
  * Test Steps:
  *   1. A party marked as onboarding (`requiresPartyToBeOnboarded = true`) is rejected during Phase
  *      1 validation on the target participant.
  *   1. The same party can still successfully submit via a source participant where they are
  *      already fully hosted.
  *   1. Once the onboarding flag is cleared on the target participant, the party's submissions
  *      succeed.
  */
final class RejectOnboardingPartySubmissionIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        target.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          adds = Seq(target.id -> ParticipantPermission.Submission),
          store = daId,
          requiresPartyToBeOnboarded = true,
        )

        alice.topology.party_to_participant_mappings.propose_delta(
          source,
          adds = Seq(target.id -> ParticipantPermission.Submission),
          store = daId,
          requiresPartyToBeOnboarded = true,
        )

      }

  "Reject an onboarding party's submission" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      target.ledger_api.javaapi.commands.submit(
        Seq(alice),
        testIou(alice, bob, 1.00).create().commands().asScala.toSeq,
        daId,
      ),
      _.shouldBeCantonErrorCode(
        TransactionProcessor.SubmissionErrors.PartyCurrentlyOnboarding
          .Rejection(Seq(alice.toLf))
          .code
      ),
    )

    Seq(source, target).foreach(
      _.ledger_api.state.acs.active_contracts_of_party(alice) should have size 0
    )
  }

  "Accept an already hosted party's submission" in { implicit env =>
    import env.*

    source.ledger_api.javaapi.commands.submit(
      Seq(alice),
      testIou(alice, bob, 2.00).create().commands().asScala.toSeq,
      daId,
    )

    Seq(source, target).foreach(
      _.ledger_api.state.acs.active_contracts_of_party(alice) should have size 1
    )
  }

  "Accept a submission once the party has been onboarded" in { implicit env =>
    import env.*

    // Clear the onboarding flag by proposing without `requiresPartyToBeOnboarded`
    target.topology.party_to_participant_mappings.propose_delta(
      party = alice,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
    )

    target.ledger_api.javaapi.commands.submit(
      Seq(alice),
      testIou(alice, bob, 3.00).create().commands().asScala.toSeq,
      daId,
    )

  }

}
