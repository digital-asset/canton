// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.logging.SuppressionRule.NoSuppression
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import org.slf4j.event.Level

/** General idea is to simulate an operator who is not following the offline party replication steps
  * properly.
  *
  * Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 1 active IOU contract between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Edge cases and failure modes where the operator violates preconditions of the party ACS
  * import process (e.g., missing topology transactions, target left online).
  */
final class ImportPartyAcsEdgeCasesIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
      }

  "ACS import aborts if no onboarding topology transaction is found" in { implicit env =>
    import env.*

    val ledgerEndP1 = source.ledger_api.state.end()

    // Use the repair ACS export to bypass the topology check buit into the party ACS export endpoint
    source.repair.export_acs(
      parties = Set(alice),
      ledgerOffset = ledgerEndP1,
      exportFilePath = acsSnapshotPath,
    )

    // Ensure the party ACS import aborts if the topology store lacks the PTP onboarding mapping.
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
      _.errorMessage should include(
        s"Refuse to import ACS for party ${alice.partyId}: No topology transaction found onboarding this party on participant ${target.id}."
      ),
    )
  }

  "ACS import aborts if the target participant is not disconnected from all synchronizers" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()

      // Target authorizes Alice to be hosted on it, but then "forgets" to disconnect thereafter,
      target.topology.party_to_participant_mappings
        .propose_delta(
          party = alice.partyId,
          adds = Seq(target.id -> ParticipantPermission.Submission),
          store = daId,
          requiresPartyToBeOnboarded = true,
        )

      // Export an ACS snapshot using repair service to bypass party ACS export-side topology checks
      source.repair.export_acs(
        parties = Set(alice),
        ledgerOffset = ledgerEndP1,
        exportFilePath = acsSnapshotPath,
      )

      // Ensure import aborts if the target is still connected to a synchronizer
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
        _.errorMessage should include(
          "There are still synchronizers connected. Please disconnect all synchronizers."
        ),
      )
  }

  /** Simulates an operator failing to follow the strict offline replication guide resulting in an
    * effective party-to-participant mapping topology transaction known to the target participant.
    *
    * The expectation is that the target participant only receives that transaction after having
    * imported the party's ACS and (re)connected to the synchronizer.
    *
    * Asserts:
    *   1. The ACS import still succeeds.
    *   1. The system correctly identifies that the transaction is already effective and logs the
    *      expected warning.
    *   1. The pending onboarding clearance operation is still successfully recorded and can be
    *      cleared.
    */
  "ACS import succeeds and logs a warning if the onboarding topology transaction is already effective" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      val beforeActivationSourceOffset = source.ledger_api.state.end()
      val beforeActivationTargetOffset = target.ledger_api.state.end()

      alice.topology.party_to_participant_mappings.propose_delta(
        source,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )

      // Wait for the topology transaction to become effective on the target participant
      // because it was left online.
      eventually() {
        target.topology.party_to_participant_mappings.list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = target.id.filterString,
        ) should not be empty
      }

      source.parties.export_party_acs(
        alice,
        daId,
        target.id,
        exportFilePath = acsSnapshotPath,
        beginOffsetExclusive = beforeActivationSourceOffset,
      )

      target.synchronizers.disconnect_all()

      runIfPv34(
        loggerFactory.assertEventuallyLogsSeq(NoSuppression)(
          target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
          logs => logs shouldBe empty,
        ),
        otherwise = {
          loggerFactory.assertLogs(
            target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
            _.warningMessage should include(
              s"Found an already effective party-to-participant mapping with the onboarding flag set for ${alice.partyId} on synchronizer ${daId.logical}"
            ),
          )
        },
      )

      reconnectAndEnsureOnboardingClearance(
        clock,
        alice,
        daName,
        providedTargetLedgerEnd = Some(beforeActivationTargetOffset),
      )
      assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 1)
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 1 active IOU contract between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Manual recovery if the node crashes during the ACS import process. Specifically, this
  * tests the scenario where the ACS import completes successfully, but the node crashes before the
  * pending onboarding clearance operation is written to the database.
  */
final class OfflinePartyReplicationCrashRecoveryIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
        IouSyntax.createIou(source)(alice, bob, 2.95).discard
        IouSyntax.createIou(source)(alice, bob, 3.95).discard
      }

  "Manual recovery succeeds if node crashes after ACS import but before pending operation insertion" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      val beforeActivationOffset = targetAuthorizesHosting(alice, daId, disconnectTarget = true)

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      // Simulate a crash. using repair ACS import endpoint which imports the contracts
      // but bypasses the insertion of the pending OnboardingClearanceOperation.
      target.repair.import_acs(daId, acsSnapshotPath)

      // Test: "Retry" the party ACS import endpoint post-crash:
      // It should idempotently import contracts again and persist the missing pending operation.
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

      reconnectAndEnsureOnboardingClearance(clock, alice, daName)
      assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 3)
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 2 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Asserts that the party ACS import endpoint is strictly idempotent. It verifies that
  * invoking the import command multiple times with the same ACS snapshot safely bypasses duplicate
  * contract insertions and duplicate pending operation insertions without corrupting node state or
  * throwing errors.
  */
final class OfflinePartyReplicationIdempotencyIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
        IouSyntax.createIou(source)(alice, bob, 2.95).discard
      }

  "ACS import is idempotent and can be invoked multiple times without errors" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

    val beforeActivationOffset = targetAuthorizesHosting(alice, daId, disconnectTarget = true)

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target.id,
      beginOffsetExclusive = beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )

    // Import the same ACS snapshot multiple times (assert idempotency)
    target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

    // Let's do a clean node shutdown and start – which should not affect the party ACS import
    target.stop()
    target.start()

    // Suppress a warning from ResilientLedgerSubscription caused by stopping/starting the node.
    // This warning should not happen; ignored here as it is irrelevant to the party ACS import testing.
    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.Level(Level.WARN) && SuppressionRule.LoggerNameContains(
        "ResilientLedgerSubscription"
      )
    )(
      within = {
        target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)
      },
      logs =>
        forAtLeast(1, logs)(m =>
          m.message should include("Ledger subscription PingService failed with an error")
        ),
    )

    target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

    reconnectAndEnsureOnboardingClearance(clock, alice, daName)
    assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 2)
  }
}
