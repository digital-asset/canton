// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.AcsImportMissingOnboardingMapping
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.PendingOperationStore
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.{ReassignmentCounter, RepairCounter}
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

    // Use the repair ACS export to bypass the topology check built into the party ACS export endpoint
    source.repair.export_acs(
      parties = Set(alice),
      ledgerOffset = ledgerEndP1,
      exportFilePath = acsSnapshotPath,
    )

    // Ensure the party ACS import aborts if the topology store lacks the PTP onboarding mapping
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
      _.shouldBeCantonErrorCode(AcsImportMissingOnboardingMapping.Error(alice).code),
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
    * The expectation is that the target participant only receives the effective transaction after
    * having imported the party's ACS and (re)connected to the synchronizer.
    *
    * Asserts:
    *   1. The ACS import still succeeds
    *   1. The pending onboarding clearance operation is still successfully recorded and can be
    *      cleared.
    */
  "ACS import succeeds even if the onboarding topology transaction is already effective" in {
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

      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

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
  *   - 3 active IOU contract between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Manual recovery if the target node crashes during the ACS import process. Specifically,
  * this tests the scenario where the ACS import completes successfully, but the node crashes before
  * the pending onboarding clearance operation is written to the database.
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

      // Simulate a crash using the repair ACS import endpoint which imports the contracts
      // but bypasses the insertion of the pending onboarding clearance operation.
      target.repair.import_acs(daId, acsSnapshotPath)

      // Assert that indeed no pending onboarding clearance operation record exists
      PendingOperationStore(
        target.underlying.value.storage,
        timeouts,
        loggerFactory,
        OnboardingClearanceOperation,
        SynchronizerId.fromString,
      )(executionContext)
        .getAll(
          operationName = OnboardingClearanceOperation.operationName,
          synchronizerId = Some(daId),
        )
        .futureValueUS shouldBe empty

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
  *   - 1 active IOU contract between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Scenario: An operator starts an offline party replication ACS import on the target node.
  * Importing contracts operates in two decoupled phases:
  *   1. Synchronous persistence: Raw payloads and metadata are committed directly to Canton's
  *      internal `ActiveContractStore` and `ContractStore`.
  *   1. Asynchronous indexing: An event is offered to an in-memory queue where a background indexer
  *      thread commits it to the Ledger API event tables and advances the ledger end.
  *
  * If the target crashes immediately after Phase 1 completes but before Phase 2 finishes, the
  * database enters a partial split-brain state: the internal storage contains the uncommitted
  * contracts, but the Ledger API is entirely unaware of them.
  *
  * Test Goal: Verify that recovery logic cleanly handles this mid-crash state. Upon executing a
  * retry of `import_party_acs`, the participant must detect the uncommitted internal records,
  * trigger
  * [[com.digitalasset.canton.participant.sync.SyncEphemeralStateFactory.cleanupPersistentState]] to
  * scrub the dirty state, cleanly re-stream the batch, and successfully commit the contracts to
  * both the internal storage and the downstream Ledger API.
  *
  * Notes:
  *   1. Get the latest stable synchronizer boundary (`lapi_ledger_end_synchronizer_index`) known to
  *      the Ledger API:
  *      [[com.digitalasset.canton.participant.sync.SyncEphemeralStateFactoryImpl.createFromPersistent]]
  * -> [[com.digitalasset.canton.participant.ledger.api.LedgerApiStore.cleanSynchronizerIndex]]
  * Because the indexer never committed in this test, this returns `None`.
  *   1. Determine the baseline repair counter for the starting points and the cleanup watermark:
  *      Passing `None` into the counter calculator
  *      [[com.digitalasset.canton.participant.sync.SyncEphemeralStateFactory.nextRepairCounter]]
  *      defaults `RepairCounter.Genesis` (0)
  *   1. Construct the composite rollback watermark (`nextTimeOfChange`) during recovery
  *      initialization in
  *      [[com.digitalasset.canton.participant.sync.SyncEphemeralStateFactory.cleanupPersistentState]].
  *      In this test:
  *      {{{nextTimeOfChange := TimeOfChange(CantonTimestamp.MinValue, RepairCounter.Genesis)}}}
  *   1. Sweep dirty records in
  *      [[com.digitalasset.canton.participant.sync.SyncEphemeralStateFactory.cleanupPersistentState]]
  *      with "deleteSince": In this test:
  *      {{{Drops records in par_active_contracts WHERE ts >= MinValue AND repair_counter >= 0}}}
  *      Because the simulated dirty mid-crash records sit at contemporary timestamps with a
  *      `repair_counter` of `1`, they satisfy this deletion condition.
  */
final class OfflinePartyReplicationAcsImportMidCrashIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
      }

  "ACS import recovers successfully if participant crashes mid-import leaving unindexed internal records" in {
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

      val (cid, contractInstance) =
        clue("Resolve the contract payload directly from the source's ContractStore") {
          val contractIdStr = source.ledger_api.state.acs.of_party(alice).head.contractId
          val cid = LfContractId.assertFromString(contractIdStr)

          val cInstance =
            source.underlying.value.sync.participantNodePersistentState.value.contractStore
              .lookup(cid)
              .value
              .futureValueUS
              .value

          (cid, cInstance)
        }

      // Simulate the ACS import mid-crash state: Directly inject the contract records into
      // internal stores, bypassing offering updates to the LedgerApiIndexer queue.
      clue("Inject dirty records directly into ContractStore and ActiveContractStore") {
        val targetStores = target.underlying.value.sync.participantNodePersistentState.value
        val targetAcsStore = target.underlying.value.sync.stateInspection
          .getAcsInspection(daId)
          .value
          .activeContractStore

        targetStores.contractStore.storeContracts(Seq(contractInstance)).futureValueUS

        // Write contract activeness record at Genesis repair counter without moving the ledger end!
        // Using `CantonTimestamp.now()` ensures the record sits ahead of any initialization watermarks.
        val dirtyRepairCounter = RepairCounter.Genesis.increment.value
        val dirtyToc = TimeOfChange(CantonTimestamp.now(), Some(dirtyRepairCounter))

        targetAcsStore
          .markContractsAdded(Seq((cid, ReassignmentCounter.Genesis, dirtyToc)))
          .value
          .futureValueUS
      }

      clue("Verify simulated mid-crash state: Internal store has it, Ledger API has 0") {

        target.underlying.value.sync.stateInspection
          .getAcsInspection(daId)
          .value
          .activeContractStore
          .contractCount(CantonTimestamp.now())
          .futureValueUS shouldBe 1

        target.ledger_api.state.acs.of_party(alice) shouldBe empty
      }

      clue("Execute post-crash recovery retry via import_party_acs") {
        target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)
      }

      // `verifyRepairPreconditions` triggers `deleteSince` upfront to roll back uncommitted partial states,
      //  cleanly streams the raw payloads, and pushes updates down to the Ledger API event writers.
      clue("Verify recovered state synchronizes across both internal storage and Ledger API") {
        eventually() {
          target.ledger_api.state.acs.of_party(alice) should have size 1

          target.underlying.value.sync.stateInspection
            .getAcsInspection(daId)
            .value
            .hasActiveContracts(alice)(traceContext, executionContext)
            .futureValueUS shouldBe true
        }
      }

      // Verify node operations seamlessly continue post-recovery
      reconnectAndEnsureOnboardingClearance(clock, alice, daName)
      assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 1)
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 2 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Asserts that the party ACS import endpoint is strictly idempotent. It verifies that
  * invoking the import command multiple times with the same ACS snapshot safely avoids duplicate
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
    import com.digitalasset.canton.integration.tests.multihostedparties.DivulgenceIntegrationTestHelpers.*

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

    // Assert that there is no indexer event duplication
    target.acsDeltas(alice) should have size 2

    assertAcsAndContinuedOperation(
      target,
      expectedNumActiveContracts = 2,
      numOfContractCreations = 3,
    )
  }
}
