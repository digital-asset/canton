// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.admin.api.client.data.FlagNotSet
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AutomaticAssignment, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.data.ImportPartyAcsForceFlag
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.{
  AcsImportAfterActivation,
  AcsImportMissingOnboardingMapping,
}
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.store.PendingOperationStore
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Party, SynchronizerId}
import com.digitalasset.canton.{HasTempDirectory, ReassignmentCounter, RepairCounter}
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.duration.DurationInt

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

  /** Simulates an operator who leaves the target participant connected until it has processed
    * events past the party's activation, and disconnects it only for the import.
    *
    * The expectation is that the target participant only receives the effective transaction after
    * having imported the party's ACS and (re)connected to the synchronizer.
    *
    * Asserts:
    *   1. The ACS import is rejected.
    *   1. With the force flag, the ACS import succeeds, and the pending onboarding clearance
    *      operation is recorded and can be cleared.
    */
  "ACS import is rejected once the target has processed the activation, unless forced" in {
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
      val results = eventually() {
        val results = target.topology.party_to_participant_mappings.list(
          synchronizerId = daId,
          filterParty = alice.filterString,
          filterParticipant = target.id.filterString,
        )
        results should have size 1
        results
      }

      // Flake prevention: Wait for the ledger end record time to exceed the PTP topology transaction's
      // effective timestamp (validFrom). This prevents a race condition where the target participant
      // disconnects before receiving a background sequencer time tick, stranding its ledger end exactly
      // on the topology offset.
      //
      // If the ledger end does not advance, the subsequent repair command will fail with:
      //   "IMPORT_ACS_ERROR: Cannot apply a repair command as the last event is a topology offset.
      //   Please reconnect to the synchronizer to move the ledger end. In case the synchronizer cannot be
      //   connected again without this operation, contact support."
      val ptpValidFrom =
        CantonTimestamp.fromInstant(results.headOption.value.context.validFrom).value
      val ledgerApiStore =
        target.testing.state_inspection.syncPersistentStateManager.ledgerApiStore.value
      eventually() {
        val ledgerEndRecordTime =
          ledgerApiStore.ledgerEnd.value.synchronizerIndices
            .get(daId)
            .value
            .recordTime

        ledgerEndRecordTime should be > ptpValidFrom
      }

      source.parties.export_party_acs(
        alice,
        daId,
        target.id,
        exportFilePath = acsSnapshotPath,
        beginOffsetExclusive = beforeActivationSourceOffset,
      )

      target.synchronizers.disconnect_all()

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath),
        _.shouldBeCantonErrorCode(AcsImportAfterActivation),
      )

      target.parties.import_party_acs(
        daId,
        Some(alice),
        acsSnapshotPath,
        forceFlags = Set(ImportPartyAcsForceFlag.AllowImportAfterActivation),
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

    target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

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

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 2 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test: Asserts that importing a later ACS snapshot picks up contracts that were created after an
  * earlier snapshot was taken. It takes a first ACS export, creates an additional contract that
  * augments the ACS, takes a second ACS export, imports the first snapshot (verifying the new
  * contract is not yet present), then imports the second snapshot (verifying the new contract is
  * now present).
  */
final class OfflinePartyReplicationIncrementalAcsImportIntegrationTest
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

  "ACS import of a later snapshot picks up contracts created after an earlier snapshot" in {
    implicit env =>
      import env.*

      val clock = env.environment.simClock.value

      // Take a first ACS export (2 active contracts).
      val firstExportOffset = source.ledger_api.state.end()
      source.repair.export_acs(
        parties = Set(alice),
        ledgerOffset = firstExportOffset,
        exportFilePath = acsSnapshotPath,
      )

      // Execute a new transaction that augments the ACS (now 3 active contracts)
      IouSyntax.createIou(source)(alice, bob, 3.95).discard
      source.ledger_api.state.acs.active_contracts_of_party(alice) should have size 3

      val secondAcsSnapshotPath =
        tempDirectory.toTempFile("offpr_incremental_second_acs_snapshot.gz").toString

      // Take a second ACS export capturing the newly created contract
      val secondExportOffset = source.ledger_api.state.end()
      source.repair.export_acs(
        parties = Set(alice),
        ledgerOffset = secondExportOffset,
        exportFilePath = secondAcsSnapshotPath,
      )

      // Only now authorize hosting: this creates the onboarding PTP mapping required by the
      // party ACS import and disconnects the target from all synchronizers.
      targetAuthorizesHosting(alice, daId, disconnectTarget = true).discard

      // Import the first ACS snapshot and verify the new contract isn't there yet
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)
      eventually() {
        target.ledger_api.state.acs.active_contracts_of_party(alice) should have size 2
      }

      // Import the second ACS snapshot and verify the new contract is now present
      target.parties.import_party_acs(daId, Some(alice), secondAcsSnapshotPath)
      eventually() {
        target.ledger_api.state.acs.active_contracts_of_party(alice) should have size 3
      }

      reconnectAndEnsureOnboardingClearance(clock, alice, daName)

      assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 3)
  }
}

/** Setup:
  *   - Alice and Carol are hosted on participant1 (source), in its namespace
  *   - Bob is hosted on participant2
  *   - 1 active IOU contract between Alice and Bob
  *   - Participant3 (target) is the target for replicating Alice
  *
  * Test: The onboarding flag clearance of Alice waits for the decision deadline of Alice's
  * onboarding, although the target is still onboarding Carol, whose decision deadline has passed.
  * The import must not take Carol's onboarding transaction for Alice's, as both parties share a
  * namespace.
  */
final class ImportPartyAcsWhileTargetOnboardsAnotherPartyIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
      }

  private def targetIsOnboarding(party: Party)(implicit env: TestConsoleEnvironment): Boolean =
    source.topology.party_to_participant_mappings
      .list(env.daId, filterParty = party.filterString, filterParticipant = target.id.filterString)
      .loneElement
      .item
      .participants
      .exists(p => p.participantId == target.id && p.onboarding)

  "The onboarding flag clearance waits for the decision deadline of the imported party's onboarding" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      // The target stays onboarding Carol, as it never imports her ACS
      val carol = source.parties.testing.enable("Carol", synchronizeParticipants = Seq(target))
      targetAuthorizesHosting(carol, daId, disconnectTarget = false).discard
      eventually()(targetIsOnboarding(carol) shouldBe true)
      // Pass the decision deadline of Carol's onboarding
      advanceTimeAndPing(clock, Duration.ofMinutes(2).plusSeconds(10), source, target)

      val beforeActivationOffset = targetAuthorizesHosting(alice, daId, disconnectTarget = true)
      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

      clue("Alice's onboarding is not yet effective on the disconnected target") {
        PendingOperationStore(
          target.underlying.value.storage,
          timeouts,
          loggerFactory,
          OnboardingClearanceOperation,
          SynchronizerId.fromString,
        )(executionContext)
          .get(
            daId.logical,
            OnboardingClearanceOperation.operationKey(alice.partyId),
            OnboardingClearanceOperation.operationName,
          )
          .value
          .futureValueUS
          .value
          .operation
          .onboardingEffectiveAt shouldBe None
      }

      val targetLedgerEnd = target.ledger_api.state.end()
      clock.advance(Duration.ofSeconds(30))
      target.synchronizers.reconnect(daName)
      source.health.ping(target)
      target.health.ping(source)

      clue("The flag stays set before the decision deadline of Alice's onboarding") {
        always(durationOfSuccess = 5.seconds)(targetIsOnboarding(alice) shouldBe true)
      }

      advanceTimeAndPing(clock, Duration.ofMinutes(2).plusSeconds(10), source, target)
      eventually() {
        target.parties.clear_party_onboarding_flag(
          alice,
          daId.logical,
          targetLedgerEnd,
        ) shouldBe FlagNotSet
      }
  }
}

/** Test scenario: the target participant stays connected while a party's hosting on it becomes
  * effective, and then processes reassignments of one of the party's contracts. The ACS snapshot is
  * taken at the activation, so it still has the contract on `da`.
  *
  * Test goal: show what a forced ACS import does with such a contract:
  *   - unassigned to `acme`: the import fails on it, rather than making it active on `da` again;
  *   - unassigned and assigned back to `da`: the import skips it, and it is active on the target.
  *
  * Setup: participant1 (source, hosts Alice and Carol) and participant2 (hosts Bob) are connected
  * to `da` and `acme`; the target participant3 only to `da`. Automatic assignment is disabled.
  */
final class ImportPartyAcsAfterUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag)
      .withSetup { implicit env =>
        import env.*
        AutomaticAssignment.disable(Seq(sequencer1), daId)
        AutomaticAssignment.disable(Seq(sequencer2), acmeId)

        Seq(participant1, participant2).foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
          participant.synchronizers.connect_local(sequencer2, alias = acmeName)
          participant.dars.upload(CantonExamplesPath, synchronizerId = daId)
          participant.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        }
        participant3.synchronizers.connect_local(sequencer1, alias = daName)
        participant3.dars.upload(CantonExamplesPath, synchronizerId = daId)

        def hostedOn(participant: LocalParticipantReference) = {
          val hosting = (
            PositiveInt.one,
            Set((participant.id, ParticipantPermission.Submission: ParticipantPermission)),
          )
          Map(daId -> hosting, acmeId -> hosting)
        }
        val parties = PartiesAllocator(Set(participant1, participant2))(
          Seq("alice" -> participant1.id, "carol" -> participant1.id, "bob" -> participant2.id),
          Map(
            "alice" -> hostedOn(participant1),
            "carol" -> hostedOn(participant1),
            "bob" -> hostedOn(participant2),
          ),
        )
        def party(name: String) = parties.find(_.toProtoPrimitive.contains(name)).value
        alice = party("alice")
        carol = party("carol")
        bob = party("bob")
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private var alice: Party = _
  private var carol: Party = _
  private var bob: Party = _

  /** Creates a contract of `party` on `da`, activates `party` on the connected target and exports
    * the party's ACS at the activation.
    *
    * @return
    *   The contract and the path of the ACS snapshot.
    */
  private def createContractAndActivateOnConnectedTarget(party: Party)(implicit
      env: TestConsoleEnvironment
  ): (LfContractId, String) = {
    import env.*
    val source = participant1
    val target = participant3
    val acsSnapshotPath =
      tempDirectory.toTempFile(s"${party.partyId.uid.identifier.str}_acs_snapshot.gz").toString

    val iou = IouSyntax.createIou(source, Some(daId))(party, bob)
    val beforeActivationSourceOffset = source.ledger_api.state.end()

    target.topology.party_to_participant_mappings.propose_delta(
      party = party.partyId,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )
    party.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )
    eventually() {
      target.topology.party_to_participant_mappings.list(
        synchronizerId = daId,
        filterParty = party.filterString,
        filterParticipant = target.id.filterString,
      ) should have size 1
    }

    source.parties.export_party_acs(
      party,
      daId,
      target.id,
      exportFilePath = acsSnapshotPath,
      beginOffsetExclusive = beforeActivationSourceOffset,
    )
    (LfContractId.assertFromString(iou.id.contractId), acsSnapshotPath)
  }

  /** Runs `reassign`, in which the target processes an unassignment of a contract that is not in
    * its ACS yet. The target's indexer then warns that the contract's activation is missing.
    */
  private def expectMissingActivationWarnings(reassign: => Unit): Unit = {
    val missingActivation = Seq(
      "deactivation reference cannot be computed",
      "Activation is missing for a deactivation",
    )
    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      {
        reassign
        // The indexer logs asynchronously
        eventually() {
          loggerFactory.fetchRecordedLogEntries.exists(
            _.warningMessage.contains(missingActivation.last)
          ) shouldBe true
        }
      },
      entries =>
        forAll(entries) { entry =>
          entry.loggerName should include("participant=participant3")
          missingActivation.exists(entry.warningMessage.contains) shouldBe true
        },
    )
  }

  private def importForced(party: Party, acsSnapshotPath: String)(implicit
      env: TestConsoleEnvironment
  ): Unit =
    env.participant3.parties.import_party_acs(
      env.daId,
      Some(party),
      acsSnapshotPath,
      forceFlags = Set(ImportPartyAcsForceFlag.AllowImportAfterActivation),
    )

  "A forced ACS import" should {
    "fail on a contract unassigned after the activation" in { implicit env =>
      import env.*
      val (contractId, acsSnapshotPath) = createContractAndActivateOnConnectedTarget(alice)

      expectMissingActivationWarnings {
        participant1.ledger_api.commands
          .submit_unassign(alice, Seq(contractId), daId, acmeId)
          .discard
        // Sequenced on `da` after the unassignment, so the target has processed it once this completes
        participant1.health.ping(participant3).discard
      }
      participant3.synchronizers.disconnect_all()

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        importForced(alice, acsSnapshotPath),
        _.errorMessage should include("needs to be strictly larger"),
      )
    }

    "skip a contract unassigned and assigned back after the activation" in { implicit env =>
      import env.*
      participant3.synchronizers.reconnect_all()
      val (contractId, acsSnapshotPath) = createContractAndActivateOnConnectedTarget(carol)

      expectMissingActivationWarnings {
        // An assignment goes to the target of its unassignment, so going back takes two reassignments
        val toAcme =
          participant1.ledger_api.commands.submit_unassign(carol, Seq(contractId), daId, acmeId)
        participant1.ledger_api.commands.submit_assign(carol, toAcme.reassignmentId, daId, acmeId)
        val toDa =
          participant1.ledger_api.commands.submit_unassign(carol, Seq(contractId), acmeId, daId)
        participant1.ledger_api.commands.submit_assign(carol, toDa.reassignmentId, acmeId, daId)
        // Sequenced on `da` after the assignment, so the target has processed it once this completes
        participant1.health.ping(participant3).discard
      }
      participant3.synchronizers.disconnect_all()

      importForced(carol, acsSnapshotPath)

      // The target keeps the counter of the assignment it processed, not the snapshot's 0
      participant3.ledger_api.state.acs
        .of_party(carol)
        .map(contract => contract.contractId -> contract.reassignmentCounter) shouldBe
        Seq(contractId.coid -> 2L)
    }
  }
}
