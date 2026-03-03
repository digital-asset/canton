// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.admin.api.client.data.{FlagSet, PartyOnboardingFlagStatus}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidState.AbortAcsExportForMissingOnboardingFlag
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersHistory,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.time.{DelegatingSimClock, PositiveSeconds}
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  ParticipantPermission as PP,
}
import com.digitalasset.canton.topology.{Party, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, SynchronizerAlias, config}

import java.time.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait OfflinePartyReplicationIntegrationTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory
    with HasExecutionContext {

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Alice's replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  protected var source: LocalParticipantReference = _
  protected var target: LocalParticipantReference = _
  protected var alice: Party = _
  protected var bob: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval.toConfig))

        alice =
          participant1.parties.testing.enable("Alice", synchronizeParticipants = Seq(participant2))
        bob =
          participant2.parties.testing.enable("Bob", synchronizeParticipants = Seq(participant1))
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  protected[offpr] val acsSnapshotPath: String = acsSnapshot.toString

  protected[this] def assertAcsAndContinuedOperation(
      participant: LocalParticipantReference,
      expectedNumActiveContracts: Long,
      numOfContractCreations: Int = 1,
  ): Unit = {
    participant.ledger_api.state.acs
      .active_contracts_of_party(alice) should have size expectedNumActiveContracts

    // Archive contract and create another one to assert regular operation after the completed party replication
    val iou = participant.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
    IouSyntax.archive(participant)(iou, alice)

    (1 to numOfContractCreations).foreach { num =>
      IouSyntax.createIou(participant)(alice, bob, s"${num}42.95".toDouble).discard
    }
  }

  /** Gets the `validFrom` timestamp of the PTP transaction that onboarded the party to the target.
    */
  protected[offpr] def getOnboardingEffectiveAt(
      daId: PhysicalSynchronizerId
  ): CantonTimestamp =
    CantonTimestamp
      .fromInstant(
        source.topology.party_to_participant_mappings
          .list(
            synchronizerId = daId,
            filterParty = alice.filterString,
            filterParticipant = target.id.filterString,
          )
          .loneElement
          .context
          .validFrom
      )
      .value

  protected[offpr] def targetAuthorizesHosting(
      party: Party,
      synchronizerId: PhysicalSynchronizerId,
      disconnectTarget: Boolean,
  ): Long = {
    target.topology.party_to_participant_mappings
      .propose_delta(
        party = party.partyId,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = synchronizerId,
        requiresPartyToBeOnboarded = true,
      )

    if (disconnectTarget) {
      target.synchronizers.disconnect_all()
    }

    val sourceLedgerEnd = source.ledger_api.state.end()

    // Authorize the PTP update from the party
    party.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = synchronizerId,
      requiresPartyToBeOnboarded = true,
    )

    sourceLedgerEnd
  }

  /** Reconnects to synchronizer and unilaterally clears the onboarding flag. */
  // TODO(#29427): May no longer be needed when the onboarding flag is cleared as part of the reconnect
  protected[offpr] def reconnectAndRemoveOnboardingFlag(
      clock: DelegatingSimClock,
      party: Party,
      synchronizerAlias: SynchronizerAlias,
  )(implicit env: TestConsoleEnvironment, traceContext: TraceContext): PartyOnboardingFlagStatus = {
    import env.*

    val clockAdvancementStep = Duration.ofSeconds(30)

    // Advance time to allow topology transactions to be processed
    clock.advance(clockAdvancementStep)

    val targetLedgerEnd = target.ledger_api.state.end()

    target.synchronizers.reconnect(synchronizerAlias)

    source.health.ping(target)

    val psid = getInitializedSynchronizer(synchronizerAlias).physicalSynchronizerId

    // Trigger the asynchronous onboarding flag clearance process
    target.parties.clear_party_onboarding_flag(party, psid.logical, targetLedgerEnd)

    // Total duration needed for the asynchronous onboarding flag clearance to complete.
    // Depends on the historical decision timeout and of topology transaction actually removing the onboarding flag
    // becoming effective.
    val totalWaitTime = Duration.ofMinutes(2).plusSeconds(10)
    val maxStepSize = clockAdvancementStep

    // Chunk total duration into smaller advancements
    val fullSteps = (totalWaitTime.toMillis / maxStepSize.toMillis).toInt
    val remainder = Duration.ofMillis(totalWaitTime.toMillis % maxStepSize.toMillis)

    // Creates Seq(30s, 30s, 30s, 30s, 10s)
    val steps = Seq.fill(fullSteps)(maxStepSize) ++ Option.when(!remainder.isZero)(remainder)

    // In Canton's static time mode, advancing the local clock too far at once causes
    // the participant's ledger time to drift ahead of the sequencer's record time.
    //
    // This will result in warnings like so:
    // "Time validation has failed: The delta of the ledger time .* and the record time .* exceeds the max of 1 minute"
    //
    // The sequencer will reject requests if this drift exceeds its 1-minute tolerance.
    // By stepping time in 30-second chunks and sending a ping, we force the sequencer
    // to process an event and update its record time, keeping the nodes synchronized.
    steps.foreach { step =>
      clock.advance(step)
      source.health.ping(target) // Drags the sequencer time forward
    }

    // Force both participants to process all messages up to the new clock time.
    // This ensures `source` processes the flag-clearing transaction sent by `target`.
    source.health.ping(target)
    target.health.ping(source)

    target.parties.clear_party_onboarding_flag(party, psid.logical, targetLedgerEnd)
  }

  protected[offpr] def replicateParty(
      clock: DelegatingSimClock,
      party: Party,
      workflowIdPrefix: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val beforeActivationOffset =
      targetAuthorizesHosting(party, daId, disconnectTarget = true)

    source.parties.export_party_acs(
      party = party,
      synchronizerId = daId.logical,
      targetParticipantId = target.id,
      beginOffsetExclusive = beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )
    target.parties.import_party_acsV2(acsSnapshotPath, daId, workflowIdPrefix)

    reconnectAndRemoveOnboardingFlag(clock, party, daName)
  }

}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 2 active IOU contract between Alice (signatory) and Bob (observer)
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test Goal: Follow the Offline Party Replication operational steps as documented in
  * `docs-open/src/sphinx/participant/howtos/operate/parties/party_replication.rst`
  *
  * Test Steps: Replicate Alice to target using the onboarding flag
  *   - Target participant authorizes Alice->target setting the onboarding flag
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target setting the onboarding flag
  *   - ACS snapshot for Alice is taken on source participant
  *   - ACS is imported on the target participant, which then reconnects to the synchronizers
  *   - Target participant clears the onboarding flag unilaterally
  *   - Assert successful clearing of onboarding flag by the target participant
  */
final class OfflinePartyReplicationIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3
      }

  "Regular offline party replication operational flow succeeds" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

    IouSyntax.createIou(participant1)(alice, bob, 3.33).discard

    target.topology.party_to_participant_mappings
      .propose_delta(
        party = alice,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )

    target.synchronizers.disconnect_all()

    val createIouCmd = testIou(alice, bob, 2.20).create().commands().asScala.toSeq

    source.ledger_api.javaapi.commands.submit(
      Seq(alice),
      createIouCmd,
      daId,
      optTimeout = None,
    )

    val sourceLedgerEnd = source.ledger_api.state.end()

    // Alice also needs to authorize the transaction
    alice.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target,
      beginOffsetExclusive = sourceLedgerEnd,
      exportFilePath = acsSnapshotPath,
    )

    // Assert ACS snapshot is non-empty (no stakeholder filtering happened)
    repair.acs.read_from_file(acsSnapshotPath) should have size 2

    target.parties.import_party_acsV2(acsSnapshotPath, daId)

    // Advance time to allow topology transactions to be processed
    clock.advance(Duration.ofSeconds(30))
    checkOnboardingFlag(daId, setOnTarget = true)

    val targetLedgerEnd = target.ledger_api.state.end()

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2

    // Trigger the asynchronous onboarding flag clearance process
    val status = target.parties.clear_party_onboarding_flag(alice, daId, targetLedgerEnd)
    val expectedTimestamp = computeExpectedDecisionDeadline(getOnboardingEffectiveAt(daId))

    status shouldBe FlagSet(expectedTimestamp)

    // Assert contract archival and creation continues work even though the onboarding flag
    // has not been cleared yet.
    assertAcsAndContinuedOperation(
      target,
      expectedNumActiveContracts = 2,
      numOfContractCreations = 7,
    )

    // Advance time to allow the asynchronous onboarding flag clearance to complete
    clock.advance(Duration.ofMinutes(2).plusSeconds(10))

    // Force both participants to process all messages up to the new clock time.
    // This ensures `source` processes the flag-clearing transaction sent by `target`.
    source.health.ping(target)
    target.health.ping(source)

    // Now that we've advanced time and synced, the onboarding flag on the target participant should be cleared
    checkOnboardingFlag(daId, setOnTarget = false)

    assertAcsAndContinuedOperation(source, expectedNumActiveContracts = 8)
  }

  private def checkOnboardingFlag(daId: => PhysicalSynchronizerId, setOnTarget: Boolean): Unit = {
    val lastPTP =
      source.topology.party_to_participant_mappings.list(synchronizerId = daId).last.item

    lastPTP.partyId shouldBe alice.partyId
    lastPTP.participants should have size 2

    forExactly(1, lastPTP.participants) { p =>
      p.participantId shouldBe source.id
      p.onboarding should be(false)
    }
    forExactly(1, lastPTP.participants) { p =>
      p.participantId shouldBe target.id
      p.onboarding should be(setOnTarget)
    }
  }

  private def computeExpectedDecisionDeadline(
      effectiveAt: CantonTimestamp
  ): CantonTimestamp = {
    val paramsWithValidity = DynamicSynchronizerParametersWithValidity(
      parameters = DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
      validFrom = CantonTimestamp.MinValue,
      validUntil = None,
    )

    DynamicSynchronizerParametersHistory
      .latestDecisionDeadlineEffectiveAt(
        Seq(paramsWithValidity),
        effectiveAt,
      )
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2 (P2)
  *   - 5 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test Focus: Edge cases, failure modes, and state recovery during offline party replication (ACS
  * export/import).
  *
  * Note: These tests run sequentially on the same nodes and share state. Therefore, the tests rely
  * on proper state cleanup to verify that failures do not permanently corrupt the node's ability to
  * perform a successful replication.
  */
final class OfflinePartyReplicationEdgeCasesIntegrationTest
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
        IouSyntax.createIou(source)(alice, bob, 4.95).discard
        IouSyntax.createIou(source)(alice, bob, 5.95).discard
      }

  /** Missing Party Activation Test Case:
    *
    * Asserts that a party ACS export aborts (times out) if the party (Alice) has not been
    * authorized/activated on the target participant prior to export.
    */
  "Missing party activation on the target participant aborts ACS export" in { implicit env =>
    import env.*

    val ledgerEndP1 = source.ledger_api.state.end()

    // no authorization for alice

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = ledgerEndP1,
        exportFilePath = acsSnapshotPath,
        waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofMillis(5)),
      ),
      _.errorMessage should include regex "The stream has not been completed in.*– Possibly missing party activation?",
    )
  }

  /** Missing Onboarding Flag Test Case:
    *   - Asserts that if Alice is authorized on the target participant but the `onboarding` flag is
    *     set to `false`, the party ACS export explicitly aborts with an expected error.
    *   - Reverses the invalid topology state by deactivating Alice on the target. This cleanup is
    *     strictly required to prepare the shared node state for the subsequent test.
    */
  "Party activation on the target participant with missing onboarding flag aborts ACS export" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()

      // Authorize Alice without onboarding flag set
      PartyToParticipantDeclarative.forParty(Set(source, target), daId)(
        source.id,
        alice,
        PositiveInt.one,
        Set(
          (source.id, PP.Submission),
          (target.id, PP.Submission),
        ),
      )(executionContext, env)

      forAll(
        source.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = alice.toProtoPrimitive,
            filterParticipant = target.toProtoPrimitive,
          )
          .loneElement
          .item
          .participants
      ) { p =>
        p.onboarding shouldBe false
      }

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.export_party_acs(
          party = alice,
          synchronizerId = daId,
          targetParticipantId = target.id,
          beginOffsetExclusive = ledgerEndP1,
          exportFilePath = acsSnapshotPath,
          waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(1)),
        ),
        _.errorMessage should include(
          AbortAcsExportForMissingOnboardingFlag(alice.partyId, target).cause
        ),
      )

      // Undo activating Alice on the target participant without onboarding flag set; for the following test
      alice.topology.party_to_participant_mappings.propose_delta(
        source,
        removes = Seq(target.id),
        store = daId,
        mustFullyAuthorize = true,
      )

      // Wait for party deactivation to propagate to both source and target participants
      eventually() {
        val sourceMapping = source.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        sourceMapping.participants should have size 1
        forExactly(1, sourceMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }

        // Ensure the target has processed the "remove" transaction to prevent flakes
        val targetMapping = target.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        targetMapping.participants should have size 1
        forExactly(1, targetMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }
      }

  }

  /** Successful Replication After Failure Recovery Test Case:
    *   - Verifies that a normal party replication still succeeds after the aborted attempt and
    *     cleanup from the previous test, proving the nodes remained in a healthy state.
    *   - Asserts the expected number of active contracts and verifies continued operations (Alice
    *     can archive and create contracts on the target).
    */
  "Party replication succeeds following cleanup of prior failed activations" in { implicit env =>
    executeSuccessfulReplicationFlow(expectedAcsSnapshotSize = 5)
  }

  /** Empty ACS Snapshot Import Test Case:
    *   - Verifies that party replication succeeds even when the generated ACS snapshot is empty.
    *   - This occurs when the target (participant2) already hosts a stakeholder (Bob) who shares
    *     all active contracts with the replicated party (Alice), resulting in an empty export due
    *     to stakeholder filtering.
    *   - Asserts that importing this empty snapshot acts as a safe no-op and the party is
    *     successfully onboarded.
    */
  "Party replication succeeds even when importing an empty ACS snapshot" in { implicit env =>
    import env.*

    // Participant2 hosts Bob who shares all active contracts with Alice.
    // Replicating Alice to participant2 will filter out all active contracts for Alice,
    // resulting in an empty ACS snapshot.
    target = participant2

    executeSuccessfulReplicationFlow(expectedAcsSnapshotSize = 0)
  }

  private def executeSuccessfulReplicationFlow(expectedAcsSnapshotSize: Long)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val clock = env.environment.simClock.value

    val beforeActivationOffset =
      targetAuthorizesHosting(alice, daId, disconnectTarget = true)

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target.id,
      beginOffsetExclusive = beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )

    repair.acs.read_from_file(acsSnapshotPath) should have size expectedAcsSnapshotSize

    target.parties.import_party_acsV2(acsSnapshotPath, daId)

    reconnectAndRemoveOnboardingFlag(clock, alice, daName)

    assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 5)
  }

}
